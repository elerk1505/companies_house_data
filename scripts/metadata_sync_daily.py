from __future__ import annotations
import os, json, tempfile, datetime as dt
from typing import List, Dict, Any
import pandas as pd
from scripts.common import (gh_release_ensure, gh_release_find_asset, gh_release_download_asset,
                            gh_release_upload_or_replace_asset, append_parquet, half_from_date, tag_for_metadata)
import requests

CH_API_KEY = os.getenv("CH_API_KEY", "")
SESS = requests.Session()
if CH_API_KEY:
    SESS.auth = (CH_API_KEY, "")

STATE_PATH = os.getenv("STATE_META_PATH", "state/metadata_state.json")
OUTPUT_BASENAME = "metadata.parquet"

META_COLUMNS = [
    'companies_house_registered_number','entity_current_legal_name','company_type','incorporation_date',
    'sic_codes','officers','registered_office_postcode','registered_office_locality',
    'company_status','last_updated'
]

API_BASE = "https://api.company-information.service.gov.uk"


def fetch_company_profile(company_id: str) -> Dict[str, Any]:
    r = SESS.get(f"{API_BASE}/company/{company_id}", timeout=60)
    r.raise_for_status()
    j = r.json()
    out: Dict[str, Any] = {
        'companies_house_registered_number': company_id,
        'entity_current_legal_name': j.get('company_name'),
        'company_type': j.get('type'),
        'incorporation_date': j.get('date_of_creation'),
        'sic_codes': j.get('sic_codes', []),
        'company_status': j.get('company_status'),
    }
    roa = j.get('registered_office_address') or {}
    out['registered_office_postcode'] = roa.get('postal_code')
    out['registered_office_locality'] = roa.get('locality')
    return out


def fetch_officers(company_id: str) -> List[str]:
    r = SESS.get(f"{API_BASE}/company/{company_id}/officers", timeout=60, params={"items_per_page": 100})
    if not r.ok:
        return []
    j = r.json()
    return [x.get('name') for x in j.get('items', []) if x.get('name')]


def fetch_advanced_enrichment(company_id: str, company_name: str | None) -> Dict[str, Any]:
    """Use Advanced Search to enrich fields like address and confirm SIC; match by company_number."""
    if not company_name:
        return {}
    params = {
        "company_name_includes": company_name,
        "size": 100,
        "start_index": 0,
    }
    try:
        r = SESS.get(f"{API_BASE}/advanced-search/companies", params=params, timeout=60)
        if not r.ok:
            return {}
        j = r.json()
        for it in j.get('items', []):
            if it.get('company_number') == company_id:
                ro = it.get('registered_office_address') or {}
                return {
                    'registered_office_postcode': ro.get('postal_code'),
                    'registered_office_locality': ro.get('locality'),
                    'sic_codes': it.get('sic_codes') or None,
                    'company_status': it.get('company_status') or None,
                }
    except Exception:
        return {}
    return {}


def main():
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    state = {"seen_ids": []}
    if os.path.exists(STATE_PATH):
        state = json.load(open(STATE_PATH, "r", encoding="utf-8"))

    today = dt.date.today()
    halves = {(d.year, 'H1' if d.month <= 6 else 'H2') for d in [today - dt.timedelta(days=i) for i in range(0, 8)]}

    all_new_ids: set[str] = set()
    for year, half in sorted(halves):
        fin_tag = f"data-{year}-{half}-financials"
        fin_rel = gh_release_ensure(fin_tag)
        asset = gh_release_find_asset(fin_rel, "financials.parquet")
        if not asset:
            continue
        tmp_fin = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False).name
        gh_release_download_asset(asset, tmp_fin)
        fin_df = pd.read_parquet(tmp_fin, columns=['companies_house_registered_number'])
        ids = set(fin_df['companies_house_registered_number'].astype(str).unique())
        for cid in ids:
            if cid not in state.get("seen_ids", []):
                all_new_ids.add(cid)

    if not all_new_ids:
        json.dump(state, open(STATE_PATH, "w", encoding="utf-8"), indent=2)
        return

    rows: List[Dict[str, Any]] = []
    for cid in sorted(all_new_ids):
        try:
            base = fetch_company_profile(cid)
            base['officers'] = fetch_officers(cid)
            adv = fetch_advanced_enrichment(cid, base.get('entity_current_legal_name'))
            base.update({k: v for k, v in adv.items() if v is not None})
            base['last_updated'] = dt.datetime.utcnow().isoformat()
            rows.append(base)
        except Exception:
            continue

    if not rows:
        return

    df = pd.DataFrame(rows, columns=META_COLUMNS)

    if df['incorporation_date'].notna().any():
        dd = pd.to_datetime(df['incorporation_date'], errors='coerce').dropna()
        ref_date = dd.iloc[0].date()
    else:
        ref_date = dt.date.today()
    year = ref_date.year
    half = half_from_date(ref_date)

    rel = gh_release_ensure(tag_for_metadata(year, half), name=f"Metadata {year} {half}")
    tmp_out = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False).name
    asset = gh_release_find_asset(rel, OUTPUT_BASENAME)
    if asset:
        gh_release_download_asset(asset, tmp_out)
    append_parquet(tmp_out, df, subset_keys=['companies_house_registered_number'])
    gh_release_upload_or_replace_asset(rel, tmp_out, name=OUTPUT_BASENAME)

    state['seen_ids'] = sorted(set(state.get('seen_ids', [])) | set(all_new_ids))
    json.dump(state, open(STATE_PATH, "w", encoding="utf-8"), indent=2)

if __name__ == "__main__":
    main()
