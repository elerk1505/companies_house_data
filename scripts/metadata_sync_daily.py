from __future__ import annotations
import os, json, tempfile, datetime as dt
from typing import List
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
    'sic_codes','officers','last_updated'
]

def fetch_company_meta(company_id: str) -> dict:
    # Companies House API endpoints (simplified):
    base = "https://api.company-information.service.gov.uk/company/"
    meta = {}
    r = SESS.get(base + company_id, timeout=60)
    if r.ok:
        j = r.json()
        meta.update({
            'companies_house_registered_number': company_id,
            'entity_current_legal_name': j.get('company_name'),
            'company_type': j.get('type'),
            'incorporation_date': j.get('date_of_creation'),
            'sic_codes': j.get('sic_codes', []),
        })
    # officers
    ro = SESS.get(base + company_id + "/officers", timeout=60)
    if ro.ok:
        jj = ro.json()
        meta['officers'] = [x.get('name') for x in jj.get('items', [])]
    meta['last_updated'] = dt.datetime.utcnow().isoformat()
    return meta


def main():
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    state = {"seen_ids": []}
    if os.path.exists(STATE_PATH):
        state = json.load(open(STATE_PATH, "r", encoding="utf-8"))

    # Strategy: iterate over financials releases for the last 7 days halves; collect new IDs
    today = dt.date.today()
    halves = set()
    for delta in range(0, 8):
        d = today - dt.timedelta(days=delta)
        halves.add((d.year, 'H1' if d.month <= 6 else 'H2'))

    all_new_ids = set()
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

    rows = []
    for cid in sorted(all_new_ids):
        try:
            meta = fetch_company_meta(cid)
            if meta.get('companies_house_registered_number'):
                rows.append(meta)
        except Exception:
            continue

    if not rows:
        return

    df = pd.DataFrame(rows, columns=META_COLUMNS)

    # Choose half by incorporation_date if present, else current date
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
