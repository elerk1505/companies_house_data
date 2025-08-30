# ================================================================
# scripts/metadata_bulk_api_fill.py
# Fill metadata for IDs NOT covered by the snapshot, using the API.
# - Reads financials + existing metadata for a half (H1/H2)
# - Computes remaining IDs
# - Fetches Company Profile + Advanced Search (no officers)
# - Throttled + Retry-After handling, checkpoints every N rows
# ================================================================
from __future__ import annotations

import os, time, tempfile, argparse, datetime as dt
from typing import Any, Dict, List, Set

import pandas as pd
import requests

from scripts.common import (
    gh_release_ensure,
    gh_release_find_asset,
    gh_release_download_asset,
    gh_release_upload_or_replace_asset,
    append_parquet,
)

API_BASE = "https://api.company-information.service.gov.uk"
OUTPUT_BASENAME = "metadata.parquet"
CH_API_KEY = os.getenv("CH_API_KEY", "")

# --- HTTP session + pacing ---
SESS = requests.Session()
SESS.headers.update({"User-Agent": "CompaniesHouseFinder/1.0"})
if CH_API_KEY:
    SESS.auth = (CH_API_KEY, "")

_last: List[float] = []
def _pace(max_rpm: int) -> None:
    now = time.time()
    while _last and now - _last[0] > 60:
        _last.pop(0)
    if len(_last) >= max_rpm:
        sleep = 60 - (now - _last[0]) + 0.05
        if sleep > 0:
            time.sleep(sleep)
    _last.append(time.time())

def _get(url: str, max_rpm: int, **kw) -> requests.Response:
    backoff = 2
    for _ in range(8):
        _pace(max_rpm)
        r = SESS.get(url, timeout=60, **kw)
        if r.status_code != 429:
            return r
        ra = r.headers.get("Retry-After")
        wait = float(ra) if ra and ra.isdigit() else backoff
        print(f"[rate] 429 {url} — sleeping {wait:.1f}s")
        time.sleep(wait)
        backoff = min(backoff * 2, 60)
    return r

# --- Normalise to your iXBRL ID style (digits-only, no leading zeros) ---
def norm_ixbrl_id(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = "".join(ch for ch in str(x).strip() if ch.isdigit())
    s = s.lstrip("0")
    return s or None

# --- API helpers (profile + advanced search only) ---
def fetch_company_profile(cid: str, max_rpm: int) -> Dict[str, Any]:
    r = _get(f"{API_BASE}/company/{cid}", max_rpm=max_rpm)
    r.raise_for_status()
    j = r.json()
    roa = j.get("registered_office_address") or {}
    return {
        "companies_house_registered_number": cid,
        "entity_current_legal_name": j.get("company_name"),
        "company_type": j.get("type"),
        "company_status": j.get("company_status"),
        "incorporation_date": j.get("date_of_creation"),
        "sic_codes": j.get("sic_codes", []),
        "registered_office_postcode": roa.get("postal_code"),
        "registered_office_post_town": roa.get("locality"),
    }

def fetch_advanced(cid: str, company_name: str | None, max_rpm: int) -> Dict[str, Any]:
    if not company_name:
        return {}
    params = {"company_name_includes": company_name, "size": 200}
    try:
        r = _get(f"{API_BASE}/advanced-search/companies", params=params, max_rpm=max_rpm)
        if not r.ok:
            return {}
        for it in r.json().get("items", []):
            if it.get("company_number"):
                # normalise to digits-only for comparison to your IDs
                test = norm_ixbrl_id(it["company_number"])
                if test == cid:
                    ro = it.get("registered_office_address") or {}
                    out = {
                        "company_status": it.get("company_status") or None,
                        "registered_office_postcode": ro.get("postal_code"),
                        "registered_office_post_town": ro.get("locality"),
                    }
                    if it.get("sic_codes"):
                        out["sic_codes"] = it["sic_codes"]
                    return out
    except Exception:
        pass
    return {}

META_COLUMNS = [
    "companies_house_registered_number",
    "entity_current_legal_name",
    "company_type",
    "company_status",
    "incorporation_date",
    "sic_codes",
    "registered_office_postcode",
    "registered_office_post_town",
    "last_updated",
]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--half", type=str, required=True, choices=["H1","H2"])
    ap.add_argument("--batch-size", type=int, default=300)
    ap.add_argument("--max-rpm", type=int, default=int(os.getenv("CH_MAX_RPM", "10")))
    ap.add_argument("--limit", type=int, default=0, help="Optional cap (testing)")
    args = ap.parse_args()

    # Load financials IDs for this half
    fin_tag = f"data-{args.year}-{args.half}-financials"
    fin_rel = gh_release_ensure(fin_tag)
    fin_asset = gh_release_find_asset(fin_rel, "financials.parquet")
    if not fin_asset:
        print(f"[error] no financials.parquet in {fin_tag}"); return
    tmp_fin = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False).name
    gh_release_download_asset(fin_asset, tmp_fin)
    fin_df = pd.read_parquet(tmp_fin)
    fin_ids_s = pd.Series(dtype=object)
    if "companies_house_registered_number" in fin_df.columns:
        fin_ids_s = pd.concat([fin_ids_s, fin_df["companies_house_registered_number"]], ignore_index=True)
    if "company_id" in fin_df.columns:
        fin_ids_s = pd.concat([fin_ids_s, fin_df["company_id"]], ignore_index=True)
    fin_ids = (
        fin_ids_s.map(norm_ixbrl_id).dropna().astype(str).unique().tolist()
    )
    fin_set = set(fin_ids)
    print(f"[info] financial IDs for {fin_tag}: {len(fin_set):,}")

    # Load existing metadata to compute remaining
    meta_tag = f"data-{args.year}-{args.half}-metadata"
    meta_rel = gh_release_ensure(meta_tag, name=f"Metadata {args.year} {args.half}")
    tmp_meta = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False).name
    existing: Set[str] = set()
    meta_asset = gh_release_find_asset(meta_rel, OUTPUT_BASENAME)
    if meta_asset:
        gh_release_download_asset(meta_asset, tmp_meta)
        try:
            existing = set(
                pd.read_parquet(tmp_meta, columns=["companies_house_registered_number"])
                  ["companies_house_registered_number"].map(norm_ixbrl_id).dropna().astype(str).unique()
            )
        except Exception:
            existing = set()
    remain = [cid for cid in fin_ids if cid not in existing]
    if args.limit > 0:
        remain = remain[: args.limit]
    if not remain:
        print("[info] nothing to fetch (all IDs already present)"); return

    print(f"[info] to fetch via API: {len(remain):,} (rpm={args.max_rpm}, batch={args.batch_size})")

    buffer: List[Dict[str, Any]] = []
    fetched = 0
    for i, cid in enumerate(remain, 1):
        try:
            base = fetch_company_profile(cid, max_rpm=args.max_rpm)
            adv = fetch_advanced(cid, base.get("entity_current_legal_name"), max_rpm=args.max_rpm)
            base.update({k: v for k, v in adv.items() if v is not None})
            base["last_updated"] = dt.datetime.utcnow().isoformat()
            buffer.append(base)
            fetched += 1
        except requests.HTTPError as e:
            print(f"[warn] {cid}: HTTP {getattr(e.response, 'status_code', '??')}")
        except Exception as e:
            print(f"[warn] {cid}: {e}")

        # checkpoint
        if i % max(1, args.batch_size) == 0:
            df = pd.DataFrame(buffer, columns=META_COLUMNS)
            if not df.empty:
                if meta_asset:
                    gh_release_download_asset(meta_asset, tmp_meta)
                append_parquet(tmp_meta, df, subset_keys=["companies_house_registered_number"])
                gh_release_upload_or_replace_asset(meta_rel, tmp_meta, name=OUTPUT_BASENAME)
                print(f"[info] checkpoint: appended {len(df)} rows (i={i})")
                buffer.clear()

    # flush remaining
    if buffer:
        df = pd.DataFrame(buffer, columns=META_COLUMNS)
        if not df.empty:
            if meta_asset:
                gh_release_download_asset(meta_asset, tmp_meta)
            append_parquet(tmp_meta, df, subset_keys=["companies_house_registered_number"])
            gh_release_upload_or_replace_asset(meta_rel, tmp_meta, name=OUTPUT_BASENAME)
            print(f"[ok] appended final {len(df)} rows")

    print(f"[ok] API fill complete for {meta_tag}. fetched={fetched}")
    return

if __name__ == "__main__":
    main()
