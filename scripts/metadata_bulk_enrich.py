# ================================================================
# scripts/metadata_bulk_enrich.py — Action 4 (Bulk Metadata)
# ================================================================
from __future__ import annotations

import os
import io
import json
import time
import tempfile
import argparse
import datetime as dt
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

# ---------- HTTP session ----------
SESS = requests.Session()
SESS.headers.update({"User-Agent": "Allosaurus/1.0"})
if CH_API_KEY:
    SESS.auth = (CH_API_KEY, "")

def _req(url: str, **kw) -> requests.Response:
    """Tiny helper with soft backoff for 429s."""
    for attempt in range(4):
        r = SESS.get(url, timeout=60, **kw)
        if r.status_code != 429:
            return r
        sleep = 2 * (attempt + 1)
        print(f"[rate] 429 from {url} — sleeping {sleep}s")
        time.sleep(sleep)
    return r

# ---------- API helpers ----------
def fetch_company_profile(company_id: str) -> Dict[str, Any]:
    r = _req(f"{API_BASE}/company/{company_id}")
    r.raise_for_status()
    j = r.json()
    roa = j.get("registered_office_address") or {}
    return {
        "companies_house_registered_number": company_id,
        "entity_current_legal_name": j.get("company_name"),
        "company_type": j.get("type"),
        "company_status": j.get("company_status"),
        "incorporation_date": j.get("date_of_creation"),
        "sic_codes": j.get("sic_codes", []),
        "registered_office_postcode": roa.get("postal_code"),
        "registered_office_locality": roa.get("locality"),
    }

def fetch_officers(company_id: str, max_pages: int = 8) -> List[str]:
    names: List[str] = []
    start_index = 0
    for _ in range(max_pages):
        r = _req(f"{API_BASE}/company/{company_id}/officers",
                 params={"items_per_page": 100, "start_index": start_index})
        if not r.ok:
            break
        j = r.json()
        items = j.get("items", []) or []
        names.extend([x.get("name") for x in items if x.get("name")])
        if len(items) < 100:
            break
        start_index += 100
    # de-dupe preserving order
    seen, out = set(), []
    for n in names:
        if n not in seen:
            seen.add(n); out.append(n)
    return out

def fetch_advanced_enrichment(company_id: str, company_name: str | None) -> Dict[str, Any]:
    if not company_name:
        return {}
    params = {"company_name_includes": company_name, "size": 500, "start_index": 0}
    try:
        r = _req(f"{API_BASE}/advanced-search/companies", params=params)
        if not r.ok:
            return {}
        j = r.json()
        for it in j.get("items", []):
            if it.get("company_number") == company_id:
                ro = it.get("registered_office_address") or {}
                out = {
                    "registered_office_postcode": ro.get("postal_code"),
                    "registered_office_locality": ro.get("locality"),
                    "company_status": it.get("company_status") or None,
                }
                if it.get("sic_codes"):
                    out["sic_codes"] = it.get("sic_codes")
                return out
    except Exception:
        return {}
    return {}

# ---------- Schema ----------
META_COLUMNS = [
    "companies_house_registered_number",
    "entity_current_legal_name",
    "company_type",
    "company_status",
    "incorporation_date",
    "sic_codes",
    "officers",
    "registered_office_postcode",
    "registered_office_locality",
    "last_updated",
]

# ---------- Main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True, help="Target year (e.g., 2025)")
    ap.add_argument("--half", type=str, required=True, choices=["H1", "H2"], help="Target half")
    ap.add_argument("--refresh", action="store_true",
                    help="Ignore existing metadata asset; re-fetch everything")
    ap.add_argument("--limit", type=int, default=0,
                    help="Optional cap on number of companies to process (for testing)")
    args = ap.parse_args()

    # 1) Load company IDs from the financials release for the chosen half
    fin_tag = f"data-{args.year}-{args.half}-financials"
    print(f"[info] loading financials release: {fin_tag}")
    fin_rel = gh_release_ensure(fin_tag)
    fin_asset = gh_release_find_asset(fin_rel, "financials.parquet")
    if not fin_asset:
        print(f"[error] no financials.parquet in {fin_tag}")
        return

    fin_tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False).name
    gh_release_download_asset(fin_asset, fin_tmp)
    fin_df = pd.read_parquet(fin_tmp, columns=["companies_house_registered_number"])
    all_ids: List[str] = (
        fin_df["companies_house_registered_number"].astype(str).dropna().unique().tolist()
    )
    print(f"[info] found {len(all_ids)} company IDs in financials")

    # 2) Decide where to write metadata
    meta_tag = f"data-{args.year}-{args.half}-metadata"
    meta_rel = gh_release_ensure(meta_tag, name=f"Metadata {args.year} {args.half}")
    meta_tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False).name
    existing_ids: Set[str] = set()

    meta_asset = gh_release_find_asset(meta_rel, OUTPUT_BASENAME)
    if meta_asset and not args.refresh:
        gh_release_download_asset(meta_asset, meta_tmp)
        try:
            meta_df = pd.read_parquet(meta_tmp, columns=["companies_house_registered_number"])
            existing_ids = set(meta_df["companies_house_registered_number"].astype(str).dropna().unique())
            print(f"[info] existing metadata rows: {len(existing_ids)} (will skip these)")
        except Exception:
            pass
    else:
        print(f"[info] starting fresh metadata for {meta_tag}")

    # 3) Plan batch
    ids = [cid for cid in all_ids if (args.refresh or cid not in existing_ids)]
    if args.limit > 0:
        ids = ids[: args.limit]
    if not ids:
        print("[info] nothing to do (no new IDs)")
        return

    print(f"[info] enriching {len(ids)} companies")

    # 4) Fetch in a simple loop (could parallelize later if needed)
    rows: List[Dict[str, Any]] = []
    for i, cid in enumerate(ids, 1):
        try:
            base = fetch_company_profile(cid)
            base["officers"] = fetch_officers(cid)
            adv = fetch_advanced_enrichment(cid, base.get("entity_current_legal_name"))
            base.update({k: v for k, v in adv.items() if v is not None})
            base["last_updated"] = dt.datetime.utcnow().isoformat()
            rows.append(base)
        except requests.HTTPError as e:
            print(f"[warn] {cid}: HTTP {e.response.status_code}")
        except Exception as e:
            print(f"[warn] {cid}: {e}")

        if i % 200 == 0:
            print(f"[info] progress: {i}/{len(ids)}")

        # polite pacing
        time.sleep(0.15)

    if not rows:
        print("[warn] no metadata rows produced")
        return

    out_df = pd.DataFrame(rows, columns=META_COLUMNS)

    # 5) Append + dedupe on company id
    if meta_asset and not args.refresh:
        gh_release_download_asset(meta_asset, meta_tmp)
    append_parquet(meta_tmp, out_df, subset_keys=["companies_house_registered_number"])
    gh_release_upload_or_replace_asset(meta_rel, meta_tmp, name=OUTPUT_BASENAME)

    print(f"[ok] metadata updated: {meta_tag} (rows added: {len(out_df)})")

if __name__ == "__main__":
    main()
