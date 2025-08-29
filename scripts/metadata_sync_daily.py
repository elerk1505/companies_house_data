# ================================================================
# scripts/metadata_sync_daily.py  — Action 2 (Daily Metadata)
# ================================================================
from __future__ import annotations

import os
import io
import json
import time
import tempfile
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
    half_from_date,
)

# ---------------- Config ----------------
STATE_PATH = os.getenv("STATE_META_PATH", "state/metadata_state.json")
OUTPUT_BASENAME = "metadata.parquet"
CH_API_KEY = os.getenv("CH_API_KEY", "")

# How many recent halves of financials to scan for new IDs
FIN_HALVES_LOOKBACK = 4  # current H? + previous 3 halves

API_BASE = "https://api.company-information.service.gov.uk"

# ---------------- HTTP session ----------------
SESS = requests.Session()
SESS.headers.update({"User-Agent": "Allosaurus/1.0"})
if CH_API_KEY:
    SESS.auth = (CH_API_KEY, "")

def _req(url: str, **kw) -> requests.Response:
    """Tiny helper with soft backoff for 429."""
    for attempt in range(3):
        r = SESS.get(url, timeout=60, **kw)
        if r.status_code != 429:
            return r
        time.sleep(2 * (attempt + 1))
    return r

# ---------------- Robust state handling ----------------
os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)

def load_state(path: str) -> dict:
    try:
        if os.path.exists(path) and os.path.getsize(path) > 0:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        print(f"[warn] could not read state file ({path}): {e}; starting fresh")
    return {"seen_ids": []}

def save_state(path: str, state: dict) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, path)

# ---------------- Companies House API calls ----------------

def fetch_company_profile(company_id: str) -> Dict[str, Any]:
    url = f"{API_BASE}/company/{company_id}"
    r = _req(url)
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

def fetch_officers(company_id: str, max_pages: int = 5) -> List[str]:
    """Collect officer names with basic paging (items_per_page=100)."""
    names: List[str] = []
    start_index = 0
    for _ in range(max_pages):
        url = f"{API_BASE}/company/{company_id}/officers"
        r = _req(url, params={"items_per_page": 100, "start_index": start_index})
        if not r.ok:
            break
        j = r.json()
        items = j.get("items", []) or []
        names.extend([x.get("name") for x in items if x.get("name")])
        if len(items) < 100:
            break
        start_index += 100
    # remove dupes while keeping order
    seen = set()
    out = []
    for n in names:
        if n not in seen:
            seen.add(n); out.append(n)
    return out

def fetch_advanced_enrichment(company_id: str, company_name: str | None) -> Dict[str, Any]:
    """Advanced Search enrichment: confirm address/SIC/status by exact company_number match."""
    if not company_name:
        return {}
    params = {"company_name_includes": company_name, "size": 100, "start_index": 0}
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

# ---------------- Metadata columns ----------------
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

# ---------------- Helpers ----------------

def list_recent_financial_halves() -> List[tuple[int, str]]:
    """Return (year, half) for the current half and the previous (FIN_HALVES_LOOKBACK-1) halves."""
    today = dt.date.today()
    halves: List[tuple[int, str]] = []
    y, h = today.year, ("H1" if today.month <= 6 else "H2")
    # generate backwards
    cur_idx = 0 if h == "H1" else 1
    # Map sequential half index to (year, H)
    def idx_to_yh(idx: int) -> tuple[int, str]:
        # idx 0 = current H1, idx 1 = current H2; move backwards by idx diff
        base_half_num = (y * 2) + (0 if h == "H1" else 1)
        target_half_num = base_half_num - idx
        ty = target_half_num // 2
        th = "H1" if (target_half_num % 2 == 0) else "H2"
        return (ty, th)
    for i in range(FIN_HALVES_LOOKBACK):
        halves.append(idx_to_yh(i))
    # de-duplicate while maintaining order
    seen = set()
    ordered = []
    for yh in halves:
        if yh not in seen:
            seen.add(yh); ordered.append(yh)
    return ordered

def gather_new_company_ids(state_seen: Set[str]) -> Set[str]:
    """Open recent financial releases and gather IDs not in state."""
    new_ids: Set[str] = set()
    halves = list_recent_financial_halves()
    for year, half in halves:
        fin_tag = f"data-{year}-{half}-financials"
        try:
            fin_rel = gh_release_ensure(fin_tag)  # creates if not exists
        except Exception:
            continue
        asset = gh_release_find_asset(fin_rel, "financials.parquet")
        if not asset:
            continue
        tmp_fin = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False).name
        gh_release_download_asset(asset, tmp_fin)
        try:
            fin_df = pd.read_parquet(tmp_fin, columns=["companies_house_registered_number"])
        except Exception:
            continue
        ids = set(fin_df["companies_house_registered_number"].astype(str).unique())
        for cid in ids:
            if cid not in state_seen:
                new_ids.add(cid)
    return new_ids

# ---------------- Main ----------------

def main():
    state = load_state(STATE_PATH)
    seen_ids: Set[str] = set(state.get("seen_ids", []))

    new_ids = gather_new_company_ids(seen_ids)
    if not new_ids:
        print("[info] no new company IDs found")
        save_state(STATE_PATH, state)
        return

    print(f"[info] enriching {len(new_ids)} companies")

    rows: List[Dict[str, Any]] = []
    for cid in sorted(new_ids):
        try:
            base = fetch_company_profile(cid)
            base["officers"] = fetch_officers(cid)
            adv = fetch_advanced_enrichment(cid, base.get("entity_current_legal_name"))
            base.update({k: v for k, v in adv.items() if v is not None})
            base["last_updated"] = dt.datetime.utcnow().isoformat()
            rows.append(base)
        except requests.HTTPError as e:
            print(f"[warn] profile/officers failed for {cid}: {e}")
        except Exception as e:
            print(f"[warn] unexpected error for {cid}: {e}")

    if not rows:
        print("[warn] no metadata rows produced")
        save_state(STATE_PATH, state)
        return

    df = pd.DataFrame(rows, columns=META_COLUMNS)

    # Bucket metadata by CURRENT date (keeps rolling metadata grouped by when we learned it)
    today = dt.date.today()
    year = today.year
    half = "H1" if today.month <= 6 else "H2"

    rel = gh_release_ensure(f"data-{year}-{half}-metadata", name=f"Metadata {year} {half}")
    tmp_out = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False).name
    asset = gh_release_find_asset(rel, OUTPUT_BASENAME)
    if asset:
        gh_release_download_asset(asset, tmp_out)

    # dedupe on company id
    append_parquet(tmp_out, df, subset_keys=["companies_house_registered_number"])
    gh_release_upload_or_replace_asset(rel, tmp_out, name=OUTPUT_BASENAME)

    # update state
    state["seen_ids"] = sorted(seen_ids | set(new_ids))
    save_state(STATE_PATH, state)
    print(f"[info] metadata updated in release: Metadata {year} {half}")

if __name__ == "__main__":
    main()
