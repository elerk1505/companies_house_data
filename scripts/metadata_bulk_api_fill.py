# ================================================================
# scripts/metadata_bulk_api_fill.py
#
# Fill metadata for IDs NOT covered by the snapshot, using the API.
# - Reads financials + existing metadata for a half (H1/H2)
# - Computes remaining IDs
# - Fetches Company Profile (+light Advanced Search enrichment)
# - Throttled, retries 429 via Retry-After, checkpoints every N rows
#
# NOTE ON IDS:
#   * Your financials use digits-only IDs with NO leading zeros.
#   * The API expects canonical numbers (8-digit zero-padded, or prefixed).
#   * We call the API with a padded ID but STORE the digits-only ID so it
#     matches your financials when joining in the app.
# ================================================================

from __future__ import annotations

import os
import time
import argparse
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
)

API_BASE = "https://api.company-information.service.gov.uk"
OUTPUT_BASENAME = "metadata.parquet"

# ------------------------ HTTP session + pacing ------------------------

SESS = requests.Session()
SESS.headers.update({"User-Agent": "CompaniesHouseFinder/1.0"})

CH_API_KEY = os.getenv("CH_API_KEY", "")
if CH_API_KEY:
    # Basic auth with API key as username
    SESS.auth = (CH_API_KEY, "")

# simple rolling window limiter (requests per minute)
_recent: List[float] = []


def _pace(max_rpm: int) -> None:
    now = time.time()
    # drop any calls older than 60s
    while _recent and now - _recent[0] > 60:
        _recent.pop(0)
    if len(_recent) >= max_rpm:
        sleep = 60 - (now - _recent[0]) + 0.05
        if sleep > 0:
            time.sleep(sleep)
    _recent.append(time.time())


def _get(url: str, *, max_rpm: int, **kw) -> requests.Response:
    backoff = 2.0
    for _ in range(8):
        _pace(max_rpm)
        r = SESS.get(url, timeout=60, **kw)
        if r.status_code != 429:
            return r
        # 429: respect Retry-After if present, otherwise exponential backoff
        ra = r.headers.get("Retry-After")
        wait = float(ra) if ra and ra.strip().isdigit() else backoff
        print(f"[rate] 429 {url} — sleeping {wait:.1f}s")
        time.sleep(wait)
        backoff = min(backoff * 2.0, 60.0)
    return r


# ----------------------------- ID helpers -----------------------------

def norm_ixbrl_id(x) -> str | None:
    """
    Your financials style:
      - keep digits only
      - strip leading zeros
      - return None if empty
    """
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = "".join(ch for ch in str(x).strip() if ch.isdigit())
    s = s.lstrip("0")
    return s or None


def to_api_company_number(ixbrl_id: str | None) -> str | None:
    """
    Convert your digits-only ID into what the API expects:
      - if digits only: left-pad to 8 (e.g., '88092' -> '00088092')
      - if already alphanumeric (e.g., 'SC123456'), return as-is
    """
    if not ixbrl_id:
        return None
    s = str(ixbrl_id).strip()
    if s.isdigit():
        return s.zfill(8)
    return s


# ------------------------------ API layer -----------------------------

def fetch_company_profile(api_id: str, *, max_rpm: int) -> Dict[str, Any]:
    r = _get(f"{API_BASE}/company/{api_id}", max_rpm=max_rpm)
    r.raise_for_status()
    j = r.json()
    roa = j.get("registered_office_address") or {}
    return {
        # NOTE: we overwrite companies_house_registered_number later with your digits-only id
        "companies_house_registered_number": api_id,
        "entity_current_legal_name": j.get("company_name"),
        "company_type": j.get("type"),
        "company_status": j.get("company_status"),
        "incorporation_date": j.get("date_of_creation"),
        "sic_codes": j.get("sic_codes", []),
        "registered_office_postcode": roa.get("postal_code"),
        "registered_office_post_town": roa.get("locality"),
    }


def fetch_advanced(api_id: str, company_name: str | None, *, max_rpm: int) -> Dict[str, Any]:
    """
    Optional enrichment via Advanced Search. We key by name and reconcile to api_id/digits-only.
    This is best-effort; skip quietly on errors.
    """
    if not company_name:
        return {}
    try:
        params = {"company_name_includes": company_name, "size": 200}
        r = _get(f"{API_BASE}/advanced-search/companies", params=params, max_rpm=max_rpm)
        if not r.ok:
            return {}
        items = r.json().get("items", [])
        for it in items:
            num = it.get("company_number")
            if not num:
                continue
            # Normalise both to canonical for compare
            want = api_id
            got = to_api_company_number(num)
            if got and want and got == want:
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


# -------------------------------- Main --------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--half", type=str, required=True, choices=["H1", "H2"])
    ap.add_argument("--batch-size", type=int, default=300)
    # robust default when env is unset or empty
    default_rpm_str = os.getenv("CH_MAX_RPM") or "10"
    ap.add_argument("--max-rpm", type=int, default=int(default_rpm_str))
    ap.add_argument("--limit", type=int, default=0, help="Optional cap for testing")
    args = ap.parse_args()

    if not CH_API_KEY:
        print("[warn] CH_API_KEY is not set; public endpoints may be heavily throttled or blocked.")

    # ---- Load financials IDs (both possible columns), normalise to digits-only ----
    fin_tag = f"data-{args.year}-{args.half}-financials"
    fin_rel = gh_release_ensure(fin_tag)
    fin_asset = gh_release_find_asset(fin_rel, "financials.parquet")
    if not fin_asset:
        print(f"[error] no financials.parquet in {fin_tag}")
        return

    tmp_fin = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False).name
    gh_release_download_asset(fin_asset, tmp_fin)
    fin_df = pd.read_parquet(tmp_fin)

    fin_ids_series = pd.Series(dtype=object)
    if "companies_house_registered_number" in fin_df.columns:
        fin_ids_series = pd.concat([fin_ids_series, fin_df["companies_house_registered_number"]], ignore_index=True)
    if "company_id" in fin_df.columns:
        fin_ids_series = pd.concat([fin_ids_series, fin_df["company_id"]], ignore_index=True)

    fin_ids = (
        fin_ids_series.map(norm_ixbrl_id).dropna().astype(str).unique().tolist()
    )
    fin_set: Set[str] = set(fin_ids)
    print(f"[info] financial IDs for {fin_tag}: {len(fin_set):,}")

    # ---- Load existing metadata -> compute remaining IDs to fetch ----
    meta_tag = f"data-{args.year}-{args.half}-metadata"
    meta_rel = gh_release_ensure(meta_tag, name=f"Metadata {args.year} {args.half}")

    existing: Set[str] = set()
    tmp_meta = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False).name
    meta_asset = gh_release_find_asset(meta_rel, OUTPUT_BASENAME)
    if meta_asset:
        gh_release_download_asset(meta_asset, tmp_meta)
        try:
            existing = set(
                pd.read_parquet(tmp_meta, columns=["companies_house_registered_number"])
                ["companies_house_registered_number"]
                .map(norm_ixbrl_id)
                .dropna()
                .astype(str)
                .unique()
            )
        except Exception:
            existing = set()

    remain = [cid for cid in fin_ids if cid not in existing]
    if args.limit and args.limit > 0:
        print(f"[info] TEST MODE: only processing first {args.limit} companies")
        remain = remain[: args.limit]

    if not remain:
        print("[info] nothing to fetch (all IDs already present)")
        return

    print(f"[info] to fetch via API: {len(remain):,} (rpm={args.max_rpm}, batch={args.batch_size})")

    # ---- Fetch loop with checkpointing ----
    buffer: List[Dict[str, Any]] = []
    fetched = 0

    for i, cid in enumerate(remain, 1):
        try:
            api_cid = to_api_company_number(cid)
            if not api_cid:
                continue

            # Profile (authoritative fields)
            r = _get(f"{API_BASE}/company/{api_cid}", max_rpm=args.max_rpm)
            if r.status_code == 404:
                print(f"[warn] {cid}: HTTP 404")
                continue
            r.raise_for_status()
            j = r.json()
            roa = j.get("registered_office_address") or {}

            base = {
                # STORE digits-only so it joins your financials
                "companies_house_registered_number": cid,
                "entity_current_legal_name": j.get("company_name"),
                "company_type": j.get("type"),
                "company_status": j.get("company_status"),
                "incorporation_date": j.get("date_of_creation"),
                "sic_codes": j.get("sic_codes", []),
                "registered_office_postcode": roa.get("postal_code"),
                "registered_office_post_town": roa.get("locality"),
            }

            # Optional enrichment (best effort)
            adv = fetch_advanced(api_cid, base.get("entity_current_legal_name"), max_rpm=args.max_rpm)
            if adv:
                base.update({k: v for k, v in adv.items() if v is not None})

            base["last_updated"] = dt.datetime.utcnow().isoformat()
            buffer.append(base)
            fetched += 1

        except requests.HTTPError as e:
            code = getattr(e.response, "status_code", "??")
            print(f"[warn] {cid}: HTTP {code}")
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


if __name__ == "__main__":
    main()
