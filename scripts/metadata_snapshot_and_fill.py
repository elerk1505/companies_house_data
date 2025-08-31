# ================================================================
# scripts/metadata_snapshot_and_fill.py
#
# Efficient metadata refresh for a half (H1/H2):
# 1) Merge Companies House monthly snapshot rows that match your
#    financials (your IDs = digits-only, no leading zeros).
# 2) API-fill remaining missing IDs with profile (+light advanced).
#
# Safe for repeated runs; it dedupes on company_id and appends.
# ================================================================

from __future__ import annotations

import argparse
import datetime as dt
import io
import os
import re
import tempfile
import time
import zipfile
from typing import Any, Dict, Iterable, List, Set, Tuple

import pandas as pd
import requests

from scripts.common import (
    gh_release_ensure,
    gh_release_find_asset,
    gh_release_download_asset,
    gh_release_upload_or_replace_asset,
    append_parquet,
)

# ----------------------------- Constants -----------------------------
API_BASE = "https://api.company-information.service.gov.uk"
OUTPUT_BASENAME = "metadata.parquet"

# --------------------------- Normalisation ---------------------------
def norm_ixbrl_id(x) -> str | None:
    """Match your iXBRL IDs: keep digits only, strip leading zeros, None if empty."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = re.sub(r"\D", "", str(x).strip())
    s = s.lstrip("0")
    return s or None

def to_api_company_number(ixbrl_id: str | None) -> str | None:
    """Convert your digits-only IDs to API canonical numbers (zero-pad to 8)."""
    if not ixbrl_id:
        return None
    s = str(ixbrl_id).strip()
    if s.isdigit():
        return s.zfill(8)
    return s

# ---------------------------- HTTP session ---------------------------
SESS = requests.Session()
SESS.headers.update({"User-Agent": "CompaniesHouseFinder/1.0"})
CH_API_KEY = os.getenv("CH_API_KEY", "")
if CH_API_KEY:
    SESS.auth = (CH_API_KEY, "")

_recent: List[float] = []
def _pace(max_rpm: int) -> None:
    now = time.time()
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
        ra = r.headers.get("Retry-After")
        wait = float(ra) if ra and ra.strip().isdigit() else backoff
        print(f"[rate] 429 {url} — sleeping {wait:.1f}s")
        time.sleep(wait)
        backoff = min(backoff * 2.0, 60.0)
    return r

# --------------------------- Snapshot helpers ------------------------
def _normalize_colname(name: str) -> str:
    name = name.replace("\ufeff", "")
    return re.sub(r"[^A-Za-z0-9]", "", name).lower()

def col(df: pd.DataFrame, *candidates: str) -> str | None:
    m = {_normalize_colname(c): c for c in df.columns}
    for cand in candidates:
        key = _normalize_colname(cand)
        if key in m:
            return m[key]
    return None

def onefile_url(year: int, month: int) -> str:
    return f"https://download.companieshouse.gov.uk/BasicCompanyDataAsOneFile-{year}-{month:02d}-01.zip"

def guess_latest_snapshot_url(today: dt.date | None = None) -> Tuple[int, int, str]:
    """Pick the latest monthly snapshot (1st of month). Fallback to previous month if 404."""
    if today is None:
        today = dt.date.today()
    # snapshot usually available within ~5 working days after month end. Use current month by default.
    y, m = today.year, today.month
    url = onefile_url(y, m)
    r = requests.get(url, timeout=30)
    if r.status_code == 404:
        # fallback to previous month
        prev = (today.replace(day=1) - dt.timedelta(days=1))
        y, m = prev.year, prev.month
        url = onefile_url(y, m)
    return y, m, url

def load_snapshot_df(url: str) -> pd.DataFrame:
    r = requests.get(url, timeout=600)
    if r.status_code == 404:
        raise FileNotFoundError(url)
    r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        csvs = [n for n in z.namelist() if n.lower().endswith(".csv")]
        if not csvs:
            raise RuntimeError("No CSV in snapshot ZIP.")
        with z.open(csvs[0]) as f:
            return pd.read_csv(f, dtype=str)

def build_metadata_from_snapshot(snap: pd.DataFrame) -> pd.DataFrame:
    cn = col(snap, "CompanyNumber")
    name = col(snap, "CompanyName")
    status = col(snap, "CompanyStatus")
    ctype = col(snap, "CompanyCategory", "CompanyType")
    incorp = col(snap, "IncorporationDate")
    post_town = col(snap, "RegAddress.PostTown")
    postcode = col(snap, "RegAddress.PostCode")
    sic1 = col(snap, "SICCode.SicText_1"); sic2 = col(snap, "SICCode.SicText_2")
    sic3 = col(snap, "SICCode.SicText_3"); sic4 = col(snap, "SICCode.SicText_4")

    need = [cn, name, status, ctype, incorp, post_town, postcode, sic1, sic2, sic3, sic4]
    cols = [c for c in need if c]
    df = snap[cols].copy()

    ren: Dict[str, str] = {}
    if cn: ren[cn] = "companies_house_registered_number"
    if name: ren[name] = "entity_current_legal_name"
    if status: ren[status] = "company_status"
    if ctype: ren[ctype] = "company_type"
    if incorp: ren[incorp] = "incorporation_date"
    if post_town: ren[post_town] = "registered_office_post_town"
    if postcode: ren[postcode] = "registered_office_postcode"
    if sic1: ren[sic1] = "sic1"
    if sic2: ren[sic2] = "sic2"
    if sic3: ren[sic3] = "sic3"
    if sic4: ren[sic4] = "sic4"
    df = df.rename(columns=ren)

    df["companies_house_registered_number"] = df["companies_house_registered_number"].map(norm_ixbrl_id)

    sic_cols = [c for c in ["sic1","sic2","sic3","sic4"] if c in df.columns]
    if sic_cols:
        def pack(row):
            vals = [str(row[c]).strip() for c in sic_cols if pd.notna(row[c]) and str(row[c]).strip()]
            return vals if vals else None
        df["sic_codes"] = df.apply(pack, axis=1)
        df.drop(columns=sic_cols, inplace=True)
    else:
        df["sic_codes"] = None

    if "incorporation_date" in df.columns:
        df["incorporation_date"] = pd.to_datetime(df["incorporation_date"], errors="coerce")

    for c in [
        "companies_house_registered_number","entity_current_legal_name","company_status",
        "company_type","incorporation_date","registered_office_post_town","registered_office_postcode",
        "sic_codes"
    ]:
        if c not in df.columns:
            df[c] = pd.NA

    df["last_updated"] = pd.Timestamp.utcnow().isoformat()
    return df[
        ["companies_house_registered_number","entity_current_legal_name","company_status","company_type",
         "incorporation_date","registered_office_post_town","registered_office_postcode","sic_codes","last_updated"]
    ]

# ----------------------------- API helpers ----------------------------
def fetch_company_profile(api_id: str, *, max_rpm: int) -> Dict[str, Any]:
    r = _get(f"{API_BASE}/company/{api_id}", max_rpm=max_rpm)
    if r.status_code == 404:
        raise requests.HTTPError("404", response=r)
    r.raise_for_status()
    j = r.json()
    roa = j.get("registered_office_address") or {}
    return {
        "entity_current_legal_name": j.get("company_name"),
        "company_type": j.get("type"),
        "company_status": j.get("company_status"),
        "incorporation_date": j.get("date_of_creation"),
        "sic_codes": j.get("sic_codes", []),
        "registered_office_postcode": roa.get("postal_code"),
        "registered_office_post_town": roa.get("locality"),
    }

def fetch_advanced(api_id: str, name: str | None, *, max_rpm: int) -> Dict[str, Any]:
    if not name:
        return {}
    try:
        params = {"company_name_includes": name, "size": 200}
        r = _get(f"{API_BASE}/advanced-search/companies", params=params, max_rpm=max_rpm)
        if not r.ok:
            return {}
        for it in r.json().get("items", []):
            num = it.get("company_number")
            got = to_api_company_number(num) if num else None
            if got and got == api_id:
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

# ------------------------------- Main ---------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, default=0, help="Target year (default: current)")
    ap.add_argument("--half", type=str, required=True, choices=["H1","H2"])
    ap.add_argument("--limit", type=int, default=1500, help="API cap per run")
    ap.add_argument("--batch-size", type=int, default=300)
    default_rpm = os.getenv("CH_MAX_RPM") or "10"
    ap.add_argument("--max-rpm", type=int, default=int(default_rpm))
    args = ap.parse_args()

    # Year default = current
    year = args.year or dt.date.today().year
    half = args.half

    # 1) Load financial IDs (digits-only)
    fin_tag = f"data-{year}-{half}-financials"
    fin_rel = gh_release_ensure(fin_tag)
    fin_asset = gh_release_find_asset(fin_rel, "financials.parquet")
    if not fin_asset:
        print(f"[error] no financials.parquet in {fin_tag}"); return
    tmp_fin = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False).name
    gh_release_download_asset(fin_asset, tmp_fin)
    fin_df = pd.read_parquet(tmp_fin)
    fin_ids_series = pd.Series(dtype=object)
    if "companies_house_registered_number" in fin_df.columns:
        fin_ids_series = pd.concat([fin_ids_series, fin_df["companies_house_registered_number"]], ignore_index=True)
    if "company_id" in fin_df.columns:
        fin_ids_series = pd.concat([fin_ids_series, fin_df["company_id"]], ignore_index=True)
    fin_ids = fin_ids_series.map(norm_ixbrl_id).dropna().astype(str).unique().tolist()
    fin_set: Set[str] = set(fin_ids)
    print(f"[info] financial IDs in {fin_tag}: {len(fin_set):,}")

    # 2) Ensure metadata release
    meta_tag = f"data-{year}-{half}-metadata"
    meta_rel = gh_release_ensure(meta_tag, name=f"Metadata {year} {half}")
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

    # 3) Snapshot merge (fast path)
    y_s, m_s, url = guess_latest_snapshot_url()
    print(f"[info] snapshot chosen: {url}")
    snap = load_snapshot_df(url)
    print(f"[info] snapshot rows: {len(snap):,}")
    snap_meta = build_metadata_from_snapshot(snap)

    # Only those in financials and not already present
    before = len(snap_meta)
    snap_meta = snap_meta[
        snap_meta["companies_house_registered_number"].isin(fin_set - existing)
    ]
    after = len(snap_meta)
    print(f"[info] snapshot matched: {after:,} / {before:,} rows")

    if after > 0:
        # Append snapshot rows
        if meta_asset:
            gh_release_download_asset(meta_asset, tmp_meta)
        append_parquet(tmp_meta, snap_meta, subset_keys=["companies_house_registered_number"])
        gh_release_upload_or_replace_asset(meta_rel, tmp_meta, name=OUTPUT_BASENAME)
        print(f"[ok] appended snapshot rows: {after}")

        # refresh 'existing' set
        existing = set(
            pd.read_parquet(tmp_meta, columns=["companies_house_registered_number"])
            ["companies_house_registered_number"].map(norm_ixbrl_id).dropna().astype(str).unique()
        )

    # 4) API fill remaining (capped)
    remain = [cid for cid in fin_ids if cid not in existing]
    if not remain:
        print("[info] nothing left to fetch via API")
    else:
        cap = max(0, args.limit or 0)
        if cap > 0:
            remain = remain[:cap]
            print(f"[info] API-fill: processing first {cap} companies")
        else:
            print(f"[info] API-fill: processing all {len(remain):,} companies")

        buffer: List[Dict[str, Any]] = []
        fetched = 0

        for i, cid in enumerate(remain, 1):
            try:
                api_cid = to_api_company_number(cid)
                if not api_cid:
                    continue
                # Profile
                r = _get(f"{API_BASE}/company/{api_cid}", max_rpm=args.max_rpm)
                if r.status_code == 404:
                    print(f"[warn] {cid}: HTTP 404")
                    continue
                r.raise_for_status()
                j = r.json()
                roa = j.get("registered_office_address") or {}
                row = {
                    "companies_house_registered_number": cid,  # store in your digits-only key
                    "entity_current_legal_name": j.get("company_name"),
                    "company_type": j.get("type"),
                    "company_status": j.get("company_status"),
                    "incorporation_date": j.get("date_of_creation"),
                    "sic_codes": j.get("sic_codes", []),
                    "registered_office_postcode": roa.get("postal_code"),
                    "registered_office_post_town": roa.get("locality"),
                    "last_updated": pd.Timestamp.utcnow().isoformat(),
                }
                # Optional enrichment
                adv = fetch_advanced(api_cid, row.get("entity_current_legal_name"), max_rpm=args.max_rpm)
                if adv:
                    row.update({k: v for k, v in adv.items() if v is not None})

                buffer.append(row); fetched += 1
            except Exception as e:
                print(f"[warn] {cid}: {e}")

            # checkpoint
            if i % max(1, args.batch_size) == 0:
                df = pd.DataFrame(buffer)
                if not df.empty:
                    if meta_asset:
                        gh_release_download_asset(meta_asset, tmp_meta)
                    append_parquet(tmp_meta, df, subset_keys=["companies_house_registered_number"])
                    gh_release_upload_or_replace_asset(meta_rel, tmp_meta, name=OUTPUT_BASENAME)
                    print(f"[info] checkpoint: appended {len(df)} rows (i={i})")
                    buffer.clear()

        # flush remaining
        if buffer:
            df = pd.DataFrame(buffer)
            if not df.empty:
                if meta_asset:
                    gh_release_download_asset(meta_asset, tmp_meta)
                append_parquet(tmp_meta, df, subset_keys=["companies_house_registered_number"])
                gh_release_upload_or_replace_asset(meta_rel, tmp_meta, name=OUTPUT_BASENAME)
                print(f"[ok] appended final {len(df)} rows")

        print(f"[ok] API fill fetched {fetched} rows")

    # 5) Log final total
    try:
        final_df = pd.read_parquet(tmp_meta, columns=["companies_house_registered_number"])
        total = len(final_df)
        print(f"[info] metadata release now contains {total:,} rows")
    except Exception as e:
        print(f"[warn] could not count final rows: {e}")

if __name__ == "__main__":
    main()
