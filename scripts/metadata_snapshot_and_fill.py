# ================================================================
# scripts/metadata_snapshot_and_fill.py
#
# Metadata Refresh (Snapshot + API) with:
# - Canonical company-number normalisation (keeps SC/NI/OC… prefixes; zero-pads digits)
# - Snapshot-first merge (fast), API for remainder (time-boxed)
# - Upload hardened with retries (safe_upload)
# - Idempotent append + dedupe on companies_house_registered_number
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
from typing import Any, Dict, List, Set, Tuple

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

# --------------------------- Normalisation ---------------------------
# Accept either AA999999 (two letters + 6 digits) or 6–8 digits
_COMP_RE = re.compile(r"(?i)\b([A-Z]{2}\d{6}|\d{6,8})\b")

def canon_company_number(x) -> str | None:
    """
    Canonical CH number for matching/uploading to API:
      - Uppercase
      - Keep prefixes (SC, NI, OC, etc.)
      - If all digits, zero-pad to 8
    """
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = str(x).strip().upper()
    m = _COMP_RE.search(s)
    if not m:
        return None
    cn = m.group(1)
    if cn.isdigit():
        cn = cn.zfill(8)
    return cn

def api_company_number(x) -> str | None:
    return canon_company_number(x)

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
    """GET with pacing + simple exponential backoff for 429."""
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
    """Pick the latest monthly snapshot (1st of month). Fallback to previous month if 404/network error."""
    if today is None:
        today = dt.date.today()
    y, m = today.year, today.month
    url = onefile_url(y, m)
    try:
        r = requests.get(url, timeout=30)
        if r.status_code == 404:
            prev = (today.replace(day=1) - dt.timedelta(days=1))
            y, m = prev.year, prev.month
            url = onefile_url(y, m)
    except Exception:
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

    # Pack SIC list
    sic_cols = [c for c in ["sic1","sic2","sic3","sic4"] if c in df.columns]
    if sic_cols:
        def pack(row):
            vals = [str(row[c]).strip() for c in sic_cols if pd.notna(row[c]) and str(row[c]).strip()]
            return vals if vals else None
        df["sic_codes"] = df.apply(pack, axis=1)
        df.drop(columns=sic_cols, inplace=True)
    else:
        df["sic_codes"] = None

    # Coerce date to ISO string
    if "incorporation_date" in df.columns:
        d = pd.to_datetime(df["incorporation_date"], errors="coerce", dayfirst=False)
        df["incorporation_date"] = d.dt.strftime("%Y-%m-%d")

    # Ensure columns exist
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

# ------------------------- Safe upload wrapper ------------------------
def safe_upload(rel: dict, path: str, *, name: str, attempts: int = 6) -> None:
    """Wrap gh_release_upload_or_replace_asset with exponential backoff; soft-fail after retries."""
    delay = 1.0
    for i in range(1, attempts + 1):
        try:
            gh_release_upload_or_replace_asset(rel, path, name=name)
            return
        except Exception as e:
            if i == attempts:
                print(f"[warn] release upload failed after retries: {e}")
                return
            print(f"[warn] upload attempt {i}/{attempts} failed: {e}; sleeping {delay:.1f}s")
            time.sleep(delay)
            delay *= 1.8

# ------------------------------- Main ---------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, default=0, help="Target year (default: current)")
    ap.add_argument("--half", type=str, required=True, choices=["H1","H2"])
    ap.add_argument("--limit", type=int, default=1500, help="API cap per run; 0 = unlimited (use time budget)")
    ap.add_argument("--batch-size", type=int, default=300, help="Checkpoint size per upload")
    default_rpm = os.getenv("CH_MAX_RPM") or "12"
    ap.add_argument("--max-rpm", type=int, default=int(default_rpm))
    ap.add_argument("--time-budget-mins", type=int,
                    default=int(os.getenv("TIME_BUDGET_MINS", "350")),
                    help="Stop API fill after this many minutes (default: 350 ≈ 5h50m).")
    ap.add_argument("--no-advanced", action="store_true",
                    help="Skip the extra advanced-search call per company.")
    args = ap.parse_args()

    year = args.year or dt.date.today().year
    half = args.half
    budget_secs = max(0, args.time_budget_mins) * 60
    t0 = time.time()

    # 1) Load financial IDs (keep originals; build canon map)
    fin_tag = f"data-{year}-{half}-financials"
    fin_rel = gh_release_ensure(fin_tag)
    fin_asset = gh_release_find_asset(fin_rel, "financials.parquet")
    if not fin_asset:
        print(f"[error] no financials.parquet in {fin_tag}"); return
    tmp_fin = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False).name
    gh_release_download_asset(fin_asset, tmp_fin)
    fin_df = pd.read_parquet(tmp_fin)

    fin_raw = pd.Series(dtype=object)
    if "companies_house_registered_number" in fin_df.columns:
        fin_raw = pd.concat([fin_raw, fin_df["companies_house_registered_number"]], ignore_index=True)
    if "company_id" in fin_df.columns:
        fin_raw = pd.concat([fin_raw, fin_df["company_id"]], ignore_index=True)
    fin_raw = fin_raw.dropna().astype(str)

    canon_to_orig: Dict[str, str] = {}
    for raw in fin_raw:
        canon = canon_company_number(raw)
        if canon and canon not in canon_to_orig:
            canon_to_orig[canon] = raw
    fin_canon_set: Set[str] = set(canon_to_orig.keys())
    print(f"[info] financial IDs (unique, canon): {len(fin_canon_set):,}")

    # 2) Ensure metadata release & existing
    meta_tag = f"data-{year}-{half}-metadata"
    meta_rel = gh_release_ensure(meta_tag, name=f"Metadata {year} {half}")
    tmp_meta = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False).name
    existing_canon: Set[str] = set()
    meta_asset = gh_release_find_asset(meta_rel, OUTPUT_BASENAME)
    if meta_asset:
        gh_release_download_asset(meta_asset, tmp_meta)
        try:
            existing_vals = pd.read_parquet(tmp_meta, columns=["companies_house_registered_number"])["companies_house_registered_number"]
            existing_canon = set(filter(None, (canon_company_number(x) for x in existing_vals.dropna().astype(str))))
        except Exception:
            existing_canon = set()

    # 3) Snapshot merge (fast path)
    try:
        _, _, url = guess_latest_snapshot_url()
        print(f"[info] snapshot chosen: {url}")
        snap = load_snapshot_df(url)
        print(f"[info] snapshot rows: {len(snap):,}")
        snap_meta = build_metadata_from_snapshot(snap)

        # Build canonical column for matching
        snap_meta["_canon"] = snap_meta["companies_house_registered_number"].map(canon_company_number)

        need_canon = fin_canon_set - existing_canon
        before = len(snap_meta)
        snap_meta = snap_meta[snap_meta["_canon"].isin(need_canon)].copy()
        after = len(snap_meta)
        print(f"[info] snapshot matched: {after:,} / {before:,} rows")

        if after > 0:
            # Replace stored number with ORIGINAL financial form for perfect join
            snap_meta["companies_house_registered_number"] = snap_meta["_canon"].map(canon_to_orig)
            snap_meta.drop(columns=["_canon"], inplace=True)

            if meta_asset:
                gh_release_download_asset(meta_asset, tmp_meta)
            append_parquet(tmp_meta, snap_meta, subset_keys=["companies_house_registered_number"])
            safe_upload(meta_rel, tmp_meta, name=OUTPUT_BASENAME)
            print(f"[ok] appended snapshot rows: {after}")

            # Refresh existing_canon after append
            meta_rel = gh_release_ensure(meta_tag)
            meta_asset = gh_release_find_asset(meta_rel, OUTPUT_BASENAME)
            try:
                existing_vals = pd.read_parquet(tmp_meta, columns=["companies_house_registered_number"])["companies_house_registered_number"]
                existing_canon = set(filter(None, (canon_company_number(x) for x in existing_vals.dropna().astype(str))))
            except Exception:
                pass
    except Exception as e:
        print(f"[warn] snapshot step skipped due to error: {e}")

    # 4) API fill remaining (time-boxed, optional numeric cap)
    remain_canon = [c for c in (fin_canon_set - existing_canon)]
    if not remain_canon:
        print("[info] nothing left to fetch via API")
    else:
        # numeric cap (0 = no cap; rely on time budget)
        if args.limit and args.limit > 0 and len(remain_canon) > args.limit:
            remain_canon = remain_canon[:args.limit]
        print(f"[info] API-fill: processing {len(remain_canon)} companies")

        buffer: List[Dict[str, Any]] = []
        fetched = 0

        for i, canon in enumerate(remain_canon, 1):
            # stop near the time budget (leave ~60s for final upload/logs)
            if budget_secs and (time.time() - t0) > (budget_secs - 60):
                print(f"[info] time budget reached (~{args.time_budget_mins} min); stopping API fill at i={i}")
                break

            orig = canon_to_orig.get(canon, canon)
            try:
                api_id = api_company_number(canon)
                if not api_id:
                    continue

                r = _get(f"{API_BASE}/company/{api_id}", max_rpm=args.max_rpm)
                if r.status_code == 404:
                    print(f"[warn] {orig}: HTTP 404")
                    continue
                r.raise_for_status()
                j = r.json()
                roa = j.get("registered_office_address") or {}
                row = {
                    "companies_house_registered_number": orig,  # keep financial form for perfect join
                    "entity_current_legal_name": j.get("company_name"),
                    "company_type": j.get("type"),
                    "company_status": j.get("company_status"),
                    "incorporation_date": j.get("date_of_creation"),
                    "sic_codes": j.get("sic_codes", []),
                    "registered_office_postcode": roa.get("postal_code"),
                    "registered_office_post_town": roa.get("locality"),
                    "last_updated": pd.Timestamp.utcnow().isoformat(),
                }

                if not args.no_advanced and row.get("entity_current_legal_name"):
                    try:
                        params = {"company_name_includes": row["entity_current_legal_name"], "size": 200}
                        rr = _get(f"{API_BASE}/advanced-search/companies", params=params, max_rpm=args.max_rpm)
                        if rr.ok:
                            for it in rr.json().get("items", []):
                                num = canon_company_number(it.get("company_number"))
                                if num == canon:
                                    ro = it.get("registered_office_address") or {}
                                    if it.get("company_status"):
                                        row["company_status"] = it["company_status"]
                                    if it.get("sic_codes"):
                                        row["sic_codes"] = it["sic_codes"]
                                    if ro.get("postal_code"):
                                        row["registered_office_postcode"] = ro["postal_code"]
                                    if ro.get("locality"):
                                        row["registered_office_post_town"] = ro["locality"]
                                    break
                    except Exception:
                        pass

                buffer.append(row); fetched += 1

            except Exception as e:
                print(f"[warn] {orig}: {e}")

            # checkpoint upload
            if i % max(1, args.batch_size) == 0:
                if buffer:
                    meta_rel = gh_release_ensure(meta_tag)
                    meta_asset = gh_release_find_asset(meta_rel, OUTPUT_BASENAME)
                    if meta_asset:
                        gh_release_download_asset(meta_asset, tmp_meta)
                    df = pd.DataFrame(buffer)
                    append_parquet(tmp_meta, df, subset_keys=["companies_house_registered_number"])
                    safe_upload(meta_rel, tmp_meta, name=OUTPUT_BASENAME)
                    print(f"[info] checkpoint: appended {len(df)} rows (i={i})")
                    buffer.clear()

        # flush remaining
        if buffer:
            meta_rel = gh_release_ensure(meta_tag)
            meta_asset = gh_release_find_asset(meta_rel, OUTPUT_BASENAME)
            if meta_asset:
                gh_release_download_asset(meta_asset, tmp_meta)
            df = pd.DataFrame(buffer)
            append_parquet(tmp_meta, df, subset_keys=["companies_house_registered_number"])
            safe_upload(meta_rel, tmp_meta, name=OUTPUT_BASENAME)
            print(f"[ok] appended final {len(df)} rows")

        print(f"[ok] API fill fetched {fetched} rows")

    # 5) Final log
    try:
        total = len(pd.read_parquet(tmp_meta, columns=["companies_house_registered_number"]))
        print(f"[info] metadata release now contains {total:,} rows")
    except Exception as e:
        print(f"[warn] could not count final rows: {e}")

if __name__ == "__main__":
    main()
