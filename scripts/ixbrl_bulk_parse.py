# ================================================================
# scripts/ixbrl_bulk_parse.py — Bulk Financials with speed knobs
# ================================================================
from __future__ import annotations

import os
import re
import io
import sys
import time
import zipfile
import tempfile
import calendar
import argparse
import datetime as dt
from typing import List, Tuple, Optional

import pandas as pd
import requests

from scripts.common import (
    SESSION,
    gh_release_ensure,
    gh_release_find_asset,
    gh_release_download_asset,
    gh_release_upload_or_replace_asset,
    append_parquet,
    tag_for_financials,
)

# Reuse the daily parser + schema.
# _parse_daily_zip(zip_path, zip_url, run_code) -> DataFrame[TARGET_COLUMNS]
from scripts.ixbrl_fetch_daily import TARGET_COLUMNS, _parse_daily_zip

BASE = "https://download.companieshouse.gov.uk"
OUTPUT_BASENAME = "financials.parquet"

DAILY_ZIP_NAME_RE = re.compile(r"Accounts_Bulk_Data-(\d{4})-(\d{2})-(\d{2})\.zip$", re.I)

# ----------------------------- URL helpers -----------------------------

def month_name(month: int) -> str:
    return calendar.month_name[int(month)]  # 'January', ...

def monthly_urls(year: int, month: int) -> List[str]:
    """Likely locations for the monthly archive (prefer primary, then archive)."""
    mname = month_name(month)
    return [
        f"{BASE}/Accounts_Monthly_Data-{mname}{year}.zip",
        f"{BASE}/archive/Accounts_Monthly_Data-{mname}{year}.zip",
    ]

def year_bundle_url(year: int) -> Optional[str]:
    """2008/2009 have a single JanuaryToDecember bundle."""
    if year in (2008, 2009):
        return f"{BASE}/archive/Accounts_Monthly_Data-JanuaryToDecember{year}.zip"
    return None

# ----------------------------- Networking ------------------------------

def http_get_to_temp(
    url: str,
    timeout: int = 600,
    cache_dir: Optional[str] = None,
) -> str:
    """
    Download URL to a file; if cache_dir is provided, reuse cached copy.
    Returns local path to the zip.
    """
    if cache_dir:
        import hashlib, os
        os.makedirs(cache_dir, exist_ok=True)
        fname = hashlib.sha1(url.encode("utf-8")).hexdigest() + ".zip"
        cpath = os.path.join(cache_dir, fname)
        if os.path.exists(cpath) and os.path.getsize(cpath) > 0:
            print(f"[cache] hit for {url} -> {cpath}")
            return cpath

    r = SESSION.get(url, timeout=timeout)
    if r.status_code == 404:
        raise FileNotFoundError(url)
    r.raise_for_status()

    if cache_dir:
        with open(cpath, "wb") as f:
            f.write(r.content)
        return cpath

    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as f:
        f.write(r.content)
        return f.name

# --------------------------- Month parsing -----------------------------

def parse_month_from_monthly_zip(
    local_zip: str,
    url: str,
    year: int,
    month: int,
    limit_files: Optional[int],
    max_workers: int,
) -> pd.DataFrame:
    """
    Delegate to the daily zip parser (it already handles nested daily zips or flat HTMLs).
    We just pass speed hints via environment variables so _parse_daily_zip can use them.
    """
    # Provenance label (coarser grain for monthly runs)
    run_code = f"{year}-{int(month):02d}"

    # Speed hints for the daily parser (safe if ignored)
    os.environ["IXBRL_LIMIT_FILES"] = str(limit_files or "")
    os.environ["IXBRL_MAX_WORKERS"] = str(max_workers or "")
    os.environ["IXBRL_RUN_CODE"] = run_code

    print(f"[info] parsing month {year}-{int(month):02d} from {url}")
    df = _parse_daily_zip(local_zip, url, run_code)
    print(f"[info] parsed {len(df)} rows for {year}-{int(month):02d}")
    return df

def parse_month_from_year_bundle(
    local_zip: str,
    bundle_url: str,
    year: int,
    month: int,
    limit_files: Optional[int],
    max_workers: int,
) -> pd.DataFrame:
    """
    2008/2009 special bundle: filter inner *daily* zips to the requested month and
    parse each via the daily parser. We still pass speed hints via env.
    """
    frames: List[pd.DataFrame] = []
    picked = 0

    with zipfile.ZipFile(local_zip, "r") as z:
        for info in z.infolist():
            name = info.filename
            m = DAILY_ZIP_NAME_RE.search(name)
            if not m:
                continue
            yy, mm, dd = int(m.group(1)), int(m.group(2)), int(m.group(3))
            if yy != year or mm != month:
                continue

            with z.open(info) as f:
                inner_bytes = f.read()
            with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tf:
                tf.write(inner_bytes)
                inner_path = tf.name

            inner_url = f"{bundle_url}::{name}"
            run_code = f"{yy}-{mm:02d}-{dd:02d}"

            os.environ["IXBRL_LIMIT_FILES"] = str(limit_files or "")
            os.environ["IXBRL_MAX_WORKERS"] = str(max_workers or "")
            os.environ["IXBRL_RUN_CODE"] = run_code

            df = _parse_daily_zip(inner_path, inner_url, run_code)
            picked += 1
            if not df.empty:
                frames.append(df)

            # Soft short-circuit: if limit_files is very small, one daily zip is often plenty
            if limit_files and limit_files < 1000 and picked >= 2:
                break

    if not frames:
        return pd.DataFrame(columns=TARGET_COLUMNS)
    out = pd.concat(frames, ignore_index=True)
    print(f"[info] parsed {len(out)} rows for {year}-{int(month):02d} from year bundle")
    return out[TARGET_COLUMNS]

# ----------------------------- Routing --------------------------------

def route_half_from_date(ts) -> str:
    try:
        m = pd.to_datetime(ts, errors="coerce").month
        return "H1" if int(m) <= 6 else "H2"
    except Exception:
        return "H1"

# ------------------------------ Main ----------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True, help="Year (e.g. 2025)")
    ap.add_argument("--months", type=str, required=True, help="Comma list: 1,2,3")
    ap.add_argument("--limit-files", type=int, default=None,
                    help="Stop after N inner iXBRL files (speed).")
    ap.add_argument("--max-workers", type=int, default=8,
                    help="Concurrent parses inside the daily parser (hint).")
    ap.add_argument("--cache-dir", type=str, default=None,
                    help="Directory to cache downloaded monthly zips.")
    args = ap.parse_args()

    months = [int(m.strip()) for m in args.months.split(",") if m.strip()]
    if not months:
        print("[error] no months provided")
        sys.exit(2)

    combined: List[pd.DataFrame] = []

    for m in months:
        # Try standard monthly URLs first
        urls = monthly_urls(args.year, m)
        parsed_this_month = False

        for u in urls:
            try:
                print(f"[info] downloading monthly archive: {u}")
                zpath = http_get_to_temp(u, cache_dir=args.cache_dir)
                df = parse_month_from_monthly_zip(
                    zpath, u, args.year, m, args.limit_files, args.max_workers
                )
                if not df.empty:
                    combined.append(df[TARGET_COLUMNS])
                parsed_this_month = True
                break
            except FileNotFoundError:
                print(f"[warn] 404: {u}")
            except requests.HTTPError as e:
                print(f"[warn] HTTP error for {u}: {e}")
            except Exception as e:
                print(f"[warn] unexpected error for {u}: {e}")

        if parsed_this_month:
            continue

        # Fallback for 2008/2009 year bundle
        yurl = year_bundle_url(args.year)
        if yurl:
            try:
                print(f"[info] downloading year bundle: {yurl}")
                ypath = http_get_to_temp(yurl, cache_dir=args.cache_dir)
                df = parse_month_from_year_bundle(
                    ypath, yurl, args.year, m, args.limit_files, args.max_workers
                )
                if not df.empty:
                    combined.append(df[TARGET_COLUMNS])
                continue
            except FileNotFoundError:
                print(f"[warn] 404: {yurl}")
            except Exception as e:
                print(f"[warn] unexpected error for bundle {yurl}: {e}")

        print(f"[warn] no data found for {args.year}-{m:02d}")
        time.sleep(0.2)  # be polite

    if not combined:
        print("[warn] no rows parsed for any selected month(s)")
        return

    df_all = pd.concat(combined, ignore_index=True)
    if df_all.empty:
        print("[warn] combined result is empty")
        return

    # Route releases by balance_sheet_date -> (year, half). Fallback to period_end, else mid-month.
    fb = pd.Timestamp(dt.date(args.year, months[0], 15))
    ref = (
        df_all["balance_sheet_date"]
        .where(df_all["balance_sheet_date"].notna(), df_all.get("period_end"))
        .where(lambda s: s.notna(), fb)
    )
    ref = pd.to_datetime(ref, errors="coerce").fillna(fb)

    df_all = df_all.assign(
        _route_year=ref.dt.year.astype(int),
        _route_half=ref.dt.month.apply(lambda mm: "H1" if int(mm) <= 6 else "H2"),
    )
    df_all["_route"] = df_all["_route_year"].astype(str) + "-" + df_all["_route_half"]

    # Append grouped by (year, half) with de-dupe
    total_appended = 0
    for route_key, part in df_all.groupby("_route"):
        y_str, half = route_key.split("-")
        year = int(y_str)

        rel = gh_release_ensure(tag_for_financials(year, half), name=f"Financials {year} {half}")

        tmp_out = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False).name
        asset = gh_release_find_asset(rel, OUTPUT_BASENAME)
        if asset:
            # warm local file for append/dedupe
            gh_release_download_asset(asset, tmp_out)

        # de-dupe keys: prefer (company_id, balance_sheet_date) else (company_id, period_end)
        keys = ["companies_house_registered_number"]
        if "balance_sheet_date" in part and part["balance_sheet_date"].notna().any():
            keys.append("balance_sheet_date")
        else:
            keys.append("period_end")

        part = part.drop(columns=[c for c in ("_route", "_route_year", "_route_half") if c in part.columns])

        append_parquet(tmp_out, part, subset_keys=keys)
        gh_release_upload_or_replace_asset(rel, tmp_out, name=OUTPUT_BASENAME)

        total_appended += len(part)
        print(f"[info] appended {len(part)} rows to Financials {year} {half}")

    print(f"[done] appended {total_appended} rows across {df_all['_route'].nunique()} releases")

if __name__ == "__main__":
    main()
