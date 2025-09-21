# ================================================================
# scripts/ixbrl_bulk_parse.py — Action 3 (Bulk financials via MONTHLY archives)
# ================================================================
from __future__ import annotations

import argparse
import calendar
import datetime as dt
import tempfile
import zipfile
from typing import List

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

# Reuse parser & schema from the daily job
from scripts.ixbrl_fetch_daily import (
    TARGET_COLUMNS,
    _parse_daily_zip,  # (zip_path, zip_url, run_code) -> DataFrame
)

BASE = "https://download.companieshouse.gov.uk"
OUTPUT_BASENAME = "financials.parquet"

def month_name(month: int) -> str:
    return calendar.month_name[month]

def monthly_urls(year: int, month: int):
    mname = month_name(month)
    return [
        f"{BASE}/Accounts_Monthly_Data-{mname}{year}.zip",          # primary
        f"{BASE}/archive/Accounts_Monthly_Data-{mname}{year}.zip",  # archive
    ]

def year_bundle_url(year: int):
    if year in (2008, 2009):
        return f"{BASE}/archive/Accounts_Monthly_Data-JanuaryToDecember{year}.zip"
    return None

def http_get_to_temp(url: str, timeout: int = 1200) -> str:
    r = SESSION.get(url, timeout=timeout)
    if r.status_code == 404:
        raise FileNotFoundError(url)
    r.raise_for_status()
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as f:
        f.write(r.content)
        return f.name

def parse_month_from_year_bundle(local_zip: str, bundle_url: str, year: int, month: int) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    with zipfile.ZipFile(local_zip, "r") as z:
        for info in z.infolist():
            name = info.filename
            # daily inner pattern: Accounts_Bulk_Data-YYYY-MM-DD.zip
            if not name.lower().endswith(".zip"):
                continue
            parts = name.split("-")
            try:
                yyyy = int(parts[-3])
                mm = int(parts[-2])
            except Exception:
                continue
            if yyyy != year or mm != month:
                continue
            with z.open(info) as f:
                inner_bytes = f.read()
            with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tf:
                tf.write(inner_bytes)
                inner_path = tf.name
            run_code = f"{yyyy}-{mm:02d}"
            df = _parse_daily_zip(inner_path, f"{bundle_url}::{name}", run_code)
            if not df.empty:
                frames.append(df)
    if not frames:
        return pd.DataFrame(columns=TARGET_COLUMNS)
    return pd.concat(frames, ignore_index=True)[TARGET_COLUMNS]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True, help="Year, e.g. 2024")
    ap.add_argument("--months", type=str, required=True, help="Comma list of months, e.g. 1,2,3")
    args = ap.parse_args()

    months = [int(m.strip()) for m in args.months.split(",") if m.strip()]
    if not months:
        print("[error] no months provided")
        return

    combined: List[pd.DataFrame] = []

    for m in months:
        urls = monthly_urls(args.year, m)
        parsed = False
        for url in urls:
            try:
                print(f"[info] downloading monthly archive: {url}")
                zpath = http_get_to_temp(url)
                run_code = f"{args.year}-{m:02d}"
                df = _parse_daily_zip(zpath, url, run_code)
                print(f"[info] parsed {len(df)} rows from monthly archive {url}")
                if not df.empty:
                    combined.append(df)
                parsed = True
                break
            except FileNotFoundError:
                print(f"[warn] 404: {url}")
            except requests.HTTPError as e:
                print(f"[warn] HTTP error for {url}: {e}")
            except Exception as e:
                print(f"[warn] unexpected error for {url}: {e}")

        if parsed:
            continue

        yurl = year_bundle_url(args.year)
        if yurl:
            try:
                print(f"[info] downloading year bundle: {yurl}")
                ypath = http_get_to_temp(yurl)
                df = parse_month_from_year_bundle(ypath, yurl, args.year, m)
                print(f"[info] parsed {len(df)} rows for {args.year}-{m:02d} from year bundle")
                if not df.empty:
                    combined.append(df)
                continue
            except FileNotFoundError:
                print(f"[warn] 404: {yurl}")
            except Exception as e:
                print(f"[warn] unexpected error for year bundle {yurl}: {e}")

        print(f"[warn] no data found for {args.year}-{m:02d}")

    if not combined:
        print("[warn] no rows parsed for selected month(s)")
        return

    df_all = pd.concat(combined, ignore_index=True)[TARGET_COLUMNS]

    # Route by balance sheet or period end (fallback mid-month)
    fb = pd.Timestamp(dt.date(args.year, months[0], 15))
    ref = (
        pd.to_datetime(df_all["balance_sheet_date"], errors="coerce")
        .fillna(pd.to_datetime(df_all["period_end"], errors="coerce"))
        .fillna(fb)
    )
    years = ref.dt.year.astype(int)
    halves = ref.dt.month.apply(lambda mm: "H1" if int(mm) <= 6 else "H2")
    df_all = df_all.assign(_route=[f"{y}-{h}" for y, h in zip(years, halves)])

    for route_key, part in df_all.groupby("_route"):
        y_str, half = route_key.split("-")
        year = int(y_str)
        rel = gh_release_ensure(tag_for_financials(year, half), name=f"Financials {year} {half}")

        tmp_out = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False).name
        asset = gh_release_find_asset(rel, OUTPUT_BASENAME)
        if asset:
            gh_release_download_asset(asset, tmp_out)

        keys = ["companies_house_registered_number"]
        if "balance_sheet_date" in part and part["balance_sheet_date"].notna().any():
            keys.append("balance_sheet_date")
        else:
            keys.append("period_end")

        append_parquet(tmp_out, part.drop(columns=["_route"]), subset_keys=keys)
        gh_release_upload_or_replace_asset(rel, tmp_out, name=OUTPUT_BASENAME)

        print(f"[info] appended {len(part)} rows to Financials {year} {half}")

if __name__ == "__main__":
    main()
