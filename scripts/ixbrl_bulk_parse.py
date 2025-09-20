# scripts/ixbrl_bulk_parse.py — Robust bulk runner over monthly/year bundles
from __future__ import annotations

import re
import sys
import time
import zipfile
import tempfile
import calendar
import argparse
import datetime as dt
from typing import Dict, List, Tuple

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
DAILY_ZIP_NAME_RE = re.compile(r"Accounts_Bulk_Data-(\d{4})-(\d{2})-(\d{2})\.zip$", re.I)

def month_name(month: int) -> str:
    return calendar.month_name[month]  # "January"

def monthly_urls(year: int, month: int) -> List[str]:
    mname = month_name(month)
    return [
        f"{BASE}/Accounts_Monthly_Data-{mname}{year}.zip",         # primary
        f"{BASE}/archive/Accounts_Monthly_Data-{mname}{year}.zip", # archive
    ]

def year_bundle_url(year: int) -> str | None:
    if year in (2008, 2009):
        return f"{BASE}/archive/Accounts_Monthly_Data-JanuaryToDecember{year}.zip"
    return None

def http_get_to_temp(url: str, timeout: int = 600) -> str:
    r = SESSION.get(url, timeout=timeout)
    if r.status_code == 404:
        raise FileNotFoundError(url)
    r.raise_for_status()
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as f:
        f.write(r.content)
        return f.name

def parse_month_from_monthly_zip(local_zip: str, url: str, year: int, month: int) -> pd.DataFrame:
    run_code = f"{year}-{month:02d}"  # month provenance
    return _parse_daily_zip(local_zip, url, run_code)

def parse_month_from_year_bundle(local_zip: str, bundle_url: str, year: int, month: int) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
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
            df = _parse_daily_zip(inner_path, inner_url, run_code)
            if not df.empty:
                frames.append(df)
    if not frames:
        return pd.DataFrame(columns=TARGET_COLUMNS)
    return pd.concat(frames, ignore_index=True)[TARGET_COLUMNS]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True, help="Year, e.g. 2021")
    ap.add_argument("--months", type=str, required=True, help="Comma list of months, e.g. 1,2,3")
    args = ap.parse_args()

    months = [int(m.strip()) for m in args.months.split(",") if m.strip()]
    if not months:
        print("[error] no months provided", file=sys.stderr)
        sys.exit(2)

    combined: List[pd.DataFrame] = []

    for m in months:
        # 1) Standard monthly archives
        urls = monthly_urls(args.year, m)
        parsed = False
        for url in urls:
            try:
                print(f"[info] downloading monthly archive: {url}")
                zpath = http_get_to_temp(url)
                df = parse_month_from_monthly_zip(zpath, url, args.year, m)
                print(f"[info] parsed {len(df)} rows from monthly archive {url}")

                # ---- Diagnostics: see whether metrics populated in this batch ----
                if not df.empty:
                    numeric_cols = [
                        "tangible_fixed_assets","debtors","cash_bank_in_hand","current_assets",
                        "creditors_due_within_one_year","creditors_due_after_one_year",
                        "net_current_assets_liabilities","total_assets_less_current_liabilities",
                        "net_assets_liabilities_including_pension_asset_liability",
                        "called_up_share_capital","profit_loss_account_reserve","shareholder_funds",
                        "turnover_gross_operating_revenue","other_operating_income","cost_sales","gross_profit_loss",
                        "administrative_expenses","raw_materials_consumables","staff_costs",
                        "depreciation_other_amounts_written_off_tangible_intangible_fixed_assets",
                        "other_operating_charges_format2","operating_profit_loss",
                        "profit_loss_on_ordinary_activities_before_tax",
                        "tax_on_profit_or_loss_on_ordinary_activities","profit_loss_for_period",
                    ]
                    pres = [c for c in numeric_cols if c in df.columns]
                    if pres:
                        nn = df[pres].notna().sum().sort_values(ascending=False)
                        print(f"[diag {args.year}-{m:02d}] non-null counts (top 10):")
                        print(nn.head(10).to_string())
                # ------------------------------------------------------------------

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

        # 2) Fallback: 2008/2009 year bundle
        yurl = year_bundle_url(args.year)
        if yurl:
            try:
                print(f"[info] downloading year bundle: {yurl}")
                ypath = http_get_to_temp(yurl)
                df = parse_month_from_year_bundle(ypath, yurl, args.year, m)
                print(f"[info] parsed {len(df)} rows for {args.year}-{m:02d} from year bundle")

                # diagnostics
                if not df.empty:
                    numeric_cols = [
                        "tangible_fixed_assets","debtors","cash_bank_in_hand","current_assets",
                        "creditors_due_within_one_year","creditors_due_after_one_year",
                        "net_current_assets_liabilities","total_assets_less_current_liabilities",
                        "net_assets_liabilities_including_pension_asset_liability",
                        "called_up_share_capital","profit_loss_account_reserve","shareholder_funds",
                        "turnover_gross_operating_revenue","other_operating_income","cost_sales","gross_profit_loss",
                        "administrative_expenses","raw_materials_consumables","staff_costs",
                        "depreciation_other_amounts_written_off_tangible_intangible_fixed_assets",
                        "other_operating_charges_format2","operating_profit_loss",
                        "profit_loss_on_ordinary_activities_before_tax",
                        "tax_on_profit_or_loss_on_ordinary_activities","profit_loss_for_period",
                    ]
                    pres = [c for c in numeric_cols if c in df.columns]
                    if pres:
                        nn = df[pres].notna().sum().sort_values(ascending=False)
                        print(f"[diag {args.year}-{m:02d} bundle] non-null counts (top 10):")
                        print(nn.head(10).to_string())

                if not df.empty:
                    combined.append(df)
                continue
            except FileNotFoundError:
                print(f"[warn] 404: {yurl}")
            except Exception as e:
                print(f"[warn] unexpected error for year bundle {yurl}: {e}")

        print(f"[warn] no data found for {args.year}-{m:02d}")
        time.sleep(0.5)

    if not combined:
        print("[warn] no rows parsed for any selected month(s)")
        return

    df_all = pd.concat(combined, ignore_index=True)[TARGET_COLUMNS]

    # Route each row to release by balance_sheet_date (fallback period_end, else mid-month)
    fb = pd.Timestamp(dt.date(args.year, months[0], 15))
    ref = (
        df_all["balance_sheet_date"]
        .where(df_all["balance_sheet_date"].notna(), df_all["period_end"])
        .where(lambda s: s.notna(), fb)
    )
    ref = pd.to_datetime(ref, errors="coerce").fillna(fb)
    years = ref.dt.year.astype(int)
    halves = ref.dt.month.apply(lambda mm: "H1" if int(mm) <= 6 else "H2")
    df_all = df_all.assign(_route=[f"{y}-{h}" for y, h in zip(years.tolist(), halves.tolist())])

    # Upload into releases partitioned by (year, half)
    for route_key, part in df_all.groupby("_route"):
        y_str, half = route_key.split("-")
        year = int(y_str)
        rel = gh_release_ensure(tag_for_financials(year, half), name=f"Financials {year} {half}")

        tmp_out = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False).name
        asset = gh_release_find_asset(rel, OUTPUT_BASENAME)
        if asset:
            gh_release_download_asset(asset, tmp_out)

        # De-dupe keys: prefer (CH number, balance_sheet_date) else (CH number, period_end)
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
