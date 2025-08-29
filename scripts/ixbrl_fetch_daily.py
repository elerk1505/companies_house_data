"""
Fetch last 5 days of daily iXBRL batches from Companies House, parse, and append to the
appropriate half-year parquet stored as a GitHub Release asset. Skips already parsed files
and 404s. De-dupes on (companies_house_registered_number, period_end).

Environment:
  GITHUB_TOKEN, GH_REPO
State file (lightweight): state/financials_state.json in repo (small JSON committed back by workflow)
"""
from __future__ import annotations
import os, json, tempfile, datetime as dt
from typing import List
import pandas as pd
from scripts.common import (SESSION, gh_release_ensure, gh_release_find_asset, gh_release_download_asset,
                            gh_release_upload_or_replace_asset, append_parquet, half_from_date, tag_for_financials)

# Placeholder: point to your daily listings. Adjust to real CH endpoints/paths.
# Suppose daily ZIP index endpoint per date — you will replace this with your actual source.
DAILY_INDEX_TEMPLATE = "https://download.companieshouse.gov.uk/en_accounts_daily_{yyyymmdd}.zip"

STATE_PATH = os.getenv("STATE_PATH", "state/financials_state.json")
OUTPUT_BASENAME = "financials.parquet"  # kept inside release as asset name

# Minimal parse for demo; replace with your real iXBRL/HTML parser

def parse_daily_zip(zip_path: str) -> pd.DataFrame:
    # TODO: implement real parser; for now return empty df with the expected schema
    cols = [
        'companies_house_registered_number','entity_current_legal_name','sic_code','company_type','incorporation_date',
        'period_start','period_end','average_number_employees_during_period','turnover_gross_operating_revenue',
        'operating_profit_loss','net_assets_liabilities_including_pension_asset_liability','cash_bank_in_hand'
    ]
    return pd.DataFrame(columns=cols)


def main():
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    state = {"processed": []}
    if os.path.exists(STATE_PATH):
        state = json.load(open(STATE_PATH, "r", encoding="utf-8"))

    today = dt.date.today()
    # Check last 5 days (Tue–Sat delivery pattern considered). Include today-1..today-5
    dates = [today - dt.timedelta(days=i) for i in range(1, 6)]

    for d in dates:
        ymd = d.strftime("%Y%m%d")
        url = DAILY_INDEX_TEMPLATE.replace("{yyyymmdd}", ymd)
        if url in state.get("processed", []):
            continue
        # Download if exists (skip 404)
        r = SESSION.get(url, timeout=300)
        if r.status_code == 404:
            continue
        r.raise_for_status()
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as f:
            f.write(r.content)
            zip_path = f.name
        # Parse into DataFrame
        df = parse_daily_zip(zip_path)
        if df.empty:
            state.setdefault("processed", []).append(url)
            continue
        # Decide release / asset
        pe_col = 'period_end'
        pe = pd.to_datetime(df[pe_col], errors='coerce')
        if pe.notna().any():
            pe_date = pe.dropna().iloc[0].date()
        else:
            pe_date = d  # fallback
        year = pe_date.year
        half = half_from_date(pe_date)
        tag = tag_for_financials(year, half)
        release = gh_release_ensure(tag, name=f"Financials {year} {half}")
        # Download current asset to temp, append, re-upload
        tmp_out = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False).name
        asset = gh_release_find_asset(release, OUTPUT_BASENAME)
        if asset:
            gh_release_download_asset(asset, tmp_out)
        append_parquet(tmp_out, df, subset_keys=["companies_house_registered_number", "period_end"])
        gh_release_upload_or_replace_asset(release, tmp_out, name=OUTPUT_BASENAME)
        state.setdefault("processed", []).append(url)

    json.dump(state, open(STATE_PATH, "w", encoding="utf-8"), indent=2)

if __name__ == "__main__":
    main()
