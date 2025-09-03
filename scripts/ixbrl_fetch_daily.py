# ================================================================
# scripts/ixbrl_fetch_daily.py  — Action 1 (Daily Financials)
# ================================================================
"""
Fetch the last N days (including today) of Companies House daily accounts ZIPs
and append parsed financials into the correct half-year Parquet stored as a
GitHub Release asset.

• Includes TODAY if available (fix for being "one day behind")
• DAYS_LOOKBACK env var (default 7) controls the rolling window
• HEAD probe avoids downloading 404s
• Skips already-processed URLs via state/financials_state.json
• De-dupes on (company_id, balance_sheet_date) else (company_id, period_end)
• Handles nested ZIPs that contain iXBRL HTML files

Requires: pandas, pyarrow, requests, (optional) ixbrlparse
"""

from __future__ import annotations

import io
import json
import os
import re
import tempfile
import datetime as dt
import zipfile
from typing import Dict, List

import pandas as pd
import requests

from scripts.common import (
    SESSION,
    gh_release_ensure,
    gh_release_find_asset,
    gh_release_download_asset,
    gh_release_upload_or_replace_asset,
    append_parquet,
    half_from_date,
    tag_for_financials,
)

# --- Constants & settings ---
DAILY_FILE_FMT = "Accounts_Bulk_Data-{date}.zip"   # e.g. 2025-09-03
DAILY_BASE_URL = "https://download.companieshouse.gov.uk/"
OUTPUT_BASENAME = "financials.parquet"
STATE_PATH = os.getenv("STATE_PATH", "state/financials_state.json")
DAYS_LOOKBACK = int(os.getenv("DAYS_LOOKBACK", "7"))  # inclusive of today

# Optional iXBRL parser
TRY_IXBRLPARSE = True
try:
    import ixbrlparse  # type: ignore
except Exception:
    TRY_IXBRLPARSE = False

# Target output columns (wide schema)
TARGET_COLUMNS = [
    # context / provenance
    "run_code", "company_id", "date", "file_type", "taxonomy", "balance_sheet_date", "zip_url", "error",
    # core identifiers
    "companies_house_registered_number", "entity_current_legal_name", "company_dormant",
    # period
    "period_start", "period_end",
    # workforce
    "average_number_employees_during_period",
    # balance sheet
    "tangible_fixed_assets", "debtors", "cash_bank_in_hand", "current_assets",
    "creditors_due_within_one_year", "creditors_due_after_one_year", "net_current_assets_liabilities",
    "total_assets_less_current_liabilities", "net_assets_liabilities_including_pension_asset_liability",
    "called_up_share_capital", "profit_loss_account_reserve", "shareholder_funds",
    # P&L
    "turnover_gross_operating_revenue", "other_operating_income", "cost_sales", "gross_profit_loss",
    "administrative_expenses", "raw_materials_consumables", "staff_costs",
    "depreciation_other_amounts_written_off_tangible_intangible_fixed_assets",
    "other_operating_charges_format2", "operating_profit_loss",
    "profit_loss_on_ordinary_activities_before_tax",
    "tax_on_profit_or_loss_on_ordinary_activities", "profit_loss_for_period",
    # misc (metadata-like fields sometimes present)
    "sic_code", "company_type", "incorporation_date",
]

# Normalizer for concept names
_norm = lambda s: "".join(ch for ch in s.lower() if ch.isalnum())

ALIASES = {
    # P&L
    "turnover": "turnover_gross_operating_revenue",
    "revenue": "turnover_gross_operating_revenue",
    "otheroperatingincome": "other_operating_income",
    "costofsales": "cost_sales",
    "grossprofitloss": "gross_profit_loss",
    "administrativeexpenses": "administrative_expenses",
    "rawmaterialsconsumables": "raw_materials_consumables",
    "staffcosts": "staff_costs",
    "depreciationotheramountswrittenofftangibleintangiblefixedassets":
        "depreciation_other_amounts_written_off_tangible_intangible_fixed_assets",
    "otheroperatingchargesformat2": "other_operating_charges_format2",
    "operatingprofitloss": "operating_profit_loss",
    "profitlossonordinaryactivitiesbeforetax": "profit_loss_on_ordinary_activities_before_tax",
    "taxonprofitorlossonordinaryactivities": "tax_on_profit_or_loss_on_ordinary_activities",
    "profitlossforperiod": "profit_loss_for_period",
    # Balance sheet
    "tangiblefixedassets": "tangible_fixed_assets",
    "debtors": "debtors",
    "cashbankinhand": "cash_bank_in_hand",
    "currentassets": "current_assets",
    "creditorsduewithinoneyear": "creditors_due_within_one_year",
    "creditorsdueafteroneyear": "creditors_due_after_one_year",
    "netcurrentassetsliabilities": "net_current_assets_liabilities",
    "totalassetslesscurrentliabilities": "total_assets_less_current_liabilities",
    "netassetsliabilitiesincludingpensionassetliability":
        "net_assets_liabilities_including_pension_asset_liability",
    "calledupsharecapital": "called_up_share_capital",
    "profitlossaccountreserve": "profit_loss_account_reserve",
    "shareholderfunds": "shareholder_funds",
    # Workforce
    "averagenumberemployeesduringperiod": "average_number_employees_during_period",
}

NAME_KEYS = ("entityCurrentLegalName", "EntityCurrentLegalName")
NUM_KEYS  = ("companiesHouseRegisteredNumber", "CompaniesHouseRegisteredNumber")
PERIOD_START_KEYS = ("periodStart", "PeriodStart", "period_start")
PERIOD_END_KEYS   = ("periodEnd", "PeriodEnd", "period_end")
BALSHEET_KEYS     = ("balanceSheetDate", "BalanceSheetDate", "balance_sheet_date")
DORMANT_KEYS      = ("companyDormant", "CompanyDormant", "dormant", "isDormant")
COMP_TYPE_KEYS    = ("companyType", "type")
SIC_KEYS          = ("sicCode", "sicCodes", "sic_code")
TAXONOMY_KEYS     = ("taxonomy", "taxonomyName", "taxonomyVersion", "documentType")

CH_NUM_RE = re.compile(rb"[^0-9]([0-9]{8})[^0-9]")

def _extract_from_ixbrl_bytes(data: bytes, zip_url: str, run_code: str) -> Dict[str, object]:
    out: Dict[str, object] = {
        "zip_url": zip_url,
        "run_code": run_code,
        "file_type": "ixbrl-html",
        "error": "",
    }

    facts = None
    if TRY_IXBRLPARSE:
        try:
            facts = ixbrlparse.parse(io.BytesIO(data))  # type: ignore
        except Exception as e:
            out["error"] = f"ixbrlparse:{type(e).__name__}"

    if isinstance(facts, dict) and facts:
        for k in NAME_KEYS:
            if k in facts: out["entity_current_legal_name"] = facts[k]; break
        for k in NUM_KEYS:
            if k in facts: out["companies_house_registered_number"] = str(facts[k]); break
        for k in PERIOD_START_KEYS:
            if k in facts: out["period_start"] = facts[k]; break
        for k in PERIOD_END_KEYS:
            if k in facts: out["period_end"] = facts[k]; break
        for k in BALSHEET_KEYS:
            if k in facts: out["balance_sheet_date"] = facts[k]; break
        for k in DORMANT_KEYS:
            if k in facts: out["company_dormant"] = facts[k]; break
        for k in COMP_TYPE_KEYS:
            if k in facts: out["company_type"] = facts[k]; break
        for k in SIC_KEYS:
            if k in facts:
                v = facts[k]
                out["sic_code"] = v[0] if isinstance(v, (list, tuple)) and v else v
                break
        for k in TAXONOMY_KEYS:
            if k in facts: out["taxonomy"] = str(facts[k]); break

        for key, val in facts.items():
            if isinstance(val, (int, float, str)):
                norm = _norm(key)
                if norm in ALIASES:
                    out[ALIASES[norm]] = val

    if "companies_house_registered_number" not in out:
        m = CH_NUM_RE.search(data)
        if m:
            out["companies_house_registered_number"] = m.group(1).decode("utf-8")

    out["date"] = run_code
    if "companies_house_registered_number" in out:
        out["company_id"] = out["companies_house_registered_number"]
    return out

def _parse_daily_zip(zip_path: str, zip_url: str, run_code: str) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    with zipfile.ZipFile(zip_path, "r") as z:
        for info in z.infolist():
            name = info.filename.lower()
            if name.endswith(".zip"):
                with z.open(info) as f:
                    inner = f.read()
                try:
                    with zipfile.ZipFile(io.BytesIO(inner), "r") as nz:
                        for ninfo in nz.infolist():
                            if ninfo.filename.lower().endswith((".html", ".htm", ".xhtml")):
                                with nz.open(ninfo) as nf:
                                    doc = nf.read()
                                rec = _extract_from_ixbrl_bytes(doc, zip_url, run_code)
                                if rec:
                                    rows.append(rec)
                except zipfile.BadZipFile:
                    continue
            elif name.endswith((".html", ".htm", ".xhtml")):
                with z.open(info) as f:
                    doc = f.read()
                rec = _extract_from_ixbrl_bytes(doc, zip_url, run_code)
                if rec:
                    rows.append(rec)

    if not rows:
        return pd.DataFrame(columns=TARGET_COLUMNS)

    df = pd.DataFrame(rows)

    for c in TARGET_COLUMNS:
        if c not in df.columns:
            df[c] = pd.NA

    for c in ["period_start", "period_end", "incorporation_date", "balance_sheet_date", "date"]:
        df[c] = pd.to_datetime(df[c], errors="coerce")

    NUMS = [
        "tangible_fixed_assets", "debtors", "cash_bank_in_hand", "current_assets",
        "creditors_due_within_one_year", "creditors_due_after_one_year", "net_current_assets_liabilities",
        "total_assets_less_current_liabilities", "net_assets_liabilities_including_pension_asset_liability",
        "called_up_share_capital", "profit_loss_account_reserve", "shareholder_funds",
        "turnover_gross_operating_revenue", "other_operating_income", "cost_sales", "gross_profit_loss",
        "administrative_expenses", "raw_materials_consumables", "staff_costs",
        "depreciation_other_amounts_written_off_tangible_intangible_fixed_assets",
        "other_operating_charges_format2", "operating_profit_loss",
        "profit_loss_on_ordinary_activities_before_tax",
        "tax_on_profit_or_loss_on_ordinary_activities", "profit_loss_for_period",
        "average_number_employees_during_period",
    ]
    for c in NUMS:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["companies_house_registered_number"])
    return df[TARGET_COLUMNS]

def _head_exists(url: str) -> bool:
    """Fast existence probe to avoid downloading 404s."""
    try:
        r = SESSION.head(url, timeout=30, allow_redirects=True)
        if r.status_code == 200:
            return True
        if r.status_code == 405:
            # some origins don’t allow HEAD reliably; fall back to GET with stream
            g = SESSION.get(url, stream=True, timeout=30)
            try:
                return g.status_code == 200
            finally:
                g.close()
        return False
    except requests.RequestException:
        return False

def main():
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    state = {"processed": []}
    if os.path.exists(STATE_PATH):
        state = json.load(open(STATE_PATH, "r", encoding="utf-8"))

    today = dt.date.today()

    # NEW: include today, then look back N-1 days
    check_dates = [today - dt.timedelta(days=i) for i in range(0, max(1, DAYS_LOOKBACK))]
    # newest first so today's file gets priority
    check_dates.sort(reverse=True)

    for d in check_dates:
        date_str = d.strftime("%Y-%m-%d")
        fname = DAILY_FILE_FMT.format(date=date_str)
        url = f"{DAILY_BASE_URL}{fname}"

        if url in state.get("processed", []):
            print(f"[skip] already processed {url}")
            continue

        if not _head_exists(url):
            print(f"[skip] {url} -> 404 (HEAD)")
            continue

        r = SESSION.get(url, timeout=600)
        r.raise_for_status()
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as f:
            f.write(r.content)
            zpath = f.name

        run_code = date_str  # provenance
        df = _parse_daily_zip(zpath, url, run_code)
        print(f"[info] parsed {url}: {len(df)} rows")

        if df.empty:
            print(f"[warn] no rows parsed from {url}; leaving unprocessed")
            continue  # don’t mark as processed so we can retry later

        # Pick release using balance_sheet_date, else period_end, else zip date
        if df["balance_sheet_date"].notna().any():
            ref = df["balance_sheet_date"].dropna().iloc[0]
        elif df["period_end"].notna().any():
            ref = df["period_end"].dropna().iloc[0]
        else:
            ref = pd.Timestamp(d)

        ref_date = pd.to_datetime(ref, errors="coerce").date()
        year = ref_date.year
        half = half_from_date(ref_date) or ("H1" if ref_date.month <= 6 else "H2")

        rel = gh_release_ensure(tag_for_financials(year, half), name=f"Financials {year} {half}")

        tmp_out = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False).name
        asset = gh_release_find_asset(rel, OUTPUT_BASENAME)
        if asset:
            gh_release_download_asset(asset, tmp_out)

        before = 0
        if os.path.exists(tmp_out) and os.path.getsize(tmp_out) > 0:
            try:
                before = len(pd.read_parquet(tmp_out))
            except Exception as e:
                print(f"[warn] could not read existing parquet ({e}); starting fresh")

        # dedupe on (company_id, balance_sheet_date|period_end)
        key_date = "balance_sheet_date" if df["balance_sheet_date"].notna().any() else "period_end"
        dedupe_keys = ["companies_house_registered_number", key_date]

        append_parquet(tmp_out, df, subset_keys=dedupe_keys)
        after = len(pd.read_parquet(tmp_out))
        print(f"[info] appended to {year} {half}: {before} -> {after} rows")

        # Upload/replace asset & mark processed only after successful append
        gh_release_upload_or_replace_asset(rel, tmp_out, OUTPUT_BASENAME)
        state.setdefault("processed", []).append(url)

    json.dump(state, open(STATE_PATH, "w", encoding="utf-8"), indent=2)

if __name__ == "__main__":
    main()
