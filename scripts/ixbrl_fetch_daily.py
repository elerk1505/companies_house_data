# ================================================================
# scripts/ixbrl_fetch_daily.py  — Action 1 (Daily Financials)
# ================================================================
"""
Fetch the last 5 days of Companies House daily accounts ZIPs and append parsed
financials into the correct half-year Parquet stored as a GitHub Release asset.

• Skips 404s automatically (some dates won't exist yet)
• Skips already-processed URLs via state/financials_state.json (committed by the workflow)
• De-dupes on (company_id, balance_sheet_date) if present; else (company_id, period_end)
• Handles nested ZIPs that contain iXBRL HTML files
• Buckets by balance_sheet_date -> year/H1|H2 (fallback to period_end, then zip date)

Requirements in your workflow step:
    python -m pip install -U pip pandas pyarrow requests ixbrlparse

Depends on helper utilities in scripts/common.py:
  - SESSION, gh_release_ensure, gh_release_find_asset, gh_release_download_asset,
    gh_release_upload_or_replace_asset, append_parquet, half_from_date, tag_for_financials
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

# --- Constants ---
DAILY_FILE_FMT = "Accounts_Bulk_Data-{date}.zip"   # e.g. 2025-08-29
DAILY_BASE_URL = "https://download.companieshouse.gov.uk/"
OUTPUT_BASENAME = "financials.parquet"
STATE_PATH = os.getenv("STATE_PATH", "state/financials_state.json")

# Try fast HTML iXBRL parsing; otherwise we'll use lightweight fallbacks
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

# Likely concept aliases -> our target columns
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

# Common keys to probe directly from parsed facts
NAME_KEYS = ("entityCurrentLegalName", "EntityCurrentLegalName")
NUM_KEYS  = ("companiesHouseRegisteredNumber", "CompaniesHouseRegisteredNumber")
PERIOD_START_KEYS = ("periodStart", "PeriodStart", "period_start")
PERIOD_END_KEYS   = ("periodEnd", "PeriodEnd", "period_end")
BALSHEET_KEYS     = ("balanceSheetDate", "BalanceSheetDate", "balance_sheet_date")
DORMANT_KEYS      = ("companyDormant", "CompanyDormant", "dormant", "isDormant")
COMP_TYPE_KEYS    = ("companyType", "type")
SIC_KEYS          = ("sicCode", "sicCodes", "sic_code")
TAXONOMY_KEYS     = ("taxonomy", "taxonomyName", "taxonomyVersion", "documentType")  # best-effort

# Minimal fallbacks
CH_NUM_RE = re.compile(rb"[^0-9]([0-9]{8})[^0-9]")
DATE_RE   = re.compile(rb"(20[0-9]{2}-[01][0-9]-[0-3][0-9])")


def _extract_from_ixbrl_bytes(data: bytes, zip_url: str, run_code: str) -> Dict[str, object]:
    """
    Parse a single iXBRL HTML/XHTML into our wide record.
    Returns {} if required keys can't be found.
    """
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
        # IDs & name
        for k in NAME_KEYS:
            if k in facts:
                out["entity_current_legal_name"] = facts[k]; break
        for k in NUM_KEYS:
            if k in facts:
                out["companies_house_registered_number"] = str(facts[k]); break
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

        # Numeric aliases
        for key, val in facts.items():
            if isinstance(val, (int, float, str)):
                norm = _norm(key)
                if norm in ALIASES:
                    out[ALIASES[norm]] = val

    # Fallbacks for minimal ID/date if parser didn’t give them
    if "companies_house_registered_number" not in out:
        m = CH_NUM_RE.search(data)
        if m:
            out["companies_house_registered_number"] = m.group(1).decode("utf-8")

    # provenance fields
    out["date"] = run_code
    if "companies_house_registered_number" in out:
        out["company_id"] = out["companies_house_registered_number"]

    return out


def _parse_daily_zip(zip_path: str, zip_url: str, run_code: str) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    with zipfile.ZipFile(zip_path, "r") as z:
        for info in z.infolist():
            name = info.filename
            # Nested monthly parts often appear as inner ZIPs containing HTML/XHTML
            if name.lower().endswith(".zip"):
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
            elif name.lower().endswith((".html", ".htm", ".xhtml")):
                with z.open(info) as f:
                    doc = f.read()
                rec = _extract_from_ixbrl_bytes(doc, zip_url, run_code)
                if rec:
                    rows.append(rec)

    if not rows:
        return pd.DataFrame(columns=TARGET_COLUMNS)

    df = pd.DataFrame(rows)

    # ensure all columns exist
    for c in TARGET_COLUMNS:
        if c not in df.columns:
            df[c] = pd.NA

    # coerce types
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

    # drop rows without an id
    df = df.dropna(subset=["companies_house_registered_number"])

    # keep column order
    return df[TARGET_COLUMNS]


def main():
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    state = {"processed": []}
    if os.path.exists(STATE_PATH):
        state = json.load(open(STATE_PATH, "r", encoding="utf-8"))

    today = dt.date.today()
    # Companies House publishes Tue–Sat (covering previous days).
    # We just grab the last 5 calendar days safely.
    check_dates = [today - dt.timedelta(days=i) for i in range(1, 6)]

    for d in check_dates:
        date_str = d.strftime("%Y-%m-%d")
        fname = DAILY_FILE_FMT.format(date=date_str)
        url = f"{DAILY_BASE_URL}{fname}"
        if url in state.get("processed", []):
            print(f"[skip] already processed {url}")
            continue

        r = SESSION.get(url, timeout=600)
        if r.status_code == 404:
            print(f"[skip] {url} -> 404")
            continue
        r.raise_for_status()
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as f:
            f.write(r.content)
            zpath = f.name

        run_code = date_str  # provenance
        df = _parse_daily_zip(zpath, url, run_code)
        print(f"[info] parsed {url}: {len(df)} rows")

        if df.empty:
            print(f"[warn] no rows parsed from {url}; leaving unprocessed")
            continue  # don't mark as processed so we can retry/tweak later

        # Pick release using balance_sheet_date, else period_end, else zip date
        ref = (
            df["balance_sheet_date"].dropna().iloc[0]
            if "balance_sheet_date" in df and df["balance_sheet_date"].notna().any()
            else (df["period_end"].dropna().iloc[0] if df["period_end"].notna().any() else pd.Timestamp(d))
        )
        ref_date = pd.to_datetime(ref, errors="coerce").date()
        year = ref_date.year
        half = half_from_date(ref_date)

        rel = gh_release_ensure(tag_for_financials(year, half), name=f"Financials {year} {half}")

        tmp_out = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False).name
        asset = gh_release_find_asset(rel, OUTPUT_BASENAME)
        if asset:
            gh_release_download_asset(asset, tmp_out)

        before = 0
        if os.path.exists(tmp_out) and os.path.getsize(tmp_out) > 0:
            before = len(pd.read_parquet(tmp_out))

        # dedupe on (company_id, balance_sheet_date|period_end)
        dedupe_keys = ["companies_house_registered_number"]
        key_date = "balance_sheet_date" if df["balance_sheet_date"].notna().any() else "period_end"
        dedupe_keys.append(key_date)

        append_parquet(tmp_out, df, subset_keys=dedupe_keys)

        after = len(pd.read_parquet(tmp_out))
        print(f"[info] appended to {year} {half}: {before} -> {after} rows")

        # mark processed only after a successful append
        state.setdefault("processed", []).append(url)

    json.dump(state, open(STATE_PATH, "w", encoding="utf-8"), indent=2)


if __name__ == "__main__":
    main()
