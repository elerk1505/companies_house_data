      # ================================================================
# scripts/ixbrl_fetch_daily.py  — core iXBRL parsing (used by daily & bulk)
# ================================================================
from __future__ import annotations

import io
import os
import re
import zipfile
import datetime as dt
from dataclasses import dataclass
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

import pandas as pd

# ixbrlparse is tiny and pure-Python. We rely only on a small surface:
# Document(f) -> .facts (list); each fact has:
#   .name (qname, e.g. "frs102:NetCurrentAssetsLiabilities")
#   .value (str/number)
#   .decimals (int/str/None)
#   .unit (str/None)
#   .period.instant (datetime|None)  OR .period.start/.period.end
from ixbrlparse import IXBRL

# --------------------------------------------------------------------------------------
# Public surface used by bulk:
#  * TARGET_COLUMNS
#  * _parse_daily_zip(zip_path, zip_url, run_code) -> DataFrame
# --------------------------------------------------------------------------------------

TARGET_COLUMNS: List[str] = [
    # provenance
    "run_code", "zip_url",

    # top-level filing/date info
    "date", "file_type", "taxonomy",

    # identity
    "companies_house_registered_number", "company_id",
    "entity_current_legal_name", "company_type", "company_dormant",
    "sic_code", "incorporation_date",

    # period information
    "balance_sheet_date", "period_start", "period_end",
    "average_number_employees_during_period",

    # balance sheet (instant)
    "tangible_fixed_assets",
    "debtors",
    "cash_bank_in_hand",
    "current_assets",
    "creditors_due_within_one_year",
    "creditors_due_after_one_year",
    "net_current_assets_liabilities",
    "total_assets_less_current_liabilities",
    "net_assets_liabilities_including_pension_asset_liability",
    "called_up_share_capital",

    # p&l (duration)
    "turnover_gross_operating_revenue",
    "other_operating_income",
    "cost_sales",
    "gross_profit_loss",
    "administrative_expenses",
    "raw_materials_consumables",
    "staff_costs",
    "depreciation_other_amounts_written_off_tangible_intangible_fixed_assets",
    "other_operating_charges_format2",
    "operating_profit_loss",
    "profit_loss_on_ordinary_activities_before_tax",
    "tax_on_profit_or_loss_on_ordinary_activities",
    "profit_loss_for_period",

    # misc
    "error",
]

# --------------------------------------------------------------------------------------
# Helpers: number normalization, period handling, synonym maps
# --------------------------------------------------------------------------------------

_NON_DIGIT = re.compile(r"[^0-9\-\.+]", re.U)

def _to_float(x) -> Optional[float]:
    """Convert strings like '£1,234', '(456)' to float; keep None for blanks."""
    if x is None:
        return None
    s = str(x).strip()
    if s == "" or s.lower() == "nan":
        return None
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    s = _NON_DIGIT.sub("", s)
    if s in ("", "-", "+"):
        return None
    try:
        return float(s)
    except Exception:
        return None

def _scale_by_decimals(val: Optional[float], decimals) -> Optional[float]:
    """Handle common 'decimals' semantics: negative => scaled thousands/millions."""
    if val is None or decimals is None:
        return val
    try:
        d = int(decimals)
    except Exception:
        return val
    if d < 0:
        return val * (10 ** (-d))
    return val

def _period_type(fact) -> str:
    """Return 'instant' or 'duration' for a fact."""
    p = getattr(fact, "period", None)
    if p is None:
        return "instant"  # pragmatic default
    if getattr(p, "instant", None) is not None:
        return "instant"
    return "duration"

def _period_dates(fact) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    """Return (bs_date, period_end) where each may be None."""
    p = getattr(fact, "period", None)
    bs_date = None
    pe = None
    if p is not None:
        if getattr(p, "instant", None) is not None:
            bs_date = pd.to_datetime(p.instant, errors="coerce")
        else:
            pe = pd.to_datetime(getattr(p, "end", None), errors="coerce")
    return (bs_date, pe)

# ---- Concept synonyms across FRS102/105, UK-GAAP, IFRS --------------------
CONCEPTS: Dict[str, List[str]] = {
    # balance sheet
    "tangible_fixed_assets": [
        "frs102:TangibleFixedAssets",
        "uk-gaap:TangibleFixedAssets",
        "uk-gaap:PropertyPlantAndEquipment",
        "ifrs-full:PropertyPlantAndEquipment",
    ],
    "debtors": [
        "frs102:Debtors",
        "uk-gaap:Debtors",
        "uk-gaap:TradeAndOtherReceivables",
        "ifrs-full:TradeAndOtherReceivables",
    ],
    "cash_bank_in_hand": [
        "frs102:CashBankInHand",
        "uk-gaap:CashBankInHand",
        "uk-gaap:CashAndCashEquivalents",
        "ifrs-full:CashAndCashEquivalents",
    ],
    "current_assets": [
        "frs102:CurrentAssets", "uk-gaap:CurrentAssets", "ifrs-full:CurrentAssets",
    ],
    "creditors_due_within_one_year": [
        "frs102:CreditorsAmountsFallingDueWithinOneYear",
        "uk-gaap:CreditorsDueWithinOneYear",
        "uk-gaap:CurrentTradeAndOtherPayables",
        "ifrs-full:TradeAndOtherCurrentPayables",
    ],
    "creditors_due_after_one_year": [
        "frs102:CreditorsAmountsFallingDueAfterMoreThanOneYear",
        "uk-gaap:CreditorsDueAfterOneYear",
        "ifrs-full:NoncurrentTradeAndOtherPayables",
    ],
    "net_current_assets_liabilities": [
        "frs102:NetCurrentAssetsLiabilities",
        "uk-gaap:NetCurrentAssetsLiabilities",
        "ifrs-full:NetCurrentAssetsLiabilities",
    ],
    "total_assets_less_current_liabilities": [
        "frs102:TotalAssetsLessCurrentLiabilities",
        "uk-gaap:TotalAssetsLessCurrentLiabilities",
        "ifrs-full:TotalAssetsLessCurrentLiabilities",
    ],
    "net_assets_liabilities_including_pension_asset_liability": [
        "frs102:NetAssetsLiabilitiesIncludingPensionAssetLiability",
        "uk-gaap:NetAssetsLiabilitiesIncludingPensionAssetLiability",
        "ifrs-full:Equity",  # pragmatic stand-in for net assets
    ],
    "called_up_share_capital": [
        "frs102:CalledUpShareCapitalPresentedWithinEquity",
        "uk-gaap:CalledUpShareCapital",
        "uk-gaap:CalledUpShareCapitalNotPaid",
        "ifrs-full:IssuedCapital",
    ],

    # p&l
    "turnover_gross_operating_revenue": [
        "frs102:TurnoverGrossOperatingRevenue",
        "uk-gaap:TurnoverGrossOperatingRevenue",
        "ifrs-full:Revenue",
    ],
    "other_operating_income": [
        "frs102:OtherOperatingIncome", "uk-gaap:OtherOperatingIncome", "ifrs-full:OtherOperatingIncome",
    ],
    "cost_sales": ["frs102:CostOfSales", "uk-gaap:CostOfSales", "ifrs-full:CostOfSales"],
    "gross_profit_loss": ["frs102:GrossProfitLoss", "uk-gaap:GrossProfitLoss", "ifrs-full:GrossProfitLoss"],
    "administrative_expenses": [
        "frs102:AdministrativeExpenses", "uk-gaap:AdministrativeExpenses", "ifrs-full:AdministrativeExpense",
    ],
    "raw_materials_consumables": [
        "frs102:RawMaterialsAndConsumables", "uk-gaap:RawMaterialsAndConsumables",
        "ifrs-full:RawMaterialsAndConsumablesUsed",
    ],
    "staff_costs": ["frs102:StaffCosts", "uk-gaap:StaffCosts", "ifrs-full:EmployeeBenefitsExpense"],
    "depreciation_other_amounts_written_off_tangible_intangible_fixed_assets": [
        "frs102:DepreciationAmortisationAndImpairment",
        "uk-gaap:DepreciationAmortisationAndImpairment",
        "ifrs-full:DepreciationAndAmortisationExpense",
    ],
    "other_operating_charges_format2": [
        "frs102:OtherOperatingCharges", "uk-gaap:OtherOperatingCharges", "ifrs-full:OtherOperatingExpense",
    ],
    "operating_profit_loss": [
        "frs102:OperatingProfitLoss", "uk-gaap:OperatingProfitLoss", "ifrs-full:ProfitLossFromOperatingActivities",
    ],
    "profit_loss_on_ordinary_activities_before_tax": [
        "frs102:ProfitLossOnOrdinaryActivitiesBeforeTax", "uk-gaap:ProfitLossOnOrdinaryActivitiesBeforeTax",
        "ifrs-full:ProfitLossBeforeTax",
    ],
    "tax_on_profit_or_loss_on_ordinary_activities": [
        "frs102:TaxOnProfitLossOnOrdinaryActivities", "uk-gaap:TaxOnProfitLossOnOrdinaryActivities",
        "ifrs-full:IncomeTaxExpenseContinuingOperations",
    ],
    "profit_loss_for_period": [
        "frs102:ProfitLossForPeriod", "uk-gaap:ProfitLossForPeriod", "ifrs-full:ProfitLoss",
    ],
}

_PANDL_LOGICALS = {
    "turnover_gross_operating_revenue", "other_operating_income", "cost_sales",
    "gross_profit_loss", "administrative_expenses", "raw_materials_consumables",
    "staff_costs", "depreciation_other_amounts_written_off_tangible_intangible_fixed_assets",
    "other_operating_charges_format2", "operating_profit_loss",
    "profit_loss_on_ordinary_activities_before_tax",
    "tax_on_profit_or_loss_on_ordinary_activities", "profit_loss_for_period",
}

# --------------------------------------------------------------------------------------
# iXBRL fact picking
# --------------------------------------------------------------------------------------

def _pick_fact(doc, qnames: List[str], prefer: str) -> Tuple[Optional[float], Optional[dict], Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    """
    prefer: "instant" or "duration"
    Returns (value, meta, balance_sheet_date, period_end)
    """
    candidates: List[Tuple[str, object]] = []
    for f in getattr(doc, "facts", []):
        if f.name in qnames:
            candidates.append((_period_type(f), f))

    if not candidates:
        return None, None, None, None

    # Put preferred period type first; if multiple, pick last in doc (often the final audited value)
    candidates.sort(key=lambda t: (t[0] != prefer,))
    fact = candidates[0][1]

    val = _scale_by_decimals(_to_float(fact.value), getattr(fact, "decimals", None))
    bs_date, pe = _period_dates(fact)
    meta = {
        "name": fact.name,
        "period": _period_type(fact),
        "decimals": getattr(fact, "decimals", None),
        "unit": getattr(fact, "unit", None),
    }
    return val, meta, bs_date, pe

def get_number(doc, logical_name: str, debug: bool = False) -> Tuple[Optional[float], Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    qnames = CONCEPTS.get(logical_name, [])
    prefer = "duration" if logical_name in _PANDL_LOGICALS else "instant"
    val, meta, bs_date, pe = _pick_fact(doc, qnames, prefer)
    if debug and meta:
        print(f"[debug] {logical_name:<55} <= {meta['name']} ({meta['period']}, dec={meta['decimals']}, unit={meta['unit']}) -> {val}")
    return val, bs_date, pe

# --------------------------------------------------------------------------------------
# Zip traversal and per-file parsing
# --------------------------------------------------------------------------------------

@dataclass
class InnerFile:
    name: str
    bytes: bytes

def _iter_ixbrl_files_from_zip(z: zipfile.ZipFile) -> Iterator[InnerFile]:
    """
    Yields iXBRL HTML files from a zip. If the archive contains nested zip(s),
    it descends one level and yields any .htm/.html inside.
    """
    for info in z.infolist():
        n = info.filename
        # Raw iXBRL files
        if n.lower().endswith((".htm", ".html", ".xhtml")):
            with z.open(info) as f:
                yield InnerFile(name=n, bytes=f.read())
            continue

        # Nested daily zips
        if n.lower().endswith(".zip"):
            with z.open(info) as f:
                nested_bytes = f.read()
            try:
                with zipfile.ZipFile(io.BytesIO(nested_bytes)) as nz:
                    for nf in nz.infolist():
                        nn = nf.filename
                        if nn.lower().endswith((".htm", ".html", ".xhtml")):
                            with nz.open(nf) as ff:
                                yield InnerFile(name=f"{n}::{nn}", bytes=ff.read())
            except zipfile.BadZipFile:
                # Skip unreadable inner zips; it's common to have non-iXBRL attachments.
                continue

def _parse_one_ixbrl(file_bytes: bytes, *, run_code: str, zip_url: str) -> Dict[str, object]:
    """
    Parse a single iXBRL HTML bytes blob into a row dict following TARGET_COLUMNS.
    Missing values are returned as None. Any extraction errors are captured in 'error'.
    """
    row: Dict[str, object] = {c: None for c in TARGET_COLUMNS}
    row["run_code"] = run_code
    row["zip_url"] = zip_url
    row["file_type"] = "ixbrl-htm"

    try:
        doc = IXBRL(io.BytesIO(file_bytes)).parse()

        # Basic metadata commonly present in doc
        # (Best effort; different filers expose these in different ways)
        row["taxonomy"] = getattr(doc, "taxonomy", None)

        # Known identity facts
        # CH number
        for cand in ("frs102:CompaniesHouseRegisteredNumber", "uk-gaap:CompaniesHouseRegisteredNumber", "ch:CompaniesHouseRegisteredNumber"):
            v = doc.facts_by_name.get(cand) if hasattr(doc, "facts_by_name") else None
            if v:
                row["companies_house_registered_number"] = str(v[0].value).strip()
                break
        row["company_id"] = row.get("companies_house_registered_number")

        # Company name
        for cand in ("entity:EntityCurrentLegalName", "frs102:EntityCurrentLegalName", "uk-gaap:EntityCurrentLegalName", "ifrs-full:NameOfReportingEntityOrOtherMeansOfIdentification"):
            v = doc.facts_by_name.get(cand) if hasattr(doc, "facts_by_name") else None
            if v:
                row["entity_current_legal_name"] = str(v[0].value).strip()
                break

        # Dormant flag (best-effort)
        for cand in ("frs102:Dormant", "uk-gaap:Dormant", "ch:Dormant"):
            v = doc.facts_by_name.get(cand) if hasattr(doc, "facts_by_name") else None
            if v:
                row["company_dormant"] = str(v[0].value).strip().lower() in ("true", "1", "yes")
                break

        # SIC (may be in metadata; optional)
        for cand in ("ch:SICCode", "uk-gaap:SicCode", "frs102:SicCode"):
            v = doc.facts_by_name.get(cand) if hasattr(doc, "facts_by_name") else None
            if v:
                row["sic_code"] = _to_float(v[0].value)
                break

        # Incorporation date (rare in accounts; best-effort)
        for cand in ("ch:IncorporationDate",):
            v = doc.facts_by_name.get(cand) if hasattr(doc, "facts_by_name") else None
            if v:
                row["incorporation_date"] = pd.to_datetime(v[0].value, errors="coerce")
                break

        # Employees (duration)
        for cand in ("frs102:AverageNumberEmployeesDuringPeriod", "uk-gaap:AverageNumberEmployeesDuringPeriod", "ifrs-full:AverageNumberOfEmployees"):
            v = doc.facts_by_name.get(cand) if hasattr(doc, "facts_by_name") else None
            if v:
                row["average_number_employees_during_period"] = _to_float(v[0].value)
                break

        # Extract numbers for each logical field
        # keep track of balance_sheet_date / period_end as we go
        bs_dates: List[pd.Timestamp] = []
        period_ends: List[pd.Timestamp] = []

        def set_num(field: str):
            val, bs, pe = get_number(doc, field, debug=False)
            row[field] = val
            if bs is not None:
                bs_dates.append(bs)
            if pe is not None:
                period_ends.append(pe)

        numeric_fields = [
            # balance sheet
            "tangible_fixed_assets", "debtors", "cash_bank_in_hand", "current_assets",
            "creditors_due_within_one_year", "creditors_due_after_one_year",
            "net_current_assets_liabilities", "total_assets_less_current_liabilities",
            "net_assets_liabilities_including_pension_asset_liability", "called_up_share_capital",
            # p&l
            "turnover_gross_operating_revenue", "other_operating_income", "cost_sales",
            "gross_profit_loss", "administrative_expenses", "raw_materials_consumables",
            "staff_costs", "depreciation_other_amounts_written_off_tangible_intangible_fixed_assets",
            "other_operating_charges_format2", "operating_profit_loss",
            "profit_loss_on_ordinary_activities_before_tax",
            "tax_on_profit_or_loss_on_ordinary_activities", "profit_loss_for_period",
        ]
        for f in numeric_fields:
            set_num(f)

        # deduce balance sheet date / period dates from any extracted fact
        row["balance_sheet_date"] = min(bs_dates) if bs_dates else None
        row["period_end"] = max(period_ends) if period_ends else None

        # For completeness, set a generic 'date' column:
        row["date"] = row["balance_sheet_date"] or row["period_end"]

        # Company type (best-effort: often appears in metadata)
        for cand in ("ch:CompanyType", "uk-gaap:CompanyType"):
            v = doc.facts_by_name.get(cand) if hasattr(doc, "facts_by_name") else None
            if v:
                row["company_type"] = str(v[0].value)
                break

    except Exception as e:
        row["error"] = f"{type(e).__name__}: {e}"

    return row

# --------------------------------------------------------------------------------------
# Public entry used by bulk and by the daily job runner
# --------------------------------------------------------------------------------------

def _parse_daily_zip(zip_path: str, zip_url: str, run_code: str) -> pd.DataFrame:
    """
    Parse a Companies House *daily* zip (or a monthly zip that contains daily zips),
    returning a DataFrame with TARGET_COLUMNS.
    - zip_path: local .zip file path
    - zip_url:  source URL (used for provenance in 'zip_url')
    - run_code: free-form provenance label (e.g. '2025-01-15' or '2025-01')
    """
    rows: List[Dict[str, object]] = []

    with zipfile.ZipFile(zip_path, "r") as z:
        for inner in _iter_ixbrl_files_from_zip(z):
            try:
                row = _parse_one_ixbrl(inner.bytes, run_code=run_code, zip_url=zip_url)
                rows.append(row)
            except Exception as e:
                rows.append({
                    **{c: None for c in TARGET_COLUMNS},
                    "run_code": run_code,
                    "zip_url": zip_url,
                    "file_type": "ixbrl-htm",
                    "error": f"{type(e).__name__}: {e}",
                })

    if not rows:
        # Return a well-formed empty frame
        return pd.DataFrame(columns=TARGET_COLUMNS)

    df = pd.DataFrame(rows)[TARGET_COLUMNS]

    # ---- Diagnostics printed into Action logs (helps choose numeric ranges)
    cols_of_interest = [
        "tangible_fixed_assets","debtors","cash_bank_in_hand","current_assets",
        "creditors_due_within_one_year","creditors_due_after_one_year",
        "net_current_assets_liabilities","total_assets_less_current_liabilities",
        "net_assets_liabilities_including_pension_asset_liability","called_up_share_capital",
        "turnover_gross_operating_revenue","other_operating_income","cost_sales","gross_profit_loss",
        "administrative_expenses","raw_materials_consumables","staff_costs",
        "depreciation_other_amounts_written_off_tangible_intangible_fixed_assets",
        "other_operating_charges_format2","operating_profit_loss",
        "profit_loss_on_ordinary_activities_before_tax",
        "tax_on_profit_or_loss_on_ordinary_activities","profit_loss_for_period",
    ]
    present = [c for c in cols_of_interest if c in df.columns]
    if present:
        nn = df[present].notna().sum().sort_values(ascending=False).head(12)
        print(f"[ixbrl_fetch_daily] non-null counts (top 12) for run {run_code}:")
        for k, v in nn.items():
            print(f"  {k:55s} {v}")

    return df
