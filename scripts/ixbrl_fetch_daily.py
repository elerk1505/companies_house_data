# scripts/ixbrl_fetch_daily.py
# Robust daily iXBRL parser used by the bulk job.
from __future__ import annotations

import io
import math
import re
import zipfile
import tempfile
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple
import pandas as pd

# ixbrlparse is optional but highly recommended
try:
    import ixbrlparse  # pip install ixbrlparse
    TRY_IXBRLPARSE = True
except Exception:
    TRY_IXBRLPARSE = False

# ---------------- Schema: keep these aligned with the app ----------------
TARGET_COLUMNS: List[str] = [
    "run_code",
    "companies_house_registered_number",
    "date",
    "file_type",
    "taxonomy",
    "balance_sheet_date",
    "entity_current_legal_name",
    "company_dormant",
    "average_number_employees_during_period",
    "period_start",
    "period_end",

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
    "profit_loss_account_reserve",
    "shareholder_funds",

    # P&L (duration)
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

    # provenance
    "error",
    "zip_url",
]

# Which normalized targets are instant (BS) vs duration (P&L)
BALANCE_SHEET_NORMALIZED = {
    "tangible_fixed_assets","debtors","cash_bank_in_hand","current_assets",
    "creditors_due_within_one_year","creditors_due_after_one_year",
    "net_current_assets_liabilities","total_assets_less_current_liabilities",
    "net_assets_liabilities_including_pension_asset_liability",
    "called_up_share_capital","profit_loss_account_reserve","shareholder_funds",
}

PNL_NORMALIZED = {
    "turnover_gross_operating_revenue","other_operating_income","cost_sales","gross_profit_loss",
    "administrative_expenses","raw_materials_consumables","staff_costs",
    "depreciation_other_amounts_written_off_tangible_intangible_fixed_assets",
    "other_operating_charges_format2","operating_profit_loss",
    "profit_loss_on_ordinary_activities_before_tax",
    "tax_on_profit_or_loss_on_ordinary_activities","profit_loss_for_period",
}

# ---------------- Concept synonyms (extend as needed) ----------------
CONCEPT_SYNONYMS: Dict[str, List[str]] = {
    # Balance sheet
    "tangible_fixed_assets": [
        "TangibleFixedAssets", "uk-gaap:TangibleFixedAssets",
    ],
    "debtors": [
        "Debtors", "TradeAndOtherReceivables", "uk-gaap:Debtors",
    ],
    "cash_bank_in_hand": [
        "CashBankInHand", "CashAndCashEquivalents", "uk-gaap:CashBankInHand",
    ],
    "current_assets": [
        "CurrentAssets", "uk-gaap:CurrentAssets",
    ],
    "creditors_due_within_one_year": [
        "CreditorsDueWithinOneYear", "CurrentBorrowings", "uk-gaap:CreditorsDueWithinOneYear",
    ],
    "creditors_due_after_one_year": [
        "CreditorsDueAfterOneYear", "NoncurrentBorrowings", "uk-gaap:CreditorsDueAfterOneYear",
    ],
    "net_current_assets_liabilities": [
        "NetCurrentAssetsLiabilities", "NetCurrentAssetsLiabilitiesTotal",
        "uk-gaap:NetCurrentAssetsLiabilities", "ifrs-full:NetCurrentAssetsLiabilities",
    ],
    "total_assets_less_current_liabilities": [
        "TotalAssetsLessCurrentLiabilities","TotalAssetsMinusCurrentLiabilities",
        "uk-gaap:TotalAssetsLessCurrentLiabilities",
    ],
    "net_assets_liabilities_including_pension_asset_liability": [
        "NetAssetsLiabilitiesIncludingPensionAssetLiability","NetAssetsLiabilitiesIncludingPensionAsset",
        "NetAssetsLiabilities","NetAssets",
        "uk-gaap:NetAssetsLiabilitiesIncludingPensionAssetLiability",
    ],
    "called_up_share_capital": [
        "CalledUpShareCapital","CalledUpShareCapitalPresentedEquity","uk-gaap:CalledUpShareCapital",
    ],
    "profit_loss_account_reserve": [
        "ProfitLossAccountReserve","RetainedEarnings","uk-gaap:ProfitLossAccountReserve",
    ],
    "shareholder_funds": [
        "ShareholderFunds","Equity","TotalEquity","uk-gaap:ShareholderFunds",
    ],

    # P&L
    "turnover_gross_operating_revenue": [
        "TurnoverGrossOperatingRevenue","Revenue","Turnover","ifrs-full:Revenue","uk-gaap:TurnoverGrossOperatingRevenue",
    ],
    "other_operating_income": [
        "OtherOperatingIncome","OtherIncome","uk-gaap:OtherOperatingIncome",
    ],
    "cost_sales": [
        "CostSales","CostOfSales","CostOfRevenue","uk-gaap:CostOfSales",
    ],
    "gross_profit_loss": [
        "GrossProfitLoss","GrossProfit","GrossLoss","uk-gaap:GrossProfitLoss",
    ],
    "administrative_expenses": [
        "AdministrativeExpenses","AdministrativeExpense","uk-gaap:AdministrativeExpenses",
    ],
    "raw_materials_consumables": [
        "RawMaterialsConsumables","RawMaterialsAndConsumablesUsed","uk-gaap:RawMaterialsAndConsumables",
    ],
    "staff_costs": [
        "StaffCosts","EmployeeBenefitsExpense","uk-gaap:StaffCosts",
    ],
    "depreciation_other_amounts_written_off_tangible_intangible_fixed_assets": [
        "DepreciationOtherAmountsWrittenOffTangibleIntangibleFixedAssets","DepreciationAmortisationAndImpairment",
        "DepreciationAndAmortisationExpense",
    ],
    "other_operating_charges_format2": [
        "OtherOperatingChargesFormat2","OtherOperatingExpenses","OtherExpenses",
    ],
    "operating_profit_loss": [
        "OperatingProfitLoss","ProfitLossFromOperatingActivities","OperatingProfit","OperatingLoss",
    ],
    "profit_loss_on_ordinary_activities_before_tax": [
        "ProfitLossOnOrdinaryActivitiesBeforeTax","ProfitLossBeforeTax","ProfitBeforeTax","LossBeforeTax",
    ],
    "tax_on_profit_or_loss_on_ordinary_activities": [
        "TaxOnProfitOrLossOnOrdinaryActivities","TaxExpense","CurrentTaxExpense",
    ],
    "profit_loss_for_period": [
        "ProfitLossForPeriod","ProfitLoss","ProfitOrLoss","ProfitForTheYear","LossForTheYear",
        "ifrs-full:ProfitLoss"
    ],
}

# identifiers & meta concept guesses
COMPANY_NO_CONCEPTS = [
    "CompaniesHouseRegisteredNumber", "CompanyNumber", "uk-bus:UKCompaniesHouseRegisteredNumber",
    "uk-core:CompanyNumber", "EntityCompaniesHouseRegisteredNumber"
]
ENTITY_NAME_CONCEPTS = [
    "EntityCurrentLegalName","EntityReportingName","NameOfReportingEntityOrOtherMeansOfIdentification",
    "EntityName","CompanyName"
]
INCORP_DATE_CONCEPTS = ["IncorporationDate","incorporationDate","DateOfIncorporation"]
PERIOD_START_CONCEPTS = ["StartDateForPeriodCoveredByReport","PeriodStart", "PeriodStartDate", "StartDate"]
PERIOD_END_CONCEPTS = ["EndDateForPeriodCoveredByReport","PeriodEnd","PeriodEndDate", "EndDate"]
BALSHEET_DATE_CONCEPTS = ["BalanceSheetDate","StatementOfFinancialPositionDate","BalanceSheetAsAt"]

BOOL_DORMANT_CONCEPTS = ["CompanyDormant","EntityDormantIndicator"]
AVG_EMPLOYEES_CONCEPTS = ["AverageNumberEmployeesDuringPeriod","AverageNumberEmployees","EmployeesAverage"]


# ---------------------- helpers: robust conversions ----------------------
def _to_float(value: Any) -> Optional[float]:
    """list/tuple → first; strings with £/commas/() → float; else numeric; None if not convertible."""
    if value is None:
        return None
    if isinstance(value, (list, tuple)) and value:
        value = value[0]
    if isinstance(value, (int, float)):
        if isinstance(value, float) and math.isnan(value):
            return None
        return float(value)
    if isinstance(value, str):
        v = value.strip()
        if not v:
            return None
        neg = v.startswith("(") and v.endswith(")")
        v = v.strip("()£$€").replace(",", "")
        try:
            x = float(v)
            return -x if neg else x
        except Exception:
            return None
    return None

def _first_match_from_facts(facts: List[dict], concept_names: Iterable[str]) -> Optional[Any]:
    for f in facts:
        c = f.get("concept") or f.get("name")
        if c in concept_names:
            v = f.get("value")
            if v not in (None, "", "NULL", "null"):
                return v
    return None

def _best_numeric_fact(
    facts: List[dict],
    synonym_list: List[str],
    want_instant: bool,
    period_end_hint: Optional[str],
    instant_hint: Optional[str],
) -> Optional[float]:
    """
    Choose the best numeric fact:
    - exact concept in synonyms
    - instant/duration matches
    - pick closest date to hint; else latest
    - accept GBP/unitless/other units; no over-filter
    """
    cands: List[Tuple[pd.Timestamp | None, float]] = []
    for f in facts:
        c = f.get("concept") or f.get("name")
        if c not in synonym_list:
            continue
        ctx = f.get("context") or {}
        p = ctx.get("period") or {}
        ptype = p.get("type")
        is_instant = (ptype == "instant")
        if want_instant != is_instant:
            continue
        d = p.get("instant") if is_instant else p.get("end")
        val = _to_float(f.get("value"))
        if val is None:
            continue
        # unit accepted (GBP/unitless/anything)
        try:
            dt = pd.to_datetime(d, errors="coerce") if d else None
        except Exception:
            dt = None
        cands.append((dt, val))

    if not cands:
        return None

    # pick closest to hint (if provided), else latest date
    def _score(rec_dt: Optional[pd.Timestamp], hint: Optional[str]) -> pd.Timedelta:
        if rec_dt is None or hint is None:
            return pd.Timedelta.max
        h = pd.to_datetime(hint, errors="coerce")
        if pd.isna(h) or pd.isna(rec_dt):
            return pd.Timedelta.max
        return abs(rec_dt - h)

    if want_instant and instant_hint:
        cands.sort(key=lambda t: _score(t[0], instant_hint))
    elif (not want_instant) and period_end_hint:
        cands.sort(key=lambda t: _score(t[0], period_end_hint))
    else:
        cands.sort(key=lambda t: (t[0] is None, t[0]))  # None last; earliest→latest

    return cands[-1][1] if (instant_hint is None and period_end_hint is None) else cands[0][1]

# -------------------- ixbrl: normalize facts from parser --------------------
def _read_ixbrl_facts(data: bytes) -> Optional[List[dict]]:
    """Return a list of dict facts with keys: concept/name, value, context{period{type,instant,end}}."""
    if not TRY_IXBRLPARSE:
        return None
    try:
        obj = ixbrlparse.parse(io.BytesIO(data))  # library’s parse
    except Exception:
        return None

    # Try to normalize across potential shapes
    facts: List[dict] = []
    raw_facts = None

    # Common: obj.facts is a list
    raw_facts = getattr(obj, "facts", None)
    if raw_facts is None and isinstance(obj, dict):
        raw_facts = obj.get("facts")

    if raw_facts is None:
        return None

    for f in raw_facts:
        # f may be simple dataclass-like; use getattr then fallback to dict
        concept = getattr(f, "name", None) or getattr(f, "concept", None)
        if concept is None and isinstance(f, dict):
            concept = f.get("name") or f.get("concept")

        value = getattr(f, "value", None)
        if value is None and isinstance(f, dict):
            value = f.get("value")

        ctx = getattr(f, "context", None)
        if ctx is None and isinstance(f, dict):
            ctx = f.get("context")

        period = None
        if ctx is not None:
            period = getattr(ctx, "period", None) if not isinstance(ctx, dict) else ctx.get("period")

        ptype = getattr(period, "type", None) if period is not None and not isinstance(period, dict) else (period.get("type") if isinstance(period, dict) else None)
        instant = getattr(period, "instant", None) if period is not None and not isinstance(period, dict) else (period.get("instant") if isinstance(period, dict) else None)
        end = getattr(period, "end", None) if period is not None and not isinstance(period, dict) else (period.get("end") if isinstance(period, dict) else None)

        facts.append({
            "concept": concept,
            "value": value,
            "context": {"period": {"type": ptype, "instant": instant, "end": end}},
        })
    return facts

# ---------------------- per-file extraction into a row ----------------------
def _extract_from_ixbrl_bytes(data: bytes, zip_url: str, run_code: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {c: None for c in TARGET_COLUMNS}
    out["zip_url"] = zip_url
    out["run_code"] = run_code
    out["file_type"] = "ixbrl-html"
    out["taxonomy"] = None
    out["error"] = ""

    facts = _read_ixbrl_facts(data)
    if facts is None:
        out["error"] = "ixbrlparse:unavailable_or_failed"
        return out

    # Meta fields (best-effort)
    def first_from(candidates: List[str]) -> Optional[Any]:
        for name in candidates:
            v = _first_match_from_facts(facts, [name])
            if v is not None:
                return v
        return None

    # IDs / names
    ch_raw = first_from(COMPANY_NO_CONCEPTS)
    if isinstance(ch_raw, str):
        ch_digits = re.sub(r"[^0-9]", "", ch_raw or "")
        out["companies_house_registered_number"] = ch_digits or None
    elif ch_raw is not None:
        out["companies_house_registered_number"] = re.sub(r"[^0-9]", "", str(ch_raw)) or None

    out["entity_current_legal_name"] = first_from(ENTITY_NAME_CONCEPTS)

    # Dates
    inc = first_from(INCORP_DATE_CONCEPTS)
    per_start = first_from(PERIOD_START_CONCEPTS)
    per_end = first_from(PERIOD_END_CONCEPTS)
    bs_date = first_from(BALSHEET_DATE_CONCEPTS)

    # normalize date strings
    def norm_date(x):
        try:
            return pd.to_datetime(x, errors="coerce").date().isoformat()
        except Exception:
            return None

    out["date"] = norm_date(per_end) or norm_date(bs_date)
    out["incorporation_date"] = norm_date(inc)
    out["period_start"] = norm_date(per_start)
    out["period_end"] = norm_date(per_end)
    out["balance_sheet_date"] = norm_date(bs_date)

    # Flags / simple numerics
    dormant = first_from(BOOL_DORMANT_CONCEPTS)
    out["company_dormant"] = (str(dormant).strip().lower() in ("true", "1", "yes")) if dormant is not None else None

    avg_emp = _to_float(first_from(AVG_EMPLOYEES_CONCEPTS))
    out["average_number_employees_during_period"] = avg_emp

    # Hints for picking best facts
    period_end_hint = out["period_end"]
    instant_hint = out["balance_sheet_date"] or out["period_end"]

    # Financial numerics (robust selection)
    for norm in (BALANCE_SHEET_NORMALIZED | PNL_NORMALIZED):
        syn = list(CONCEPT_SYNONYMS.get(norm, []))
        # also try direct CamelCase of snake name
        camel = "".join([p.capitalize() for p in norm.split("_")])
        if camel not in syn:
            syn.append(camel)
        want_instant = (norm in BALANCE_SHEET_NORMALIZED)
        val = _best_numeric_fact(
            facts, syn, want_instant=want_instant,
            period_end_hint=period_end_hint, instant_hint=instant_hint
        )
        if val is not None:
            out[norm] = val

    return out

# ------------------------- daily zip parsing (entry) -------------------------
def _parse_daily_zip(zip_path: str, zip_url: str, run_code: str) -> pd.DataFrame:
    """
    Parse a "daily" Companies House accounts zip (or a parent that contains daily zips).
    Returns a DataFrame with TARGET_COLUMNS.
    """
    rows: List[Dict[str, Any]] = []

    def _maybe_parse_html_bytes(b: bytes):
        row = _extract_from_ixbrl_bytes(b, zip_url=zip_url, run_code=run_code)
        rows.append(row)

    with zipfile.ZipFile(zip_path, "r") as z:
        for info in z.infolist():
            fname = info.filename
            # nested zips: recurse once
            if fname.lower().endswith(".zip"):
                with z.open(info) as nested_fp:
                    nested_bytes = nested_fp.read()
                with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tf:
                    tf.write(nested_bytes)
                    nested_path = tf.name
                try:
                    with zipfile.ZipFile(nested_path, "r") as z2:
                        for inner in z2.infolist():
                            if inner.filename.lower().endswith((".html", ".htm", ".xhtml")):
                                with z2.open(inner) as f:
                                    _maybe_parse_html_bytes(f.read())
                except Exception:
                    # ignore bad nested archives
                    continue
            elif fname.lower().endswith((".html", ".htm", ".xhtml")):
                with z.open(info) as f:
                    _maybe_parse_html_bytes(f.read())

    if not rows:
        return pd.DataFrame(columns=TARGET_COLUMNS)

    df = pd.DataFrame(rows)
    # ensure all target columns exist
    for c in TARGET_COLUMNS:
        if c not in df.columns:
            df[c] = None

    # Diagnostics: show which metrics populated in this batch
    numeric_cols = [c for c in TARGET_COLUMNS if c not in {
        "run_code","companies_house_registered_number","date","file_type","taxonomy","balance_sheet_date",
        "entity_current_legal_name","company_dormant","average_number_employees_during_period",
        "period_start","period_end","error","zip_url"
    }]
    if not df.empty:
        nn = df[numeric_cols].notna().sum().sort_values(ascending=False)
        print("[ixbrl_fetch_daily] non-null counts (top 10):")
        print(nn.head(10).to_string())

    # Return only the schema columns (order fixed)
    return df[TARGET_COLUMNS]
