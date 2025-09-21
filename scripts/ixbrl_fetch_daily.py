# ================================================================
# scripts/ixbrl_fetch_daily.py — Action 2 (Daily financials)
# ================================================================
from __future__ import annotations

import io
import os
import re
import sys
import zipfile
import tempfile
import datetime as dt
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup

from scripts.common import (
    SESSION,
    gh_release_ensure,
    gh_release_find_asset,
    gh_release_download_asset,
    gh_release_upload_or_replace_asset,
    append_parquet,
    tag_for_financials,
)

# ----------------------------- Config -----------------------------

BASE = "https://download.companieshouse.gov.uk"
DAILY_FMT = BASE + "/Accounts_Bulk_Data-{yyyy}-{mm}-{dd}.zip"
OUTPUT_BASENAME = "financials.parquet"
MAX_LOOKBACK_DAYS = 5  # if today has no file, walk back up to N-1 days

# Columns we will emit (order matters for parquet stability)
TARGET_COLUMNS: List[str] = [
    "run_code",
    "company_id",
    "date",
    "file_type",
    "taxonomy",
    "balance_sheet_date",
    "companies_house_registered_number",
    "entity_current_legal_name",
    "company_dormant",
    "average_number_employees_during_period",
    "period_start",
    "period_end",
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
    # room for P&L if you add later
    "zip_url",
    "error",
]

# ----------------------- Concept mapping --------------------------

DEFAULT_CONCEPTS: Dict[str, List[str]] = {
    "company_id": [
        "ns10:UKCompaniesHouseRegisteredNumber",
        "bus:UKCompaniesHouseRegisteredNumber",
        "uk-bus:UKCompaniesHouseRegisteredNumber",
    ],
    "entity_current_legal_name": [
        "bus:EntityCurrentLegalRegisteredName",
        "uk-bus:EntityCurrentLegalRegisteredName",
    ],
    "balance_sheet_date": ["bus:BalanceSheetDate"],
    "period_start": ["bus:StartDateForPeriodCoveredByReport"],
    "period_end": ["bus:EndDateForPeriodCoveredByReport"],
    "cash_bank_in_hand": ["core:CashBankOnHand", "ns5:CashBankOnHand"],
    "debtors": ["core:Debtors", "ns5:Debtors"],
    "current_assets": ["core:CurrentAssets", "ns5:CurrentAssets"],
    "creditors_due_within_one_year": ["ns5:CreditorsDueWithinOneYear", "core:Creditors"],
    "net_current_assets_liabilities": [
        "core:NetCurrentAssetsLiabilities",
        "ns5:NetCurrentAssetsLiabilities",
        "uk-gaap-pt:NetCurrentAssetsLiabilities",
        "uk-gaap:NetCurrentAssetsLiabilities",
    ],
    "total_assets_less_current_liabilities": [
        "core:TotalAssetsLessCurrentLiabilities",
        "ns5:TotalAssetsLessCurrentLiabilities",
        "uk-gaap-pt:TotalAssetsLessCurrentLiabilities",
        "uk-gaap:TotalAssetsLessCurrentLiabilities",
    ],
    "net_assets_liabilities_including_pension_asset_liability": [
        "uk-gaap:NetAssetsLiabilitiesIncludingPensionAssetLiability",
        "core:NetAssetsLiabilities",
    ],
    "tangible_fixed_assets": [
        "ns5:TangibleFixedAssets",
        "uk-gaap-pt:TangibleFixedAssets",
        "core:PropertyPlantEquipment",
    ],
    "called_up_share_capital": ["ns5:CalledUpShareCapital"],
    "shareholder_funds": ["ns5:ShareholderFunds", "core:Equity", "ns5:Equity"],
}

def _load_concepts_map() -> Dict[str, List[str]]:
    # load external YAML if present
    path = os.path.join(os.path.dirname(__file__), "concepts_map.yaml")
    try:
        import yaml  # type: ignore
        if os.path.exists(path):
            with open(path, "r") as f:
                data = yaml.safe_load(f) or {}
            # merge defaults with overrides / additions
            merged = dict(DEFAULT_CONCEPTS)
            for k, v in (data or {}).items():
                if not isinstance(v, list):
                    continue
                merged[k] = v
            return merged
    except Exception:
        pass
    return dict(DEFAULT_CONCEPTS)

CONCEPTS = _load_concepts_map()

# ---------------------- iXBRL parsing helpers ---------------------

_QNAME_ATTRS = ("name", "contextref", "unitref")

def _clean_number(s: Optional[str]) -> Optional[float]:
    if s is None:
        return None
    t = s.strip()
    if not t:
        return None
    # Parentheses as negatives, strip currency/commas/spaces
    neg = t.startswith("(") and t.endswith(")")
    t = re.sub(r"[^0-9.\-]", "", t)
    if not t:
        return None
    try:
        val = float(t)
        return -val if neg else val
    except ValueError:
        return None

def _parse_ixbrl_values(html_bytes: bytes) -> Dict[str, str]:
    """Return a dict { qualified_name -> raw_text_value } from ix:nonFraction & ix:nonNumeric."""
    # Use XML parser if available for better namespace handling; fall back to lxml-html
    soup = BeautifulSoup(html_bytes, "lxml-xml")
    if soup.find() is None:
        soup = BeautifulSoup(html_bytes, "lxml")

    out: Dict[str, str] = {}

    # Any tag (namespace-insensitive) with required attrs
    def collect(tags: Iterable):
        for tag in tags:
            try:
                qn = tag.get("name") or tag.get("concept")
                if not qn:
                    continue
                text = tag.get_text(strip=True)
                if text is None:
                    continue
                # first-win per concept; we only need a representative value
                out.setdefault(qn, text)
            except Exception:
                continue

    collect(soup.find_all(["nonfraction", "nonFraction"]))   # ix:nonFraction
    collect(soup.find_all(["nonnumeric", "nonNumeric"]))     # ix:nonNumeric
    # also match namespaced like ix:nonFraction (BeautifulSoup normalizes)
    collect(soup.find_all("ix:nonFraction"))
    collect(soup.find_all("ix:nonNumeric"))

    return out

def _first_present(raw: Dict[str, str], candidates: Iterable[str], numeric: bool) -> Optional[str | float]:
    for qn in candidates:
        if qn in raw:
            return _clean_number(raw[qn]) if numeric else raw[qn]
    return None

# ------------------------- Zip walkers ----------------------------

def _iter_ixbrl_members(z: zipfile.ZipFile):
    """Yield (member_name, html_bytes) for any iXBRL HTML/XHTML inside the (possibly nested) zip."""
    for info in z.infolist():
        name = info.filename
        if name.lower().endswith((".html", ".xhtml", ".htm")):
            with z.open(info) as f:
                yield name, f.read()
        elif name.lower().endswith(".zip"):
            with z.open(info) as f:
                inner = io.BytesIO(f.read())
            try:
                with zipfile.ZipFile(inner, "r") as z2:
                    for sub_name, sub_bytes in _iter_ixbrl_members(z2):
                        yield f"{name}::{sub_name}", sub_bytes
            except zipfile.BadZipFile:
                continue

# -------------------- Row extraction per file ---------------------

def _row_from_html(html: bytes, run_code: str, zip_url: str) -> Dict[str, object]:
    raw = _parse_ixbrl_values(html)

    # map into our output columns
    company_id = _first_present(raw, CONCEPTS["company_id"], numeric=False)
    entity_name = _first_present(raw, CONCEPTS["entity_current_legal_name"], numeric=False)
    bs_date    = _first_present(raw, CONCEPTS["balance_sheet_date"], numeric=False)
    p_start    = _first_present(raw, CONCEPTS["period_start"], numeric=False)
    p_end      = _first_present(raw, CONCEPTS["period_end"], numeric=False)

    def num(field: str) -> Optional[float]:
        return _first_present(raw, CONCEPTS.get(field, []), numeric=True)  # type: ignore

    row = {
        "run_code": run_code,
        "company_id": str(company_id) if company_id is not None else None,
        "date": None,                # fill later if you like
        "file_type": "html",
        "taxonomy": None,            # can be filled by namespace scan if desired
        "balance_sheet_date": bs_date,
        "companies_house_registered_number": str(company_id) if company_id is not None else None,
        "entity_current_legal_name": entity_name,
        "company_dormant": None,
        "average_number_employees_during_period": None,
        "period_start": p_start,
        "period_end": p_end,

        "tangible_fixed_assets": num("tangible_fixed_assets"),
        "debtors": num("debtors"),
        "cash_bank_in_hand": num("cash_bank_in_hand"),
        "current_assets": num("current_assets"),
        "creditors_due_within_one_year": num("creditors_due_within_one_year"),
        "creditors_due_after_one_year": None,  # add mapping later if needed
        "net_current_assets_liabilities": num("net_current_assets_liabilities"),
        "total_assets_less_current_liabilities": num("total_assets_less_current_liabilities"),
        "net_assets_liabilities_including_pension_asset_liability": num("net_assets_liabilities_including_pension_asset_liability"),
        "called_up_share_capital": num("called_up_share_capital"),
        "profit_loss_account_reserve": None,
        "shareholder_funds": num("shareholder_funds"),

        "zip_url": zip_url,
        "error": None,
    }
    return row

# ------------------------ Public parse API ------------------------

def _parse_daily_zip(local_zip: str, url: str, run_code: str) -> pd.DataFrame:
    """Shared parser used by both daily and bulk. Returns DataFrame[TARGET_COLUMNS]."""
    rows: List[Dict[str, object]] = []
    with zipfile.ZipFile(local_zip, "r") as z:
        for name, html in _iter_ixbrl_members(z):
            try:
                rows.append(_row_from_html(html, run_code, url))
            except Exception as e:
                rows.append({
                    **{k: None for k in TARGET_COLUMNS},
                    "run_code": run_code,
                    "file_type": "html",
                    "zip_url": url,
                    "error": f"{type(e).__name__}: {e}",
                })
    if not rows:
        return pd.DataFrame(columns=TARGET_COLUMNS)
    df = pd.DataFrame(rows)
    return df[[c for c in TARGET_COLUMNS if c in df.columns]]

# ----------------------------- Main -------------------------------

def _daily_urls_for(date: dt.date) -> Tuple[str, str]:
    return DAILY_FMT.format(yyyy=date.year, mm=f"{date.month:02d}", dd=f"{date.day:02d}"), f"{date:%Y-%m-%d}"

def http_get_to_temp(url: str, timeout: int = 600) -> str:
    r = SESSION.get(url, timeout=timeout)
    if r.status_code == 404:
        raise FileNotFoundError(url)
    r.raise_for_status()
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as f:
        f.write(r.content)
        return f.name

def main():
    today = dt.date.today()
    tried = 0
    all_frames: List[pd.DataFrame] = []

    while tried < MAX_LOOKBACK_DAYS:
        url, run_code = _daily_urls_for(today - dt.timedelta(days=tried))
        try:
            zpath = http_get_to_temp(url)
        except FileNotFoundError:
            tried += 1
            continue
        except requests.HTTPError:
            tried += 1
            continue

        df = _parse_daily_zip(zpath, url, run_code)
        if not df.empty:
            all_frames.append(df)
        break  # stop at first available day

    if not all_frames:
        print("[info] no daily archives found in lookback window")
        return

    df_all = pd.concat(all_frames, ignore_index=True)

    # Route rows by balance sheet date (or period_end → fallback today)
    ref = (
        pd.to_datetime(df_all["balance_sheet_date"], errors="coerce")
        .fillna(pd.to_datetime(df_all["period_end"], errors="coerce"))
        .fillna(pd.Timestamp(today))
    )
    years = ref.dt.year.astype(int)
    halves = ref.dt.month.apply(lambda m: "H1" if int(m) <= 6 else "H2")
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
