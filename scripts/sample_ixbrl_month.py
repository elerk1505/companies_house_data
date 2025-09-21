#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bulk iXBRL extractor with diagnostics (URL or local .zip).

- Input: a Companies House daily/monthly ZIP **URL** (or local zip path)
- Recursively scans: monthly.zip -> (daily.zip)* -> *.html / *.htm / *.xhtml / *.xhtml.gz
- Extracts identifiers, dates, and common financial fields
- Prints:
    • non-null counts for mapped fields
    • "Top unmapped numeric tags" (tag -> count, sample value)
- Optional: write extracted rows to Parquet/CSV (use --out)

Usage examples:
  python scripts/ixbrl_bulk_parse_with_diagnostics.py --input "https://download.companieshouse.gov.uk/archive/Accounts_Monthly_Data-January2020.zip" --max 500
  python scripts/ixbrl_bulk_parse_with_diagnostics.py --input "/path/to/Accounts_Bulk_Data-2025-09-18.zip" --out out/fin_sample.parquet
"""
from __future__ import annotations

import io
import os
import re
import sys
import gzip
import zipfile
import argparse
from collections import Counter
from typing import Dict, Iterable, Tuple, List, Optional

import requests
from lxml import etree
import pandas as pd

# ------------------------
# Helpers
# ------------------------

NUM_RE = re.compile(r"[-+]?\d[\d,.\s]*")
CURRENCY_JUNK = re.compile(r"[^\d.\-+]")

def clean_numeric(text: str) -> Optional[float]:
    if text is None:
        return None
    t = str(text).strip().replace("\u00A0", " ")
    neg = False
    if t.startswith("(") and t.endswith(")"):
        neg, t = True, t[1:-1]
    t = CURRENCY_JUNK.sub("", t)
    if not t or not re.search(r"\d", t):
        return None
    try:
        val = float(t.replace(",", ""))
        return -val if neg else val
    except Exception:
        return None

def text_or_none(el: etree._Element) -> Optional[str]:
    if el is None:
        return None
    t = "".join(el.itertext()).strip()
    return t or None

def qname(el: etree._Element) -> str:
    # Prefer the XBRL "name" attr on ix:nonFraction/ix:nonNumeric; else element's local-name
    nattr = el.get("name")
    if nattr:
        return nattr
    if el.tag and "}" in el.tag:
        return el.tag.split("}", 1)[1]
    return el.tag

def is_numeric_ix_fact(el: etree._Element) -> bool:
    if el.tag.endswith("nonFraction"):
        return True
    if el.tag.endswith("nonNumeric"):
        return clean_numeric(text_or_none(el)) is not None
    return False

def iter_ix_facts(doc: etree._ElementTree) -> Iterable[etree._Element]:
    # Match ix facts regardless of namespace prefix
    xpath = "//*[local-name()='nonFraction' or local-name()='nonNumeric']"
    for el in doc.xpath(xpath):
        yield el

def load_bytes(path_or_url: str) -> bytes:
    if path_or_url.startswith(("http://", "https://")):
        print(f"[info] downloading: {path_or_url}")
        r = requests.get(path_or_url, timeout=600)
        r.raise_for_status()
        return r.content
    with open(path_or_url, "rb") as f:
        return f.read()

def _looks_like_zip(name: str) -> bool:
    return name.lower().endswith(".zip")

def _looks_like_ixbrl(name: str) -> bool:
    n = name.lower()
    return n.endswith((".html", ".htm", ".xhtml", ".xhtml.gz"))

def _read_member_to_bytes(zf: zipfile.ZipFile, member: zipfile.ZipInfo) -> bytes:
    with zf.open(member, "r") as fh:
        blob = fh.read()
    if member.filename.lower().endswith(".gz"):
        try:
            return gzip.decompress(blob)
        except Exception:
            return gzip.GzipFile(fileobj=io.BytesIO(blob)).read()
    return blob

def iter_html_from_zip_bytes(zip_bytes: bytes, diag_head: int = 40) -> Iterable[Tuple[str, bytes]]:
    """
    Yield (name, html_bytes) for any iXBRL HTML found under:
    monthly.zip -> (daily.zip)* -> (folders)* -> *.html/*.htm/*.xhtml/*.xhtml.gz
    """
    with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as monthly:
        names = monthly.namelist()
        print(f"[diag] monthly members (first {min(diag_head, len(names))}):")
        for nm in names[:diag_head]:
            print(f"  - {nm}")

        for m in monthly.infolist():
            mname = m.filename
            if _looks_like_zip(mname):
                # inner daily zip
                daily_bytes = _read_member_to_bytes(monthly, m)
                with zipfile.ZipFile(io.BytesIO(daily_bytes), "r") as daily:
                    dnames = daily.namelist()
                    print(f"[diag] daily zip '{mname}' has {len(dnames)} entries; e.g. {dnames[:5]}")
                    for dmem in daily.infolist():
                        dname = dmem.filename
                        if _looks_like_ixbrl(dname):
                            yield dname, _read_member_to_bytes(daily, dmem)
            elif _looks_like_ixbrl(mname):
                # rare: html directly in monthly zip
                yield mname, _read_member_to_bytes(monthly, m)

# ------------------------
# Mapping (extend/adjust as needed)
# ------------------------

ID_MAP = {
    "bus:UKCompaniesHouseRegisteredNumber": "companies_house_registered_number",
    "bus:EntityCurrentLegalOrRegisteredName": "entity_current_legal_name",
    "core:BalanceSheetDate": "balance_sheet_date",
    "bus:StartDateForPeriodCoveredByReport": "period_start",
    "bus:EndDateForPeriodCoveredByReport": "period_end",
}

FIN_MAP = {
    # Balance sheet / equity
    "core:CashBankOnHand": "cash_bank_in_hand",
    "core:Debtors": "debtors",
    "core:CurrentAssets": "current_assets",
    "core:CreditorsDueWithinOneYear": "creditors_due_within_one_year",
    "core:CreditorsAfterOneYear": "creditors_due_after_one_year",
    "core:NetCurrentAssetsLiabilities": "net_current_assets_liabilities",
    "core:TotalAssetsLessCurrentLiabilities": "total_assets_less_current_liabilities",
    "core:NetAssetsLiabilitiesIncludingPensionAssetLiability": "net_assets_liabilities_including_pension_asset_liability",
    "core:CalledUpShareCapitalPresentedWithinEquity": "called_up_share_capital",
    "core:ProfitLossAccountReserve": "profit_loss_account_reserve",
    "core:Equity": "shareholder_funds",

    # P&L (if provided; many filleted accounts omit)
    "core:TurnoverGrossOperatingRevenue": "turnover_gross_operating_revenue",
    "core:OtherOperatingIncome": "other_operating_income",
    "core:CostSales": "cost_sales",
    "core:GrossProfitLoss": "gross_profit_loss",
    "core:AdministrativeExpenses": "administrative_expenses",
    "core:RawMaterialsConsumablesUsed": "raw_materials_consumables",
    "core:StaffCostsEmployeeBenefitsExpense": "staff_costs",
    "core:DepreciationAmortisationImpairmentExpense": "depreciation_other_amounts_written_off_tangible_intangible_fixed_assets",
    "core:OtherOperatingChargesFormat2": "other_operating_charges_format2",
    "core:OperatingProfitLoss": "operating_profit_loss",
    "core:ProfitLossBeforeTax": "profit_loss_on_ordinary_activities_before_tax",
    "core:TaxExpenseContinuingOperations": "tax_on_profit_or_loss_on_ordinary_activities",
    "core:ProfitLoss": "profit_loss_for_period",

    # Employment
    "core:AverageNumberEmployeesDuringPeriod": "average_number_employees_during_period",
}

# Add discovered synonyms here as you find them in the diagnostics
SYNONYMS = {
    # "ifrs-full:CashAndCashEquivalents": "cash_bank_in_hand",
    # "uk-gaap:NetCurrentAssetsLiabilities": "net_current_assets_liabilities",
}
FIN_MAP.update(SYNONYMS)

OUTPUT_COLS = [
    "companies_house_registered_number",
    "entity_current_legal_name",
    "balance_sheet_date",
    "period_start",
    "period_end",
] + list(FIN_MAP.values())

# ------------------------
# Single-file extraction
# ------------------------

def extract_from_html_bytes(html_bytes: bytes, source_name: str = "") -> Dict[str, object]:
    row: Dict[str, object] = {c: None for c in OUTPUT_COLS}
    row["_source"] = source_name
    row["_unknown_numeric_tags"] = []  # filled with [(tag, sample_value), ...]

    parser = etree.HTMLParser(recover=True, huge_tree=True)
    try:
        doc = etree.parse(io.BytesIO(html_bytes), parser=parser)
    except Exception:
        try:
            doc = etree.parse(io.BytesIO(html_bytes), etree.XMLParser(recover=True, huge_tree=True))
        except Exception:
            row["_error"] = "parse_failed"
            return row

    for el in iter_ix_facts(doc):
        tag = qname(el)
        txt = text_or_none(el)

        # IDs / dates (keep raw then coerce)
        if tag in ID_MAP:
            col = ID_MAP[tag]
            if row.get(col) is None and txt:
                row[col] = txt
            continue

        # Numeric
        if is_numeric_ix_fact(el):
            val = clean_numeric(txt or "")
            if val is None:
                continue
            if tag in FIN_MAP:
                col = FIN_MAP[tag]
                if row.get(col) is None:
                    row[col] = val
            else:
                # collect unknown numeric tags for diagnostics
                sample = (txt or "").strip().replace("\n", " ")
                if len(sample) > 100:
                    sample = sample[:97] + "..."
                row["_unknown_numeric_tags"].append((tag, sample))

    return row

# ------------------------
# Bulk runner
# ------------------------

def coerce_date_strings(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.strftime("%Y-%m-%d")
    return df

def run_bulk(input_path_or_url: str, max_files: int = 0):
    data = load_bytes(input_path_or_url)
    if not zipfile.is_zipfile(io.BytesIO(data)):
        raise ValueError("Input must be a .zip URL or path")

    records: List[Dict[str, object]] = []
    unk_counter: Counter = Counter()
    unk_sample: Dict[str, str] = {}

    for i, (name, html_bytes) in enumerate(iter_html_from_zip_bytes(data), start=1):
        rec = extract_from_html_bytes(html_bytes, source_name=name)

        # gather unknowns
        for tag, sample in rec.pop("_unknown_numeric_tags", []):
            unk_counter[tag] += 1
            if tag not in unk_sample:
                unk_sample[tag] = sample

        records.append(rec)
        if max_files and i >= max_files:
            break

    df = pd.DataFrame.from_records(records)
    if not df.empty:
        df = coerce_date_strings(df, ["balance_sheet_date", "period_start", "period_end"])
    return df, unk_counter, unk_sample

# ------------------------
# CLI
# ------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="CH monthly/daily .zip URL or local .zip path")
    ap.add_argument("--max", type=int, default=0, help="Stop after N files (0 = all)")
    ap.add_argument("--out", type=str, default="", help="Optional output file (.parquet or .csv)")
    ap.add_argument("--topn", type=int, default=40, help="Top N unmapped tags to print")
    args = ap.parse_args()

    print(f"[info] scanning: {args.input}")
    df, unk_counter, unk_sample = run_bulk(args.input, max_files=args.max)

    print(f"[info] parsed files: {len(df)}")
    if not df.empty:
        nn = df[OUTPUT_COLS].notna().sum().sort_values(ascending=False)
        print("\n[diag] non-null counts (top 20):")
        print(nn.head(20).to_string())

    if unk_counter:
        print("\n[diag] Top unmapped numeric tags:")
        rows = []
        for tag, cnt in unk_counter.most_common(args.topn):
            rows.append({"tag": tag, "count": cnt, "sample_value": unk_sample.get(tag, "")})
        print(pd.DataFrame(rows).to_string(index=False))
    else:
        print("\n[diag] No unmapped numeric tags encountered.")

    if args.out and not df.empty:
        out = args.out
        os.makedirs(os.path.dirname(os.path.abspath(out)) or ".", exist_ok=True)
        low = out.lower()
        if low.endswith(".parquet"):
            try:
                import pyarrow as pa  # noqa
                import pyarrow.parquet as pq  # noqa
                df.to_parquet(out, index=False)
                print(f"[info] wrote: {out}")
            except Exception as e:
                print(f"[warn] pyarrow not available ({e}); writing CSV instead")
                df.to_csv(out + ".csv", index=False)
                print(f"[info] wrote: {out}.csv")
        elif low.endswith(".csv"):
            df.to_csv(out, index=False)
            print(f"[info] wrote: {out}")
        else:
            # choose parquet if available else csv
            try:
                import pyarrow as pa  # noqa
                df.to_parquet(out + ".parquet", index=False)
                print(f"[info] wrote: {out}.parquet")
            except Exception:
                df.to_csv(out + ".csv", index=False)
                print(f"[info] wrote: {out}.csv")

if __name__ == "__main__":
    main()
