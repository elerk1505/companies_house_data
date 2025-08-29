# ================================================================
# scripts/ixbrl_fetch_daily.py  — Action 1 (Daily Financials)
# ================================================================
"""
Fetch the last 5 days of Companies House daily accounts ZIPs and append parsed
financials into the correct half‑year Parquet stored as a GitHub Release asset.

• Skips 404s automatically (some dates won't exist yet)
• Skips already‑processed URLs via state/financials_state.json (committed by the workflow)
• De‑dupes on (companies_house_registered_number, period_end)
• Handles nested ZIPs that contain iXBRL HTML files

Requirements in the workflow step:
    python -m pip install -U pip pandas pyarrow requests ixbrlparse

Depends on helper utilities in scripts/common.py (uploaded earlier):
  - SESSION, gh_release_ensure, gh_release_find_asset, gh_release_download_asset,
    gh_release_upload_or_replace_asset, append_parquet, half_from_date, tag_for_financials
"""
from __future__ import annotations
import os, json, io, tempfile, datetime as dt, zipfile, re
from typing import List, Dict
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

# Try fast HTML iXBRL parsing; otherwise we'll use lightweight regex fallbacks
TRY_IXBRLPARSE = True
try:
    import ixbrlparse  # type: ignore
except Exception:
    TRY_IXBRLPARSE = False

# Target output columns
TARGET_COLUMNS = [
    'companies_house_registered_number',
    'entity_current_legal_name',
    'sic_code',
    'company_type',
    'incorporation_date',
    'period_start',
    'period_end',
    'average_number_employees_during_period',
    'turnover_gross_operating_revenue',
    'operating_profit_loss',
    'net_assets_liabilities_including_pension_asset_liability',
    'cash_bank_in_hand',
]

# Aliases to catch common UK concepts in iXBRL
ALIASES = {
    'turnover': 'turnover_gross_operating_revenue',
    'revenue': 'turnover_gross_operating_revenue',
    'operatingprofitloss': 'operating_profit_loss',
    'netassetsliabilitiesincludingpensionassetliability': 'net_assets_liabilities_including_pension_asset_liability',
    'cashbankinhand': 'cash_bank_in_hand',
    'averagenumberemployeesduringperiod': 'average_number_employees_during_period',
}

_norm = lambda s: ''.join(ch for ch in s.lower() if ch.isalnum())
CH_NUM_RE = re.compile(rb"[^0-9]([0-9]{8})[^0-9]")
DATE_RE = re.compile(rb"(20[0-9]{2}-[01][0-9]-[0-3][0-9])")


def _extract_from_ixbrl_bytes(data: bytes) -> Dict[str, object]:
    """Parse a single iXBRL HTML/XHTML document into our target dict.
    Returns {} if required keys can't be found.
    """
    out: Dict[str, object] = {}
    if TRY_IXBRLPARSE:
        try:
            facts = ixbrlparse.parse(io.BytesIO(data))  # type: ignore
            # Names
            for k in ('entityCurrentLegalName', 'EntityCurrentLegalName'):
                if k in facts:
                    out['entity_current_legal_name'] = facts[k]
                    break
            for k in ('companiesHouseRegisteredNumber', 'CompaniesHouseRegisteredNumber'):
                if k in facts:
                    out['companies_house_registered_number'] = str(facts[k])
                    break
            for k in ('periodStart', 'PeriodStart', 'period_start'):
                if k in facts:
                    out['period_start'] = str(facts[k])
                    break
            for k in ('periodEnd', 'PeriodEnd', 'period_end'):
                if k in facts:
                    out['period_end'] = str(facts[k])
                    break
            # numeric concepts by alias map
            for cname, target in ALIASES.items():
                for key, val in facts.items():
                    if _norm(key).endswith(cname) and isinstance(val, (int, float, str)):
                        out[target] = val
        except Exception:
            out = {}

    if not out:
        # very light regex fallback to salvage company number and dates if parser unavailable
        try:
            m = CH_NUM_RE.search(data)
            if m:
                out['companies_house_registered_number'] = m.group(1).decode('utf-8')
            # try to infer period_end from any ISO date in doc (best-effort)
            dates = DATE_RE.findall(data)
            if dates:
                out['period_end'] = dates[-1].decode('utf-8')
        except Exception:
            pass
    return out


def _parse_daily_zip(zip_path: str) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    with zipfile.ZipFile(zip_path, 'r') as z:
        for info in z.infolist():
            name = info.filename
            # Nested monthly parts often appear as inner ZIPs containing HTML/XHTML
            if name.lower().endswith('.zip'):
                with z.open(info) as f:
                    inner = f.read()
                try:
                    with zipfile.ZipFile(io.BytesIO(inner), 'r') as nz:
                        for ninfo in nz.infolist():
                            if ninfo.filename.lower().endswith(('.html', '.htm', '.xhtml')):
                                with nz.open(ninfo) as nf:
                                    doc = nf.read()
                                rec = _extract_from_ixbrl_bytes(doc)
                                if rec:
                                    rows.append(rec)
                except zipfile.BadZipFile:
                    continue
            elif name.lower().endswith(('.html', '.htm', '.xhtml')):
                with z.open(info) as f:
                    doc = f.read()
                rec = _extract_from_ixbrl_bytes(doc)
                if rec:
                    rows.append(rec)
            else:
                continue
    if not rows:
        return pd.DataFrame(columns=TARGET_COLUMNS)
    df = pd.DataFrame(rows)
    # ensure all columns exist
    for c in TARGET_COLUMNS:
        if c not in df.columns:
            df[c] = pd.NA
    # coerce types
    for c in ['period_start','period_end','incorporation_date']:
        df[c] = pd.to_datetime(df[c], errors='coerce')
    for c in ['turnover_gross_operating_revenue','operating_profit_loss','net_assets_liabilities_including_pension_asset_liability','cash_bank_in_hand','average_number_employees_during_period']:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    # drop incomplete
    df = df.dropna(subset=['companies_house_registered_number', 'period_end'])
    return df[TARGET_COLUMNS]


def main():
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    state = {"processed": []}
    if os.path.exists(STATE_PATH):
        state = json.load(open(STATE_PATH, 'r', encoding='utf-8'))

    today = dt.date.today()
    # Companies House publishes Tue–Sat (covering previous days). We just grab the last 5 calendar days safely.
    check_dates = [today - dt.timedelta(days=i) for i in range(1, 6)]

    for d in check_dates:
        date_str = d.strftime('%Y-%m-%d')
        fname = DAILY_FILE_FMT.format(date=date_str)
        url = f"{DAILY_BASE_URL}{fname}"
        if url in state.get('processed', []):
            continue
        r = SESSION.get(url, timeout=600)
        if r.status_code == 404:
            # not published for that date
            continue
        r.raise_for_status()
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as f:
            f.write(r.content)
            zpath = f.name
        df = _parse_daily_zip(zpath)
        # mark processed regardless (prevents re-downloading empty days repeatedly)
        state.setdefault('processed', []).append(url)
        if df.empty:
            continue
        # choose release (by period_end if available; else by the file date)
        pe = pd.to_datetime(df['period_end'], errors='coerce')
        ref_date = pe.dropna().iloc[0].date() if pe.notna().any() else d
        year = ref_date.year
        half = half_from_date(ref_date)
        rel = gh_release_ensure(tag_for_financials(year, half), name=f"Financials {year} {half}")
        tmp_out = tempfile.NamedTemporaryFile(suffix='.parquet', delete=False).name
        asset = gh_release_find_asset(rel, OUTPUT_BASENAME)
        if asset:
            gh_release_download_asset(asset, tmp_out)
        append_parquet(tmp_out, df, subset_keys=['companies_house_registered_number', 'period_end'])
        gh_release_upload_or_replace_asset(rel, tmp_out, name=OUTPUT_BASENAME)

    json.dump(state, open(STATE_PATH, 'w', encoding='utf-8'), indent=2)

if __name__ == '__main__':
    main()
