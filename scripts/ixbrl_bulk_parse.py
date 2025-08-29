from __future__ import annotations
import os, argparse, tempfile, zipfile, io, glob
import pandas as pd
import datetime as dt
from scripts.common import (gh_release_ensure, gh_release_find_asset, gh_release_download_asset,
                            gh_release_upload_or_replace_asset, append_parquet, half_from_date, tag_for_financials)

TRY_IXBRLPARSE = True
try:
    import ixbrlparse  # type: ignore
except Exception:
    TRY_IXBRLPARSE = False

TARGET_COLUMNS = [
    'companies_house_registered_number','entity_current_legal_name','sic_code','company_type','incorporation_date',
    'period_start','period_end','average_number_employees_during_period','turnover_gross_operating_revenue',
    'operating_profit_loss','net_assets_liabilities_including_pension_asset_liability','cash_bank_in_hand'
]
MAP = {
    'turnover': 'turnover_gross_operating_revenue',
    'revenue': 'turnover_gross_operating_revenue',
    'operatingprofitloss': 'operating_profit_loss',
    'netassetsliabilitiesincludingpensionassetliability': 'net_assets_liabilities_including_pension_asset_liability',
    'cashbankinhand': 'cash_bank_in_hand',
    'averagenumberemployeesduringperiod': 'average_number_employees_during_period',
}

def _norm(s: str) -> str:
    return ''.join(ch for ch in s.lower() if ch.isalnum())


def _extract_from_ixbrl_bytes(data: bytes) -> dict:
    if not TRY_IXBRLPARSE:
        return {}
    try:
        facts = ixbrlparse.parse(io.BytesIO(data))  # type: ignore
        out: dict = {}
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
        for cname, target in MAP.items():
            for key, val in facts.items():
                if _norm(key).endswith(cname) and isinstance(val, (int, float, str)):
                    out[target] = val
        return out
    except Exception:
        return {}


def parse_month(path: str) -> pd.DataFrame:
    rows = []
    if os.path.isdir(path):
        entries = [os.path.join(path, p) for p in os.listdir(path) if p.lower().endswith('.zip')]
    else:
        entries = [path]
    for p in entries:
        try:
            with zipfile.ZipFile(p, 'r') as z:
                for info in z.infolist():
                    name = info.filename
                    if name.lower().endswith('.zip'):
                        with z.open(info) as f:
                            data = f.read()
                        with zipfile.ZipFile(io.BytesIO(data), 'r') as nz:
                            for ninfo in nz.infolist():
                                if ninfo.filename.lower().endswith(('.html', '.htm', '.xhtml')):
                                    with nz.open(ninfo) as nf:
                                        doc = nf.read()
                                    rec = _extract_from_ixbrl_bytes(doc)
                                    if rec:
                                        rows.append(rec)
                    elif name.lower().endswith(('.html', '.htm', '.xhtml')):
                        with z.open(info) as f:
                            doc = f.read()
                        rec = _extract_from_ixbrl_bytes(doc)
                        if rec:
                            rows.append(rec)
        except Exception:
            continue
    if not rows:
        return pd.DataFrame(columns=TARGET_COLUMNS)
    df = pd.DataFrame(rows)
    for c in TARGET_COLUMNS:
        if c not in df.columns:
            df[c] = pd.NA
    df['period_start'] = pd.to_datetime(df['period_start'], errors='coerce')
    df['period_end'] = pd.to_datetime(df['period_end'], errors='coerce')
    df['incorporation_date'] = pd.to_datetime(df['incorporation_date'], errors='coerce')
    for c in ['turnover_gross_operating_revenue','operating_profit_loss','net_assets_liabilities_including_pension_asset_liability','cash_bank_in_hand','average_number_employees_during_period']:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    df = df.dropna(subset=['companies_house_registered_number', 'period_end'])
    return df[TARGET_COLUMNS]


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--year', type=int, required=True)
    p.add_argument('--months', type=str, required=True, help='Comma list, e.g. 1,2,3')
    p.add_argument('--inputs', type=str, required=True, help='Folder or zip pattern with month contents (use {YYYY} and {MM})')
    args = p.parse_args()

    months = [int(x) for x in args.months.split(',') if x.strip()]
    for m in months:
        month_glob = args.inputs.replace("{YYYY}", str(args.year)).replace("{MM}", f"{m:02d}")
        paths = sorted(glob.glob(month_glob)) or [args.inputs]
        df_all = []
        for pth in paths:
            df = parse_month(pth)
            if not df.empty:
                df_all.append(df)
        if not df_all:
            continue
        dfm = pd.concat(df_all, ignore_index=True)
        ref = dt.date(args.year, m, 1)
        half = half_from_date(ref)
        rel = gh_release_ensure(tag_for_financials(args.year, half), name=f"Financials {args.year} {half}")
        tmp_out = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False).name
        asset = gh_release_find_asset(rel, "financials.parquet")
        if asset:
            gh_release_download_asset(asset, tmp_out)
        append_parquet(tmp_out, dfm, subset_keys=["companies_house_registered_number", "period_end"])
        gh_release_upload_or_replace_asset(rel, tmp_out, name="financials.parquet")

if __name__ == "__main__":
    main()
