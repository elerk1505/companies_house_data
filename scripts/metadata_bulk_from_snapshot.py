# ================================================================
# scripts/metadata_bulk_from_snapshot.py
# Build metadata from Companies House "Basic Company Data" snapshot
# Append to data-YYYY-H?-metadata/metadata.parquet
# Matching uses your iXBRL ID style (digits only, no leading zeros).
# Adds diagnostics + fallback to 'company_id' if needed.
# ================================================================
from __future__ import annotations

import io
import zipfile
import tempfile
import argparse
import datetime as dt
from typing import Dict, List, Iterable

import pandas as pd
import requests

from scripts.common import (
    gh_release_ensure,
    gh_release_find_asset,
    gh_release_download_asset,
    gh_release_upload_or_replace_asset,
    append_parquet,
)

OUTPUT_BASENAME = "metadata.parquet"

SNAP_TO_OURS: Dict[str, str] = {
    "CompanyNumber": "companies_house_registered_number",
    "CompanyName": "entity_current_legal_name",
    "CompanyCategory": "company_type",
    "CompanyStatus": "company_status",
    "CountryOfOrigin": "country_of_origin",
    "IncorporationDate": "incorporation_date",
    "DissolutionDate": "dissolution_date",
    "URI": "uri",
    "RegAddress.CareOf": "registered_office_care_of",
    "RegAddress.POBox": "registered_office_po_box",
    "RegAddress.AddressLine1": "registered_office_address_line_1",
    "RegAddress.AddressLine2": "registered_office_address_line_2",
    "RegAddress.AddressLine3": "registered_office_address_line_3",
    "RegAddress.AddressLine4": "registered_office_address_line_4",
    "RegAddress.PostTown": "registered_office_post_town",
    "RegAddress.County": "registered_office_county",
    "RegAddress.Country": "registered_office_country",
    "RegAddress.PostCode": "registered_office_postcode",
    "Accounts.AccountRefDay": "accounts_ref_day",
    "Accounts.AccountRefMonth": "accounts_ref_month",
    "Accounts.NextDueDate": "accounts_next_due_date",
    "Accounts.LastMadeUpDate": "accounts_last_made_up_date",
    "Returns.NextDueDate": "returns_next_due_date",
    "Returns.LastMadeUpDate": "returns_last_made_up_date",
    "ConfStmtNextDueDate": "conf_stmt_next_due_date",
    "ConfStmtLastMadeUpDate": "conf_stmt_last_made_up_date",
    "Mortgages.NumMortCharges": "mortgages_num_charges",
    "Mortgages.NumMortOutstanding": "mortgages_num_outstanding",
    "Mortgages.NumMortPartSatisfied": "mortgages_num_part_satisfied",
    "Mortgages.NumMortSatisfied": "mortgages_num_satisfied",
    "SICCode.SicText_1": "sic1",
    "SICCode.SicText_2": "sic2",
    "SICCode.SicText_3": "sic3",
    "SICCode.SicText_4": "sic4",
}

META_COLUMNS: List[str] = [
    "companies_house_registered_number",
    "entity_current_legal_name",
    "company_type",
    "company_status",
    "country_of_origin",
    "incorporation_date",
    "dissolution_date",
    "sic_codes",
    "registered_office_care_of",
    "registered_office_po_box",
    "registered_office_address_line_1",
    "registered_office_address_line_2",
    "registered_office_address_line_3",
    "registered_office_address_line_4",
    "registered_office_post_town",
    "registered_office_county",
    "registered_office_country",
    "registered_office_postcode",
    "accounts_ref_day",
    "accounts_ref_month",
    "accounts_next_due_date",
    "accounts_last_made_up_date",
    "returns_next_due_date",
    "returns_last_made_up_date",
    "conf_stmt_next_due_date",
    "conf_stmt_last_made_up_date",
    "mortgages_num_charges",
    "mortgages_num_outstanding",
    "mortgages_num_part_satisfied",
    "mortgages_num_satisfied",
    "uri",
    "last_updated",
]

# ---------- Normalisation (match your iXBRL IDs) ----------
def normalize_to_ixbrl_format(x):
    """Keep digits only; strip leading zeros; None if empty."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = "".join(ch for ch in str(x).strip() if ch.isdigit())
    s = s.lstrip("0")
    return s or None

def summarize_ids(label: str, ids: Iterable[str], sample: int = 10) -> None:
    ids = [i for i in ids if i]
    lens = [len(i) for i in ids[:5000]]  # sample lens
    print(f"[debug] {label}: count={len(ids)} "
          f"min_len={min(lens) if lens else 'n/a'} max_len={max(lens) if lens else 'n/a'}")
    print(f"[debug] {label} sample: {ids[:sample]}")

# ---------- Snapshot helpers ----------
def onefile_url(year: int, month: int) -> str:
    return f"https://download.companieshouse.gov.uk/BasicCompanyDataAsOneFile-{year}-{month:02d}-01.zip"

def load_snapshot_df(url: str) -> pd.DataFrame:
    r = requests.get(url, timeout=600)
    if r.status_code == 404:
        raise FileNotFoundError(url)
    r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        csv_names = [n for n in z.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise RuntimeError("No CSV file found in snapshot ZIP.")
        with z.open(csv_names[0]) as f:
            df = pd.read_csv(f, dtype=str)
    return df

def to_metadata_schema(snap: pd.DataFrame) -> pd.DataFrame:
    keep = {src: dst for src, dst in SNAP_TO_OURS.items() if src in snap.columns}
    df = snap[list(keep.keys())].rename(columns=keep)
    sic_cols = [c for c in ["sic1", "sic2", "sic3", "sic4"] if c in df.columns]
    if sic_cols:
        def pack_sic(row):
            vals = [str(row[c]).strip() for c in sic_cols if pd.notna(row[c]) and str(row[c]).strip()]
            return vals if vals else None
        df["sic_codes"] = df.apply(pack_sic, axis=1)
        df.drop(columns=[c for c in sic_cols if c in df.columns], inplace=True)
    else:
        df["sic_codes"] = None
    for c in [
        "incorporation_date", "dissolution_date",
        "accounts_next_due_date", "accounts_last_made_up_date",
        "returns_next_due_date", "returns_last_made_up_date",
        "conf_stmt_next_due_date", "conf_stmt_last_made_up_date",
    ]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    for c in META_COLUMNS:
        if c not in df.columns:
            df[c] = pd.NA
    return df[META_COLUMNS]

# ---------- Main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True, help="Target year (e.g., 2025)")
    ap.add_argument("--month", type=int, required=True, help="Snapshot month (1–12)")
    ap.add_argument("--half", type=str, required=True, choices=["H1", "H2"], help="Target half")
    args = ap.parse_args()

    # Download snapshot (fallback to previous month if needed)
    url = onefile_url(args.year, args.month)
    try:
        print(f"[info] downloading snapshot: {url}")
        snap_raw = load_snapshot_df(url)
    except FileNotFoundError:
        d = dt.date(args.year, args.month, 1) - dt.timedelta(days=1)
        url_prev = onefile_url(d.year, d.month)
        print(f"[warn] snapshot not found, trying previous month: {url_prev}")
        snap_raw = load_snapshot_df(url_prev)
    print(f"[info] snapshot rows: {len(snap_raw):,}")

    meta = to_metadata_schema(snap_raw)
    # NORMALIZE snapshot IDs
    meta["companies_house_registered_number"] = meta["companies_house_registered_number"].map(
        normalize_to_ixbrl_format
    )

    # Load financials IDs (try CH number first, then fallback to 'company_id')
    fin_tag = f"data-{args.year}-{args.half}-financials"
    fin_rel = gh_release_ensure(fin_tag)
    fin_asset = gh_release_find_asset(fin_rel, "financials.parquet")
    if not fin_asset:
        print(f"[error] no financials.parquet in {fin_tag}")
        return
    tmp_fin = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False).name
    gh_release_download_asset(fin_asset, tmp_fin)
    fin_df = pd.read_parquet(tmp_fin)

    # Build candidate ID sets from financials
    fin_ids_primary = pd.Series(dtype=object)
    if "companies_house_registered_number" in fin_df.columns:
        fin_ids_primary = fin_df["companies_house_registered_number"]
    fin_ids_fallback = pd.Series(dtype=object)
    if "company_id" in fin_df.columns:
        fin_ids_fallback = fin_df["company_id"]

    fin_ids_primary = fin_ids_primary.map(normalize_to_ixbrl_format).dropna().astype(str).unique().tolist()
    fin_ids_fallback = fin_ids_fallback.map(normalize_to_ixbrl_format).dropna().astype(str).unique().tolist()

    # Snapshot ID set
    snap_ids = meta["companies_house_registered_number"].dropna().astype(str).unique().tolist()

    # Diagnostics
    summarize_ids("financials.primary(ch_number)", fin_ids_primary)
    summarize_ids("financials.fallback(company_id)", fin_ids_fallback)
    summarize_ids("snapshot.ids", snap_ids)

    # Choose which financials ID set actually intersects snapshot
    set_snap = set(snap_ids)
    inter_primary = set(set(fin_ids_primary)) & set_snap
    inter_fallback = set(set(fin_ids_fallback)) & set_snap
    print(f"[debug] intersection sizes -> primary: {len(inter_primary)}  fallback: {len(inter_fallback)}")

    fin_ids_set = inter_primary or inter_fallback  # prefer primary if it hits
    if not fin_ids_set:
        print("[warn] No intersection found with either key — nothing to append.")
        return

    # Show a few matched IDs for sanity
    matched_sample = list(fin_ids_set)[:10]
    print(f"[debug] matched sample: {matched_sample}")

    # Filter snapshot rows to matched IDs
    before = len(meta)
    meta = meta[meta["companies_house_registered_number"].isin(fin_ids_set)]
    after = len(meta)
    print(f"[info] matched to financials: {after:,} / {before:,} rows")

    if after == 0:
        print("[warn] no matching rows after filter; nothing to append")
        return

    meta["last_updated"] = pd.Timestamp.utcnow().isoformat()

    # Write/append to metadata release (dedupe on company id)
    meta_tag = f"data-{args.year}-{args.half}-metadata"
    meta_rel = gh_release_ensure(meta_tag, name=f"Metadata {args.year} {args.half}")
    tmp_out = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False).name
    asset = gh_release_find_asset(meta_rel, OUTPUT_BASENAME)
    if asset:
        gh_release_download_asset(asset, tmp_out)

    append_parquet(tmp_out, meta, subset_keys=["companies_house_registered_number"])
    gh_release_upload_or_replace_asset(meta_rel, tmp_out, name=OUTPUT_BASENAME)
    print(f"[ok] snapshot merged → {meta_tag} (+{after} rows)")

if __name__ == "__main__":
    main()
