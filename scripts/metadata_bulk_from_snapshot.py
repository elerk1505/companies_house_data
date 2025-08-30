# ================================================================
# scripts/metadata_bulk_from_snapshot.py
# Build metadata from Companies House "Basic Company Data" snapshot
# and append it to data-YYYY-H?-metadata/metadata.parquet
# ID matching is aligned to your iXBRL outputs: digits-only, no leading zeros.
# ================================================================
from __future__ import annotations

import io
import zipfile
import tempfile
import argparse
import datetime as dt
from typing import Dict, List

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

# Map snapshot column names -> our unified schema
SNAP_TO_OURS: Dict[str, str] = {
    # Core identifiers
    "CompanyNumber": "companies_house_registered_number",
    "CompanyName": "entity_current_legal_name",
    "CompanyCategory": "company_type",
    "CompanyStatus": "company_status",
    "CountryOfOrigin": "country_of_origin",
    "IncorporationDate": "incorporation_date",
    "DissolutionDate": "dissolution_date",
    "URI": "uri",
    # Registered office (full address detail)
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
    # Accounts / Returns / Confirmation statement
    "Accounts.AccountRefDay": "accounts_ref_day",
    "Accounts.AccountRefMonth": "accounts_ref_month",
    "Accounts.NextDueDate": "accounts_next_due_date",
    "Accounts.LastMadeUpDate": "accounts_last_made_up_date",
    "Returns.NextDueDate": "returns_next_due_date",
    "Returns.LastMadeUpDate": "returns_last_made_up_date",
    "ConfStmtNextDueDate": "conf_stmt_next_due_date",
    "ConfStmtLastMadeUpDate": "conf_stmt_last_made_up_date",
    # Mortgages
    "Mortgages.NumMortCharges": "mortgages_num_charges",
    "Mortgages.NumMortOutstanding": "mortgages_num_outstanding",
    "Mortgages.NumMortPartSatisfied": "mortgages_num_part_satisfied",
    "Mortgages.NumMortSatisfied": "mortgages_num_satisfied",
    # SIC text columns (we’ll compress to list)
    "SICCode.SicText_1": "sic1",
    "SICCode.SicText_2": "sic2",
    "SICCode.SicText_3": "sic3",
    "SICCode.SicText_4": "sic4",
}

# Final column order we write to the parquet
META_COLUMNS: List[str] = [
    "companies_house_registered_number",
    "entity_current_legal_name",
    "company_type",
    "company_status",
    "country_of_origin",
    "incorporation_date",
    "dissolution_date",
    "sic_codes",
    # Registered office (full)
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
    # Accounts
    "accounts_ref_day",
    "accounts_ref_month",
    "accounts_next_due_date",
    "accounts_last_made_up_date",
    # Returns / Confirmation statement
    "returns_next_due_date",
    "returns_last_made_up_date",
    "conf_stmt_next_due_date",
    "conf_stmt_last_made_up_date",
    # Mortgages
    "mortgages_num_charges",
    "mortgages_num_outstanding",
    "mortgages_num_part_satisfied",
    "mortgages_num_satisfied",
    # Misc
    "uri",
    "last_updated",
]

# ---------- ID normalisation to match your iXBRL outputs ----------
def normalize_to_ixbrl_format(x):
    """
    Match your financials 'company_id' style:
    - keep digits only
    - strip leading zeros
    - return None if nothing left
    """
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = "".join(ch for ch in str(x).strip() if ch.isdigit())
    s = s.lstrip("0")
    return s or None

# ---------- Snapshot helpers ----------
def onefile_url(year: int, month: int) -> str:
    """Single-file snapshot URL (always dated on the 1st)."""
    return f"https://download.companieshouse.gov.uk/BasicCompanyDataAsOneFile-{year}-{month:02d}-01.zip"

def load_snapshot_df(url: str) -> pd.DataFrame:
    """Download the monthly ZIP and return its CSV as a DataFrame (all strings)."""
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
    # Keep only columns we know and rename to our schema
    keep = {src: dst for src, dst in SNAP_TO_OURS.items() if src in snap.columns}
    df = snap[list(keep.keys())].rename(columns=keep)

    # Build SIC list
    sic_cols = [c for c in ["sic1", "sic2", "sic3", "sic4"] if c in df.columns]
    if sic_cols:
        def pack_sic(row):
            vals = [str(row[c]).strip() for c in sic_cols if pd.notna(row[c]) and str(row[c]).strip()]
            return vals if vals else None
        df["sic_codes"] = df.apply(pack_sic, axis=1)
        df.drop(columns=[c for c in sic_cols if c in df.columns], inplace=True)
    else:
        df["sic_codes"] = None

    # Coerce date-like strings (silently ignore bad formats)
    for c in [
        "incorporation_date", "dissolution_date",
        "accounts_next_due_date", "accounts_last_made_up_date",
        "returns_next_due_date", "returns_last_made_up_date",
        "conf_stmt_next_due_date", "conf_stmt_last_made_up_date",
    ]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    # Ensure all final columns exist
    for c in META_COLUMNS:
        if c not in df.columns:
            df[c] = pd.NA

    # Keep final order
    return df[META_COLUMNS]

# ---------- Main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True, help="Target year (e.g., 2025)")
    ap.add_argument("--month", type=int, required=True, help="Snapshot month (1–12)")
    ap.add_argument("--half", type=str, required=True, choices=["H1", "H2"], help="Target half")
    args = ap.parse_args()

    # Build URL; auto-fallback to previous month if the chosen one isn't published yet
    url = onefile_url(args.year, args.month)
    try:
        print(f"[info] downloading snapshot: {url}")
        snap_raw = load_snapshot_df(url)
    except FileNotFoundError:
        # previous month
        d = dt.date(args.year, args.month, 1) - dt.timedelta(days=1)
        url_prev = onefile_url(d.year, d.month)
        print(f"[warn] snapshot not found, trying previous month: {url_prev}")
        snap_raw = load_snapshot_df(url_prev)

    print(f"[info] snapshot rows: {len(snap_raw):,}")

    # Map to our schema and NORMALIZE IDs to match your iXBRL format
    meta = to_metadata_schema(snap_raw)
    meta["companies_house_registered_number"] = meta[
        "companies_house_registered_number"
    ].map(normalize_to_ixbrl_format)

    # 1) Load company IDs from the financials release for the chosen half
    fin_tag = f"data-{args.year}-{args.half}-financials"
    fin_rel = gh_release_ensure(fin_tag)
    fin_asset = gh_release_find_asset(fin_rel, "financials.parquet")
    if not fin_asset:
        print(f"[error] no financials.parquet in {fin_tag}")
        return

    tmp_fin = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False).name
    gh_release_download_asset(fin_asset, tmp_fin)
    fin_ids = (
        pd.read_parquet(tmp_fin, columns=["companies_house_registered_number"])
        ["companies_house_registered_number"]
        .map(normalize_to_ixbrl_format)
        .dropna()
        .unique()
    )
    fin_ids_set = set(fin_ids)
    print(f"[info] found {len(fin_ids_set)} company IDs in {fin_tag}")

    # 2) Filter snapshot to only IDs present in financials (post-normalisation)
    before = len(meta)
    meta = meta[meta["companies_house_registered_number"].isin(fin_ids_set)]
    after = len(meta)
    print(f"[info] matched to financials: {after:,} / {before:,} rows")

    if after == 0:
        print("[warn] no matching rows; nothing to append")
        return

    # Stamp last_updated
    meta["last_updated"] = pd.Timestamp.utcnow().isoformat()

    # 3) Append (dedupe by company id) into data-YYYY-H?-metadata/metadata.parquet
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
