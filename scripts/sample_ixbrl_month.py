#!/usr/bin/env python3
# scripts/dump_ixbrl_concepts.py
#
# Purpose: Inspect a Companies House monthly Accounts_Bulk_Data zip,
# list the iXBRL concept QNames actually present, and show sample values.
#
# This does NOT rely on a particular Python ixbrl library; instead it reads
# ix:nonFraction / ix:nonNumeric tags with BeautifulSoup (lxml parser).
#
# Outputs:
#  - stdout summary of top concepts
#  - concept_counts.csv (qname, kind, count, sample_value, min, max)
#  - concept_samples.txt (pretty text incl. suggested FIN_MAP entries)
#
# Usage (locally):
#   python scripts/dump_ixbrl_concepts.py --url "https://download.companieshouse.gov.uk/Accounts_Monthly_Data-January2025.zip" --limit 300
#
# In GitHub Actions use the provided workflow (sample_monthly_concepts.yml).

from __future__ import annotations

import argparse
import collections
import io
import math
import os
import re
import sys
import zipfile
from typing import Dict, Iterable, List, Tuple

import requests
import pandas as pd
from bs4 import BeautifulSoup

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

NUM_RX = re.compile(r"[-+]?\d{1,3}(?:[,\s]\d{3})*(?:\.\d+)?|[-+]?\d+(?:\.\d+)?")

def as_number_or_none(text: str):
    """
    Try to pull a numeric value from iXBRL nonFraction text.
    We strip currency symbols/commas and keep sign/decimals.
    """
    if not text:
        return None
    m = NUM_RX.search(text.replace("\xa0", " ").strip())
    if not m:
        return None
    raw = m.group(0)
    # Remove thousands separators (comma/space) but keep minus/decimal
    cleaned = raw.replace(",", "").replace(" ", "")
    try:
        return float(cleaned)
    except ValueError:
        return None

def iter_ixbrl_html_bytes_from_zip(z: zipfile.ZipFile) -> Iterable[Tuple[str, bytes]]:
    """
    Yield (name, bytes) for each ixbrl HTML file inside the zip.
    If there are nested zips, descend and yield their HTML members too.
    """
    for info in z.infolist():
        name = info.filename
        if name.endswith("/"):
            continue
        with z.open(info) as f:
            data = f.read()

        lname = name.lower()
        if lname.endswith(".zip"):
            # Nested daily zips
            try:
                with zipfile.ZipFile(io.BytesIO(data), "r") as z2:
                    for inner_name, inner_bytes in iter_ixbrl_html_bytes_from_zip(z2):
                        yield f"{name}::{inner_name}", inner_bytes
            except zipfile.BadZipFile:
                # Not actually a zip; ignore
                continue
        elif lname.endswith(".html") or lname.endswith(".xhtml") or lname.endswith(".htm"):
            yield name, data

def is_ixbrl_tag(tag_name: str) -> bool:
    """
    BeautifulSoup drops the prefix (ix:), and lowercases names.
    iXBRL facts are 'ix:nonFraction' and 'ix:nonNumeric'.
    We match their local names.
    """
    t = (tag_name or "").lower()
    return t.endswith("nonfraction") or t.endswith("nonnumeric")

def kind_for(tag_name: str) -> str:
    t = (tag_name or "").lower()
    if t.endswith("nonfraction"):
        return "numeric"
    if t.endswith("nonnumeric"):
        return "text"
    return "other"

# -----------------------------------------------------------------------------
# Main routine
# -----------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True, help="Monthly archive URL (e.g., https://download.companieshouse.gov.uk/Accounts_Monthly_Data-January2025.zip)")
    ap.add_argument("--limit", type=int, default=200, help="Max HTML files to scan (default 200)")
    ap.add_argument("--mincount", type=int, default=5, help="Only show concepts appearing at least this many times in the ranking (default 5)")
    args = ap.parse_args()

    print(f"[info] downloading: {args.url}")
    r = requests.get(args.url, timeout=600)
    r.raise_for_status()

    # State
    scanned = 0
    counts: Dict[str, int] = collections.Counter()
    kinds: Dict[str, str] = {}
    first_sample: Dict[str, str] = {}
    minval: Dict[str, float] = {}
    maxval: Dict[str, float] = {}

    with zipfile.ZipFile(io.BytesIO(r.content), "r") as z:
        # Optional: preview first ~40 names to show we see the archive structure
        preview = [m.filename for m in z.infolist() if not m.is_dir()][:40]
        if preview:
            print("[diag] monthly members (first 40):")
            for p in preview:
                print("  -", p)

        for name, html_bytes in iter_ixbrl_html_bytes_from_zip(z):
            try:
                soup = BeautifulSoup(html_bytes, "lxml")
            except Exception:
                continue

            # Find iXBRL fact tags
            for tag in soup.find_all(is_ixbrl_tag):
                qname = tag.get("name")  # e.g., frc-core:Debtors
                if not qname:
                    continue

                k = kind_for(tag.name)
                counts[qname] += 1
                kinds[qname] = k

                if qname not in first_sample:
                    # Record 1st sample text as-is
                    text = tag.get_text(strip=True)
                    if not text and tag.has_attr("content"):
                        text = str(tag.get("content") or "")
                    first_sample[qname] = text

                if k == "numeric":
                    # Try to track min / max numeric value
                    text = tag.get_text(strip=True)
                    if not text and tag.has_attr("content"):
                        text = str(tag.get("content") or "")
                    val = as_number_or_none(text)
                    if val is not None:
                        if qname not in minval or val < minval[qname]:
                            minval[qname] = val
                        if qname not in maxval or val > maxval[qname]:
                            maxval[qname] = val

            scanned += 1
            if scanned >= args.limit:
                break

    print(f"\n[info] parsed files: {scanned}")
    if scanned == 0:
        print("[hint] Did the URL point to a valid monthly archive? Try a different month or the 'archive/' URL form.")
        return

    if not counts:
        print("[diag] No iXBRL facts detected in the sample.")
        return

    # Build DataFrame for export
    rows = []
    for q, c in counts.items():
        rows.append({
            "qname": q,
            "kind": kinds.get(q, ""),
            "count": c,
            "sample_value": first_sample.get(q, ""),
            "min": minval.get(q, None),
            "max": maxval.get(q, None),
        })
    df = pd.DataFrame(rows).sort_values(["count", "qname"], ascending=[False, True])

    # Save artifacts
    df.to_csv("concept_counts.csv", index=False)

    # Pretty text with suggestions
    with open("concept_samples.txt", "w", encoding="utf-8") as f:
        f.write(f"Scanned files: {scanned}\n\n")
        f.write("Top concepts (>= {args.mincount} occurrences):\n")
        for _, row in df[df["count"] >= args.mincount].head(200).iterrows():
            q = row["qname"]
            k = row["kind"]
            c = int(row["count"])
            sample = row["sample_value"]
            lo = row["min"]
            hi = row["max"]
            lo_hi = ""
            if k == "numeric" and (pd.notna(lo) or pd.notna(hi)):
                lo_hi = f"  [min={lo if pd.notna(lo) else 'NA'}, max={hi if pd.notna(hi) else 'NA'}]"
            f.write(f"- {q:<45} {k:<7}  count={c:<6} sample={sample!r}{lo_hi}\n")

        f.write("\nSuggested FIN_MAP entries (edit to taste):\n")
        # Very light heuristic: map localname to a sane canonical field
        # E.g., 'frc-core:Debtors' -> 'debtors'; 'ifrs-full:ProfitLoss' -> 'profit_loss_for_period'
        suggestions = []
        for _, row in df[df["count"] >= args.mincount].iterrows():
            q = row["qname"]
            local = q.split(":", 1)[-1].lower()
            canon = None
            # A tiny synonym table (extend as you learn)
            if local in ("debtors", "tradeandotherreceivables"):
                canon = "debtors"
            elif local in ("cashbankonhand", "cashandequivalents", "cashandequivalent", "cashandequivalentsatbankandinhand"):
                canon = "cash_bank_in_hand"
            elif local in ("tangiblefixedassets", "propertyplantandequipment"):
                canon = "tangible_fixed_assets"
            elif local in ("netcurrentassetsliabilities", "netcurrentassetsliability"):
                canon = "net_current_assets_liabilities"
            elif local in ("totalassetslesscurrentliabilities",):
                canon = "total_assets_less_current_liabilities"
            elif local in ("profitloss", "profitlossforperiod", "profitlossaccountreserve"):
                canon = "profit_loss_for_period"
            elif local in ("turnover", "revenue", "grossoperatingrevenue"):
                canon = "turnover_gross_operating_revenue"
            elif local in ("administrativeexpenses",):
                canon = "administrative_expenses"
            elif local in ("costofsales", "costsales"):
                canon = "cost_sales"

            if canon:
                suggestions.append((q, canon))

        if not suggestions:
            f.write("(none matched heuristic; use concept_counts.csv to build your map)\n")
        else:
            for q, canon in suggestions:
                f.write(f"    FIN_MAP['{q}'] = '{canon}'\n")

    # Console summary
    print("\n[diag] Top concepts by frequency:")
    shown = 0
    for _, row in df[df["count"] >= args.mincount].head(30).iterrows():
        q = row["qname"]
        c = int(row["count"])
        k = row["kind"]
        sample = row["sample_value"]
        print(f"  {q:<45} {k:<7}  count={c:<6} sample={sample!r}")
        shown += 1
    if shown == 0:
        print("  (none above mincount; open concept_counts.csv artifact for full list)")

    print("\n[done] Wrote concept_counts.csv and concept_samples.txt")

if __name__ == "__main__":
    main()
