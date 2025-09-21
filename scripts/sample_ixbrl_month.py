#!/usr/bin/env python3
# scripts/sample_ixbrl_month.py
from __future__ import annotations

import sys
import io
import zipfile
import requests
import collections
from typing import Dict, Tuple, Iterable, Optional

# pip install ixbrlparse (already done in your workflows)
try:
    from ixbrlparse import IXBRL
except Exception as e:
    print("[error] ixbrlparse is required: pip install ixbrlparse", file=sys.stderr)
    raise


# ---------- Helpers ----------

def open_ixbrl_bytes(html_bytes: bytes) -> IXBRL:
    """
    Create an IXBRL document from raw HTML bytes.
    """
    return IXBRL(html_bytes)


def _looks_numeric(s: str) -> bool:
    """
    Heuristic: says whether text looks like a numeric value (allowing commas, signs, decimals).
    """
    if s is None:
        return False
    s = str(s).strip()
    if not s or s.lower() in ("nil", "nan", "inf", "-inf"):
        return False
    # common placeholders that aren't numbers
    if s in ("—", "–", "-", "—", "— —"):
        return False
    # Try a forgiving parse
    try:
        float(s.replace(",", ""))
        return True
    except Exception:
        return False


def is_numeric_fact(fact) -> bool:
    """
    Decide if an ixbrlparse fact is numeric enough for sampling.
    We avoid depending on internal library attributes and just check value-like-ness.
    """
    # ixbrlparse exposes .value; sometimes also .unit / .decimals etc
    v = getattr(fact, "value", None)
    if v is None:
        return False
    return _looks_numeric(v)


def concept_key(fact) -> str:
    """
    Build a stable concept key (e.g., with QName if available).
    """
    # Try several common attributes seen in ixbrl libs
    for attr in ("qname", "name", "concept", "concept_name"):
        val = getattr(fact, attr, None)
        if val:
            return str(val)
    # Fallback to string of the fact's concept-like repr
    return str(getattr(fact, "concept", "UNKNOWN_CONCEPT"))


def iter_ixbrl_members(z: zipfile.ZipFile) -> Iterable[bytes]:
    """
    Yield raw HTML bytes for every iXBRL file found in the monthly archive.
    Handles both top-level .html/.htm/.xhtml and one level of nested .zip (daily bundles).
    """
    def is_ixbrl_name(name: str) -> bool:
        n = name.lower()
        return n.endswith(".html") or n.endswith(".htm") or n.endswith(".xhtml")

    for info in z.infolist():
        name = info.filename
        lname = name.lower()
        try:
            if lname.endswith(".zip"):
                # Nested daily zip: open & scan for HTML
                with z.open(info) as f:
                    data = f.read()
                with zipfile.ZipFile(io.BytesIO(data), "r") as inner:
                    for inner_info in inner.infolist():
                        if is_ixbrl_name(inner_info.filename):
                            try:
                                with inner.open(inner_info) as g:
                                    yield g.read()
                            except Exception:
                                # Skip problematic file and continue
                                continue
            elif is_ixbrl_name(lname):
                with z.open(info) as f:
                    yield f.read()
        except Exception:
            # One bad entry shouldn't kill the run
            continue


# ---------- Main ----------

def main():
    if len(sys.argv) < 2:
        print("usage: python scripts/sample_ixbrl_month.py <MONTHLY_ARCHIVE_URL> [LIMIT_FILES]", file=sys.stderr)
        sys.exit(2)

    MONTH_URL = sys.argv[1].strip()
    try:
        LIMIT_FILES = int(sys.argv[2]) if len(sys.argv) >= 3 else 200
    except Exception:
        LIMIT_FILES = 200

    print(f"[info] downloading: {MONTH_URL}")
    r = requests.get(MONTH_URL, timeout=600)
    r.raise_for_status()

    seen = 0
    counts: collections.Counter[str] = collections.Counter()
    sample_vals: Dict[str, Tuple[int, str]] = {}  # concept -> (count_of_samples_kept, example_value)

    with zipfile.ZipFile(io.BytesIO(r.content), "r") as z:
        for html in iter_ixbrl_members(z):
            try:
                doc = open_ixbrl_bytes(html)
            except Exception:
                # Skip broken/parsing failures
                continue

            # Walk all facts in the document
            facts = getattr(doc, "facts", [])
            for fact in facts:
                if not is_numeric_fact(fact):
                    continue
                key = concept_key(fact)
                counts[key] += 1

                # Keep up to 3 example values per concept
                val_text = str(getattr(fact, "value", ""))
                if key not in sample_vals:
                    sample_vals[key] = (1, val_text)
                else:
                    c, _ = sample_vals[key]
                    if c < 3:
                        sample_vals[key] = (c + 1, val_text)

            seen += 1
            if seen >= LIMIT_FILES:
                break

    print(f"\nParsed {seen} iXBRL files (LIMIT_FILES={LIMIT_FILES}).")
    print(f"Unique numeric concepts found: {len(counts)}\n")

    # Show top concepts
    TOP_N = 50
    print(f"Top {min(TOP_N, len(counts))} numeric concepts by frequency:")
    for i, (k, v) in enumerate(counts.most_common(TOP_N), 1):
        sample_value = sample_vals.get(k, (0, ""))[1]
        print(f"{i:2d}. {k:60s}  {v:7d}   sample={sample_value}")

    # If none found, give a hint
    if not counts:
        print("\n[hint] No numeric concepts found in the sample. "
              "Try a different monthly archive (older months often contain richer filings), "
              "or increase LIMIT_FILES.")

if __name__ == "__main__":
    main()
