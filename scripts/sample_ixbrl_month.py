#!/usr/bin/env python3
import io, os, sys, zipfile, collections, re, tempfile, requests
from typing import Dict, Tuple

MONTH_URL = sys.argv[1] if len(sys.argv) > 1 else "https://download.companieshouse.gov.uk/Accounts_Monthly_Data-January2025.zip"
LIMIT_FILES = int(os.environ.get("LIMIT_FILES", "500"))  # parse up to N inner ixbrl files
SHOW_TOP = 40

try:
    import ixbrlparse  # pip install ixbrlparse
except Exception as e:
    print("[error] pip install ixbrlparse first: pip install ixbrlparse")
    raise

def open_ixbrl_bytes(b: bytes):
    return ixbrlparse.parse(io.BytesIO(b))

def iter_ixbrl_members(z: zipfile.ZipFile):
    for info in z.infolist():
        name = info.filename
        low = name.lower()
        if low.endswith(".html") or low.endswith(".htm") or low.endswith(".xhtml"):
            # likely ixbrl leaf
            yield info
        elif low.endswith(".zip"):
            # nested daily zip
            with z.open(info) as f:
                data = f.read()
            with zipfile.ZipFile(io.BytesIO(data), "r") as z2:
                for info2 in z2.infolist():
                    low2 = info2.filename.lower()
                    if low2.endswith(".html") or low2.endswith(".htm") or low2.endswith(".xhtml"):
                        yield (info, info2)  # (outer, inner)

def concept_key(fact) -> str:
    # Prefer qname including prefix/namespace, fall back to localname
    qn = getattr(fact, "qname", None) or ""
    if qn:
        return str(qn)
    return getattr(fact, "name", "") or ""

def is_numeric_fact(fact) -> bool:
    # ixbrlparse normalises numeric facts to having a numeric value and a unit
    try:
        _ = fact.decimals
        _ = fact.unit
        _ = float(str(fact.value).replace(",", ""))
        return True
    except Exception:
        return False

def main():
    print(f"[info] downloading: {MONTH_URL}")
    r = requests.get(MONTH_URL, timeout=600)
    r.raise_for_status()

    seen = 0
    counts = collections.Counter()
    sample_vals: Dict[str, Tuple[int, str]] = {}

    with zipfile.ZipFile(io.BytesIO(r.content), "r") as z:
        for member in iter_ixbrl_members(z):
            if isinstance(member, tuple):
                outer, inner = member
                with z.open(outer) as f:
                    b = f.read()
                with zipfile.ZipFile(io.BytesIO(b), "r") as z2:
                    with z2.open(inner) as g:
                        html = g.read()
            else:
                with z.open(member) as f:
                    html = f.read()

            try:
                doc = open_ixbrl_bytes(html)
            except Exception:
                continue

            # Walk all facts
            for fact in doc.facts:
                if not is_numeric_fact(fact):
                    continue
                key = concept_key(fact)
                counts[key] += 1
                if key not in sample_vals:
                    sample_vals[key] = (1, str(fact.value))
                else:
                    c, _ = sample_vals[key]
                    if c < 3:  # keep a couple examples
                        sample_vals[key] = (c + 1, str(fact.value))

            seen += 1
            if seen >= LIMIT_FILES:
                break

    print(f"\nParsed {seen} iXBRL files (LIMIT_FILES={LIMIT_FILES}).")
    print(f"Unique numeric concepts found: {len(counts)}")
    print(f"\nTop {min(SHOW_TOP, len(counts))} numeric concepts by frequency:\n")
    for name, cnt in counts.most_common(SHOW_TOP):
        example = sample_vals.get(name, (0, ""))[1]
        print(f"{cnt:7d}  {name}   e.g. {example}")

if __name__ == "__main__":
    main()
