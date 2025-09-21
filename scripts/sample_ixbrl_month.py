# scripts/sample_ixbrl_month.py
# Robust iXBRL sampler for Companies House monthly (or archive) ZIPs.
# - Works with both .html and .xml members
# - Finds ix:nonFraction / ix:nonNumeric by local-name() (prefix-agnostic)
# - Writes concept_counts.csv and concept_samples.txt to GITHUB_WORKSPACE
from __future__ import annotations

import io
import os
import sys
import zipfile
import collections
from typing import Dict, Tuple

import requests
from lxml import etree

MONTH_URL = sys.argv[1] if len(sys.argv) > 1 else ""
LIMIT_FILES = int(sys.argv[2]) if len(sys.argv) > 2 else 300

if not MONTH_URL:
    print("[error] Provide the monthly archive URL as the first argument.")
    sys.exit(2)

# Where to write outputs (so GitHub Actions can upload them easily)
OUT_DIR = os.environ.get("GITHUB_WORKSPACE", os.getcwd())
OUT_COUNTS = os.path.join(OUT_DIR, "concept_counts.csv")
OUT_SAMPLES = os.path.join(OUT_DIR, "concept_samples.txt")

def fetch_zip(url: str) -> bytes:
    print(f"[info] downloading: {url}")
    r = requests.get(url, timeout=600)
    r.raise_for_status()
    return r.content

def iter_members(z: zipfile.ZipFile):
    # Yield inner file names we actually want to try (.html or .xml)
    for info in z.infolist():
        name = info.filename
        # Skip directories
        if name.endswith("/") or name.endswith("\\"):
            continue
        lower = name.lower()
        if lower.endswith(".html") or lower.endswith(".xml"):
            yield name

def parse_ixbrl_bytes(doc_bytes: bytes) -> etree._Element | None:
    """
    Parse content as XML. Use a forgiving parser; accept HTML-ish too.
    1) Try as XML
    2) If that fails, try HTML5 fallback through lxml's parser with recover=True
    """
    # XML first
    try:
        parser = etree.XMLParser(recover=True, huge_tree=True)
        return etree.fromstring(doc_bytes, parser=parser)
    except Exception:
        pass
    # HTML-ish fallback
    try:
        parser = etree.HTMLParser(recover=True)
        return etree.fromstring(doc_bytes, parser=parser)
    except Exception:
        return None

def text_content(el: etree._Element) -> str:
    return "".join(el.itertext()).strip()

def is_numeric_value(s: str) -> bool:
    s = s.replace(",", "").replace(" ", "")
    if not s:
        return False
    try:
        float(s)
        return True
    except Exception:
        return False

def main():
    raw = fetch_zip(MONTH_URL)
    z = zipfile.ZipFile(io.BytesIO(raw), "r")
    members = list(iter_members(z))
    print("[diag] monthly members (first 40):")
    for m in members[:40]:
        print(" -", m)

    counts: Dict[str, int] = collections.Counter()
    samples: Dict[str, Tuple[str, str]] = {}  # concept -> (kind, sample)
    parsed = 0

    # XPath to find ixbrl facts regardless of prefix
    # Matches both ix:nonFraction and ix:nonNumeric (prefix-agnostic)
    XPATH_FACTS = "//*[local-name()='nonFraction' or local-name()='nonNumeric']"

    for name in members[:LIMIT_FILES]:
        try:
            with z.open(name) as f:
                b = f.read()
        except Exception:
            continue

        root = parse_ixbrl_bytes(b)
        if root is None:
            continue

        # Gather facts
        try:
            facts = root.xpath(XPATH_FACTS)
        except Exception:
            # Some HTML documents might confuse xpath; skip
            continue

        if not facts:
            continue

        parsed += 1
        for fact in facts:
            # Concept name can be @name or namespaced; use local-name() logic
            concept = None
            # Try normal @name first
            concept = fact.get("name")
            if concept is None:
                # Try any attribute whose local-name is 'name'
                for k, v in fact.attrib.items():
                    if k.split("}")[-1].lower() == "name":
                        concept = v
                        break
            if not concept:
                continue

            value = text_content(fact)
            kind = "numeric" if is_numeric_value(value) else "text"

            counts[concept] += 1
            if concept not in samples:
                # keep a small sample
                samples[concept] = (kind, value[:120])

    print(f"[info] parsed files: {parsed}")
    if parsed == 0:
        print("[hint] Did the URL point to a valid monthly archive? Try a different month,"
              " an 'archive' URL form, or increase LIMIT_FILES.")
    # Print top concepts for quick view
    top = counts.most_common(30)
    print("\n[diag] Top concepts by frequency:")
    for concept, cnt in top:
        kind, sample = samples.get(concept, ("?", ""))
        print(f"{concept:40s}  {kind:7s}  count={cnt:<5d}  sample='{sample}'")

    # Write artifacts
    try:
        with open(OUT_COUNTS, "w", encoding="utf-8") as f:
            f.write("concept,kind,count,sample\n")
            for concept, cnt in counts.most_common():
                kind, sample = samples.get(concept, ("", ""))
                sample = sample.replace("\n", " ").replace('"', '""')
                f.write(f"\"{concept}\",{kind},{cnt},\"{sample}\"\n")

        with open(OUT_SAMPLES, "w", encoding="utf-8") as f:
            for concept, (kind, sample) in samples.items():
                f.write(f"{concept}\t{kind}\t{sample}\n")
        print(f"[done] Wrote {OUT_COUNTS} and {OUT_SAMPLES}")
    except Exception as e:
        print("[warn] could not write artifacts:", e)

if __name__ == "__main__":
    main()
