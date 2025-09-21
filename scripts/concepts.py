# scripts/concepts.py
from typing import Any, Dict, Iterable, Optional
import re
from bs4 import BeautifulSoup

Number = Optional[float]

def _to_number(val: str) -> Number:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    # handle commas, spaces, currency, parentheses for negatives
    neg = s.startswith("(") and s.endswith(")")
    s = re.sub(r"[^0-9.\-]", "", s)
    if not s:
        return None
    try:
        x = float(s)
        return -x if neg else x
    except ValueError:
        return None

def first_non_null(doc: "IxbrlDoc", candidates: Iterable[str], as_number: bool):
    """Return the first candidate concept value present in the doc."""
    for qn in candidates:
        v = doc.get_first(qn)  # your existing ixbrl doc accessor
        if v is None or (isinstance(v, str) and not v.strip()):
            continue
        return _to_number(v) if as_number else v
    return None
