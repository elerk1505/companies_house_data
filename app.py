# app.py
from __future__ import annotations
import os, re, json, sys
from functools import lru_cache
from typing import Dict, Any, Optional, Tuple, List

import streamlit as st
import requests

# Try to import duckdb only here (to show helpful message if the runtime is wrong)
try:
    import duckdb  # noqa: F401
except Exception as e:
    st.error(
        "DuckDB isn’t available in this runtime.\n\n"
        f"Python: {sys.version}\n"
        f"sys.path[0]: {sys.path[0]}\n"
        f"Exception: {e}\n\n"
        "Fixes:\n"
        "• Ensure Streamlit Cloud Python version is 3.12 (Settings → Hardware → Python version).\n"
        "• Ensure repo has requirements.txt with `duckdb==0.10.3`.\n"
        "• (Optional) Add runtime.txt with `python-3.12.11`.\n"
        "• Menu → Clear cache, then Reboot."
    )
    st.stop()

# -------------------------------
# Config – change only if needed
# -------------------------------
GH_REPO_DEFAULT = "elerk1505/companies_house_data"   # your repo
FIN_NAME = "financials.parquet"
META_NAME = "metadata.parquet"

# Optional token (public repo works without it; token just improves API rate limits)
try:
    GITHUB_TOKEN = os.getenv("GITHUB_TOKEN") or st.secrets.get("GITHUB_TOKEN", "")
except Exception:
    GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")

# -------------------------------
# Streamlit setup
# -------------------------------
st.set_page_config("Companies House – Search", layout="wide")
st.title("Companies House – Search across all releases")
st.caption(
    "Queries **all** public releases in the GitHub repo and scans Parquet **in-place** via DuckDB httpfs. "
    "Always up-to-date — no manual uploads."
)

# -------------------------------
# GitHub helpers
# -------------------------------
def gh_session() -> requests.Session:
    s = requests.Session()
    if GITHUB_TOKEN:
        s.headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    s.headers["User-Agent"] = "CH-Finder/1.0"
    s.headers["Accept"] = "application/vnd.github+json"
    return s

@lru_cache(maxsize=1)
def list_releases(repo: str) -> list[dict]:
    """Return up to 100 most-recent releases."""
    url = f"https://api.github.com/repos/{repo}/releases?per_page=100"
    r = gh_session().get(url, timeout=60)
    r.raise_for_status()
    return r.json()

def pick_assets_from_release(rel: dict) -> tuple[Optional[str], Optional[str]]:
    fin_url = meta_url = None
    for a in rel.get("assets", []):
        nm = a.get("name") or ""
        if nm == FIN_NAME:
            fin_url = a.get("browser_download_url")
        elif nm == META_NAME:
            meta_url = a.get("browser_download_url")
    return fin_url, meta_url

def tag_parts(tag: str) -> Optional[tuple[int,str,str]]:
    # Example: data-2025-H1-financials
    m = re.match(r"^data-(\d{4})-(H[12])-(financials|metadata)$", tag or "")
    if not m:
        return None
    return int(m.group(1)), m.group(2), m.group(3)

@st.cache_data(show_spinner=True)
def build_manifest(repo: str) -> dict:
    """
    Build a manifest of all releases with direct download URLs for financials & metadata.
    Returns:
      { "all_pairs": {(year,half): {"fin":url|None, "meta":url|None}}, "latest": {...} }
    """
    rels = list_releases(repo)
    rows = []
    for r in rels:
        parts = tag_parts(r.get("tag_name", ""))
        if not parts:
            continue
        y, h, _ = parts
        fin, meta = pick_assets_from_release(r)
        if fin or meta:
            rows.append({"year": y, "half": h, "fin": fin, "meta": meta, "created_at": r.get("created_at")})

    # All pairs: keep every (y,h) we ever saw, last observed wins
    all_pairs: Dict[tuple, Dict[str, Any]] = {}
    for row in rows:
        key = (row["year"], row["half"])
        all_pairs[key] = {"year": row["year"], "half": row["half"], "fin": row["fin"], "meta": row["meta"]}

    # Latest: choose the variant with the most assets present; otherwise last seen
    latest: Dict[tuple, Dict[str, Any]] = {}
    for key, row in all_pairs.items():
        latest[key] = row

    return {"all_pairs": all_pairs, "latest": latest}

# -------------------------------
# DuckDB set-up
# -------------------------------
@st.cache_resource(show_spinner=False)
def connect_duckdb():
    import duckdb
    from duckdb import CatalogException
    con = duckdb.connect(database=":memory:")
    # httpfs for reading https parquet
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    # Prefer modern HTTP metadata cache; gracefully degrade on older builds
    try:
        con.execute("SET enable_http_metadata_cache = true;")
    except CatalogException:
        pass
    for setting in [
        "SET http_metadata_cache_size = '512MB';",
        "SET parquet_metadata_cache = true;",  # fallback for older versions
    ]:
        try:
            con.execute(setting)
        except CatalogException:
            pass
    return con

def reset_views(con):
    # Drop in reverse-dependency order; CASCADE removes dependents if needed
    for v in ["_fin_latest", "_fin", "_meta_uniq", "_meta", "_meta_raw"]:
        con.execute(f"DROP VIEW IF EXISTS {v} CASCADE")

def sql_section_case(col: str) -> str:
    """Map 2-digit SIC prefixes to Sections A..U."""
    return f"""
    CASE 
      WHEN try_strict_cast({col} AS INT) IS NULL THEN NULL
      ELSE 
        CASE WHEN try_strict_cast({col} AS INT)/100 BETWEEN 1 AND 3  THEN 'A'
             WHEN try_strict_cast({col} AS INT)/100 BETWEEN 5 AND 9  THEN 'B'
             WHEN try_strict_cast({col} AS INT)/100 BETWEEN 10 AND 33 THEN 'C'
             WHEN try_strict_cast({col} AS INT)/100 = 35               THEN 'D'
             WHEN try_strict_cast({col} AS INT)/100 BETWEEN 36 AND 39 THEN 'E'
             WHEN try_strict_cast({col} AS INT)/100 BETWEEN 41 AND 43 THEN 'F'
             WHEN try_strict_cast({col} AS INT)/100 BETWEEN 45 AND 47 THEN 'G'
             WHEN try_strict_cast({col} AS INT)/100 BETWEEN 49 AND 53 THEN 'H'
             WHEN try_strict_cast({col} AS INT)/100 BETWEEN 55 AND 56 THEN 'I'
             WHEN try_strict_cast({col} AS INT)/100 BETWEEN 58 AND 63 THEN 'J'
             WHEN try_strict_cast({col} AS INT)/100 BETWEEN 64 AND 66 THEN 'K'
             WHEN try_strict_cast({col} AS INT)/100 = 68               THEN 'L'
             WHEN try_strict_cast({col} AS INT)/100 BETWEEN 69 AND 75 THEN 'M'
             WHEN try_strict_cast({col} AS INT)/100 BETWEEN 77 AND 82 THEN 'N'
             WHEN try_strict_cast({col} AS INT)/100 = 84               THEN 'O'
             WHEN try_strict_cast({col} AS INT)/100 = 85               THEN 'P'
             WHEN try_strict_cast({col} AS INT)/100 BETWEEN 86 AND 88 THEN 'Q'
             WHEN try_strict_cast({col} AS INT)/100 BETWEEN 90 AND 93 THEN 'R'
             WHEN try_strict_cast({col} AS INT)/100 BETWEEN 94 AND 96 THEN 'S'
             WHEN try_strict_cast({col} AS INT)/100 BETWEEN 97 AND 98 THEN 'T'
             WHEN try_strict_cast({col} AS INT)/100 = 99               THEN 'U'
        END
    END
    """

def build_url_tables(manifest: dict, use_latest_only: bool) -> tuple[list[str], list[str]]:
    pairs = manifest["latest"] if use_latest_only else manifest["all_pairs"]
    fin_urls, meta_urls = [], []
    for (_key, row) in sorted(pairs.items()):
        if row.get("fin"):
            fin_urls.append(row["fin"])
        if row.get("meta"):
            meta_urls.append(row["meta"])
    return fin_urls, meta_urls

# Known numeric columns we want to support (filter inputs show even if some are absent)
NUMERIC_COLS = [
    "average_number_employees_during_period",
    "tangible_fixed_assets","debtors","cash_bank_in_hand","current_assets",
    "creditors_due_within_one_year","creditors_due_after_one_year","net_current_assets_liabilities",
    "total_assets_less_current_liabilities","net_assets_liabilities_including_pension_asset_liability",
    "called_up_share_capital","profit_loss_account_reserve","shareholder_funds",
    "turnover_gross_operating_revenue","other_operating_income","cost_sales","gross_profit_loss",
    "administrative_expenses","raw_materials_consumables","staff_costs",
    "depreciation_other_amounts_written_off_tangible_intangible_fixed_assets",
    "other_operating_charges_format2","operating_profit_loss",
    "profit_loss_on_ordinary_activities_before_tax","tax_on_profit_or_loss_on_ordinary_activities",
    "profit_loss_for_period",
]

# -------------------------------
# UI – source + options
# -------------------------------
repo = GH_REPO_DEFAULT
st.sidebar.subheader("Options")
latest_toggle = st.sidebar.toggle(
    "Show latest record per company (recommended)", value=True,
    help="If off, queries span ALL filings across all releases."
)
row_cap = st.sidebar.number_input("Max rows to show", 100, 200_000, 10_000, step=1000)

# Search inputs
st.sidebar.subheader("Search")
q_name = st.sidebar.text_input("Company name contains", "")
q_id   = st.sidebar.text_input("Company ID equals (digits only)", "")
q_sic  = st.sidebar.text_input("SIC codes include (comma/space separated)", "")

# Dropdowns
st.sidebar.subheader("Dropdown filters")
wanted_section = st.sidebar.selectbox("SIC section (A–U)", ["(any)"] + list("ABCDEFGHIJKLMNOPQRSTU"))
cat_choice = st.sidebar.selectbox("Company category", ["(any)"])  # populated after reading metadata
stat_choice = st.sidebar.selectbox("Company status", ["(any)"])    # populated after reading metadata

# Numeric filters
st.sidebar.subheader("Numeric filters")
chosen_numeric = st.sidebar.multiselect("Pick numeric columns", NUMERIC_COLS, default=[])

# Date range
st.sidebar.subheader("Incorporation date")
date_from = st.sidebar.text_input("From (YYYY-MM-DD)", "")
date_to   = st.sidebar.text_input("To (YYYY-MM-DD)", "")

# -------------------------------
# Build and run query
# -------------------------------
con = connect_duckdb()
manifest = build_manifest(repo)

fin_urls, meta_urls = build_url_tables(manifest, use_latest_only=False)  # always scan all; we slim later
if not fin_urls or not meta_urls:
    st.warning("No financials or metadata assets discovered in releases.")
    st.stop()

# Reset temp views cleanly on rerun
reset_views(con)

# 1) RAW SCANS
con.execute("CREATE TEMP VIEW _fin_raw  AS SELECT * FROM read_parquet($urls)", {"urls": fin_urls})
con.execute("CREATE TEMP VIEW _meta_raw AS SELECT * FROM read_parquet($urls)", {"urls": meta_urls})

# Introspect available columns to build robust expressions
fin_cols  = {r[1] for r in con.execute("PRAGMA table_info('_fin_raw')").fetchall()}
meta_cols = {r[1] for r in con.execute("PRAGMA table_info('_meta_raw')").fetchall()}

def present(cols: set[str], *cands: str) -> list[str]:
    """Return [first_existing] or [] if none present."""
    for c in cands:
        if c in cols:
            return [c]
    return []

def coalesce_sql(cols: set[str], alias: str, *cands: str) -> str:
    picks = [c for c in cands if c in cols]
    if not picks:
        return f"NULL AS {alias}"
    if len(picks) == 1:
        return f"{picks[0]} AS {alias}"
    return f"COALESCE({', '.join(picks)}) AS {alias}"

# 2) FINANCIALS: normalized key + keep all columns (exclude original id columns to avoid duplicates)
fin_key_exprs = present(fin_cols, "companies_house_registered_number") + present(fin_cols, "company_id")
fin_key_coalesce = "COALESCE(" + ",".join(
    [f"NULLIF(LTRIM(REGEXP_REPLACE({c}::TEXT,'[^0-9]','','g'),'0'),'')" for c in fin_key_exprs]
) + ")" if fin_key_exprs else "NULL"

fin_view = f"""
SELECT
  {fin_key_coalesce} AS _key,
  * EXCLUDE ({', '.join([c for c in ['companies_house_registered_number','company_id'] if c in fin_cols])})
FROM _fin_raw
"""
con.execute("CREATE TEMP VIEW _fin AS " + fin_view)

# 3) METADATA: normalized key + preferred fields
meta_key_exprs = (
    present(meta_cols, "companies_house_registered_number")
    + present(meta_cols, "CompanyNumber")
    + present(meta_cols, "company_number")
)
meta_key_coalesce = "COALESCE(" + ",".join(
    [f"NULLIF(LTRIM(REGEXP_REPLACE({c}::TEXT,'[^0-9]','','g'),'0'),'')" for c in meta_key_exprs]
) + ")" if meta_key_exprs else "NULL"

meta_entity = coalesce_sql(meta_cols, "entity_current_legal_name", "entity_current_legal_name", "company_name")
meta_type   = coalesce_sql(meta_cols, "company_type", "company_type", "CompanyCategory", "type")
meta_stat   = coalesce_sql(meta_cols, "company_status", "company_status", "CompanyStatus", "status")
meta_incorp = coalesce_sql(meta_cols, "incorporation_date", "incorporation_date", "IncorporationDate", "date_of_creation")
meta_sic    = ("sic_codes AS sic_codes") if "sic_codes" in meta_cols else "NULL AS sic_codes"

meta_base = f"""
SELECT
  {meta_key_coalesce} AS _key,
  {meta_entity},
  {meta_type},
  {meta_stat},
  {meta_incorp},
  {meta_sic}
FROM _meta_raw
WHERE _key IS NOT NULL
"""
con.execute("CREATE TEMP VIEW _meta AS " + meta_base)

# 4) One metadata row per company (no reliable updated_at → arbitrary stable pick)
con.execute("""
CREATE TEMP VIEW _meta_uniq AS
SELECT * FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY _key ORDER BY _key) AS rn
  FROM _meta
) t WHERE rn = 1
""")

# Populate category/status dropdown values
cats = [r[0] for r in con.execute("SELECT DISTINCT company_type FROM _meta_uniq WHERE company_type IS NOT NULL ORDER BY 1").fetchall()]
stats = [r[0] for r in con.execute("SELECT DISTINCT company_status FROM _meta_uniq WHERE company_status IS NOT NULL ORDER BY 1").fetchall()]
if cats:
    cat_choice = st.sidebar.selectbox("Company category", ["(any)"] + cats, index=0)
if stats:
    stat_choice = st.sidebar.selectbox("Company status", ["(any)"] + stats, index=0)

# 5) If "latest per company" → pick latest financial row by any available date casted to TIMESTAMP
date_coalesce = []
for c in ["balance_sheet_date", "period_end", "date"]:
    if c in fin_cols:
        date_coalesce.append(f"try_cast({c} AS TIMESTAMP)")
date_coalesce.append("TIMESTAMP '1900-01-01 00:00:00'")
date_order_sql = "COALESCE(" + ", ".join(date_coalesce) + ")"

con.execute(f"""
CREATE TEMP VIEW _fin_latest AS
SELECT * FROM (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY _key
           ORDER BY {date_order_sql} DESC
         ) AS rn
  FROM _fin
) t WHERE rn = 1
""")

# -------------------------------
# WHERE clause from sidebar
# -------------------------------
where = []
params: Dict[str, Any] = {}

if q_name.strip():
    where.append("LOWER(_meta_uniq.entity_current_legal_name) LIKE LOWER($name_pattern)")
    params["name_pattern"] = f"%{q_name.strip()}%"

if q_id.strip():
    where.append("_fin._key = REGEXP_REPLACE($cid,'[^0-9]','','g')")
    params["cid"] = q_id.strip()

if q_sic.strip():
    parts = [p for p in re.split(r"[,\s;]+", q_sic.strip()) if p]
    ors = [f"list_contains(_meta_uniq.sic_codes, '{code}')" for code in parts]
    if ors:
        where.append("(" + " OR ".join(ors) + ")")

if cat_choice and cat_choice != "(any)":
    where.append("_meta_uniq.company_type = $cat")
    params["cat"] = cat_choice

if stat_choice and stat_choice != "(any)":
    where.append("_meta_uniq.company_status = $st")
    params["st"] = stat_choice

def valid_date(s: str) -> bool:
    return bool(re.match(r"^\d{4}-\d{2}-\d{2}$", s))

if date_from and valid_date(date_from):
    where.append("try_strict_cast(_meta_uniq.incorporation_date AS DATE) >= try_strict_cast($dfrom AS DATE)")
    params["dfrom"] = date_from

if date_to and valid_date(date_to):
    where.append("try_strict_cast(_meta_uniq.incorporation_date AS DATE) <= try_strict_cast($dto AS DATE)")
    params["dto"] = date_to

# Section filter via UNNEST(sic_codes)
if wanted_section and wanted_section != "(any)":
    sec_case = sql_section_case("c")
    where.append(f"""
EXISTS (
  SELECT 1
  FROM UNNEST(_meta_uniq.sic_codes) AS c
  WHERE {sec_case} = $sec
)
""")
    params["sec"] = wanted_section

# Numeric filters (apply only if col exists in _fin_raw)
for col in chosen_numeric:
    mn = st.sidebar.text_input(f"{col} min", "")
    mx = st.sidebar.text_input(f"{col} max", "")
    if col in fin_cols:
        if mn.strip():
            where.append(f"try_strict_cast(_fin.{col} AS DOUBLE) >= try_strict_cast($mn_{col} AS DOUBLE)")
            params[f"mn_{col}"] = mn.strip()
        if mx.strip():
            where.append(f"try_strict_cast(_fin.{col} AS DOUBLE) <= try_strict_cast($mx_{col} AS DOUBLE)")
            params[f"mx_{col}"] = mx.strip()

where_sql = "WHERE " + " AND ".join(where) if where else ""

# Choose which fin table to join
fin_table = "_fin_latest" if latest_toggle else "_fin"

# Final select: include all metadata fields (qualified) and all financial columns,
# but exclude duplicate meta-like names from the _fin wildcard to avoid clashes.
dup_exclude = [c for c in [
    "_key", "entity_current_legal_name", "company_type", "company_status",
    "incorporation_date", "sic_codes"
] if c in fin_cols]

select_cols = f"""
  _fin._key AS company_id,
  _meta_uniq.entity_current_legal_name AS entity_current_legal_name,
  _meta_uniq.company_type AS company_type,
  _meta_uniq.company_status AS company_status,
  _meta_uniq.incorporation_date AS incorporation_date,
  _meta_uniq.sic_codes AS sic_codes,
  _fin.* EXCLUDE ({', '.join(dup_exclude) if dup_exclude else ''})
"""

sql = f"""
SELECT {select_cols}
FROM {fin_table} AS _fin
JOIN _meta_uniq ON _fin._key = _meta_uniq._key
{where_sql}
LIMIT {int(row_cap)}
"""

with st.spinner("Querying releases…"):
    df = con.execute(sql, params).fetch_df()

st.success(
    f"Results: {len(df):,} rows • Source: "
    f"{'latest per company' if latest_toggle else 'all filings'} across all releases"
)
st.dataframe(df, use_container_width=True)

st.download_button(
    "Download CSV (shown rows)",
    df.to_csv(index=False).encode("utf-8"),
    file_name="companies_filtered.csv",
    mime="text/csv",
)
