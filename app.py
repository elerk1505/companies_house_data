# app.py
from __future__ import annotations
import os, re, time, json, math
import sys
from functools import lru_cache
from typing import List, Tuple, Dict, Any, Optional

import streamlit as st
import duckdb
import requests


try:
    import duckdb  # noqa: F401
except Exception as e:
    st.error(
        "DuckDB isn’t available in this runtime.\n\n"
        f"Python: {sys.version}\n"
        f"sys.path[0]: {sys.path[0]}\n"
        f"Exception: {e}\n\n"
        "Fixes:\n"
        "• Ensure runtime.txt at repo root is `python-3.12.4`.\n"
        "• Ensure requirements.txt contains `duckdb==0.10.3`.\n"
        "• Reboot & clear cache from Streamlit Cloud."
    )
    st.stop()

# -------------------------------
# Config – change only if needed
# -------------------------------
GH_REPO_DEFAULT = "elerk1505/companies_house_data"   # your repo
FIN_NAME = "financials.parquet"
META_NAME = "metadata.parquet"

# If you set a token (optional, improves API rate limits)
# export GITHUB_TOKEN=ghp_xxx   (or set in Streamlit secrets)
# If public repo, token is optional
try:
    GITHUB_TOKEN = os.getenv("GITHUB_TOKEN") or st.secrets.get("GITHUB_TOKEN", "")
except Exception:
    GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")

# -------------------------------
# Streamlit setup
# -------------------------------
st.set_page_config("Companies House – Search", layout="wide")
st.title("Companies House – Search across all releases")

st.caption("Indexing public GitHub releases and querying Parquet **in-place** via DuckDB. "
           "No local files required; always up-to-date.")

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
    """Return all releases (max 100 most recent is usually plenty)."""
    url = f"https://api.github.com/repos/{repo}/releases?per_page=100"
    s = gh_session()
    r = s.get(url, timeout=60)
    r.raise_for_status()
    return r.json()

def pick_assets_from_release(rel: dict) -> tuple[Optional[str], Optional[str]]:
    fin_url = meta_url = None
    for a in rel.get("assets", []):
        nm = a.get("name", "")
        if nm == FIN_NAME:
            fin_url = a.get("browser_download_url")
        if nm == META_NAME:
            meta_url = a.get("browser_download_url")
    return fin_url, meta_url

def tag_parts(tag: str) -> Optional[tuple[int,str,str]]:
    # Ex: data-2025-H1-financials
    m = re.match(r"data-(\d{4})-(H[12])-(financials|metadata)", tag)
    if not m: return None
    return int(m.group(1)), m.group(2), m.group(3)

@st.cache_data(show_spinner=True)
def build_manifest(repo: str) -> dict:
    """
    Build a manifest of all releases with direct download URLs for
    financials & metadata assets.
    """
    rels = list_releases(repo)
    rows = []
    for r in rels:
        tag = r.get("tag_name", "")
        parts = tag_parts(tag)
        if not parts: 
            continue
        year, half, kind = parts
        fin, meta = pick_assets_from_release(r)
        rows.append({"tag": tag, "year": year, "half": half, "fin": fin, "meta": meta, "created_at": r.get("created_at")})
    # Keep only releases that contain at least one asset we care about
    rows = [x for x in rows if x["fin"] or x["meta"]]
    # Group by (year, half) -> pick most recent per pair (in case there are multiple publishes)
    latest: Dict[tuple,int] = {}
    for i, row in enumerate(rows):
        key = (row["year"], row["half"])
        if key not in latest:
            latest[key] = i
        else:
            # prefer the one with both assets; else prefer latest in list
            j = latest[key]
            score_i = int(bool(rows[i]["fin"])) + int(bool(rows[i]["meta"]))
            score_j = int(bool(rows[j]["fin"])) + int(bool(rows[j]["meta"]))
            if score_i > score_j:
                latest[key] = i
    # Create final manifest
    manifest = {}
    for (y,h), idx in latest.items():
        manifest[(y,h)] = {"year":y, "half":h, "fin":rows[idx]["fin"], "meta":rows[idx]["meta"]}
    # Also keep all variants (not only latest) so "All filings" can span everything
    # for All filings we still want every pair we ever saw:
    all_pairs = {}
    for row in rows:
        key = (row["year"], row["half"])
        all_pairs.setdefault(key, {"year":row["year"], "half":row["half"], "fin":row["fin"], "meta":row["meta"]})
    return {"latest":manifest, "all_pairs":all_pairs}

# -------------------------------
# DuckDB set-up
# -------------------------------
@st.cache_resource(show_spinner=False)
def connect_duckdb():
    import duckdb
    from duckdb import CatalogException
    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    # Prefer modern HTTP cache, fall back silently on older builds
    try:
        con.execute("SET enable_http_metadata_cache=true;")
    except CatalogException:
        pass
    try:
        con.execute("SET http_metadata_cache_size='512MB';")
    except CatalogException:
        try:
            con.execute("SET parquet_metadata_cache=true;")
        except CatalogException:
            pass
    return con

def sql_section_case(col: str) -> str:
    """
    Map integer SIC 2-digit prefix to Section A..U
    Input 'col' is a string/int of a single SIC code.
    """
    # works if col is TEXT or INTEGER; we CAST to INT safely with try_strict
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

# Build a virtual table list of all relevant Parquet asset URLs (for both "latest" and "all")
def build_url_tables(con: duckdb.DuckDBPyConnection, manifest: dict, use_latest_only: bool) -> tuple[str, str]:
    """
    Returns (fin_sql, meta_sql): SELECT ... FROM parquet_scan([urls...])
    """
    pairs = manifest["latest"] if use_latest_only else manifest["all_pairs"]
    fin_urls, meta_urls = [], []
    for (y,h), row in sorted(pairs.items()):
        if row["fin"]:
            fin_urls.append(row["fin"])
        if row["meta"]:
            meta_urls.append(row["meta"])
    if not fin_urls or not meta_urls:
        raise RuntimeError("No financials or metadata assets discovered in releases.")
    fin_sql  = f"SELECT * FROM read_parquet({json.dumps(fin_urls)})"
    meta_sql = f"SELECT * FROM read_parquet({json.dumps(meta_urls)})"
    return fin_sql, meta_sql

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
latest_toggle = st.sidebar.toggle("Show latest record per company (recommended)", value=True,
                                  help="If off, queries span ALL filings across all releases.")

row_cap = st.sidebar.number_input("Max rows to show", 100, 200_000, 10_000, step=1000)

# Search inputs
st.sidebar.subheader("Search")
q_name = st.sidebar.text_input("Company name contains", "")
q_id   = st.sidebar.text_input("Company ID equals (digits only)", "")
q_sic  = st.sidebar.text_input("SIC codes include (comma/space separated)", "")

# Dropdowns – we’ll populate values lazily from metadata
st.sidebar.subheader("Dropdown filters")
wanted_section = st.sidebar.selectbox("SIC section (A–U)", ["(any)"] + list("ABCDEFGHIJKLMNOPQRSTU"))
cat_choice = st.sidebar.selectbox("Company category", ["(any)"])  # will update after we peek metadata
stat_choice = st.sidebar.selectbox("Company status", ["(any)"])    # will update after we peek metadata

# Ranged filters
st.sidebar.subheader("Numeric filters")
chosen_numeric = st.sidebar.multiselect("Pick numeric columns", NUMERIC_COLS, default=[])

st.sidebar.subheader("Incorporation date")
date_from = st.sidebar.text_input("From (YYYY-MM-DD)", "")
date_to   = st.sidebar.text_input("To (YYYY-MM-DD)", "")

# -------------------------------
# Query building
# -------------------------------
con = connect_duckdb()
manifest = build_manifest(repo)
fin_sql, meta_sql = build_url_tables(con, manifest, use_latest_only=latest_toggle)

# Create base views for metadata to discover dropdown values quickly
# We normalise the key in SQL (digits only, no leading zeros)
def norm_col(col: str) -> str:
    # remove non-digits then ltrim zeros
    return f"REGEXP_REPLACE({col}::TEXT, '[^0-9]', '', 'g')::TEXT"

# Metadata likely has one of these columns:
meta_key = f"""
COALESCE(
    NULLIF(LTRIM(REGEXP_REPLACE(companies_house_registered_number::TEXT, '[^0-9]','','g'),'0'),''),
    NULLIF(LTRIM(REGEXP_REPLACE(CompanyNumber::TEXT,'[^0-9]','','g'),'0'),''),
    NULLIF(LTRIM(REGEXP_REPLACE(company_number::TEXT,'[^0-9]','','g'),'0'),'')
) AS _key
"""

meta_cols_sql = f"""
SELECT
  {meta_key},
  entity_current_legal_name,
  company_name,
  company_type,
  CompanyCategory,
  CompanyStatus,
  type,
  status,
  incorporation_date,
  IncorporationDate,
  date_of_creation,
  sic_codes
FROM ({meta_sql})
"""
con.execute("CREATE OR REPLACE TEMP VIEW _meta_base AS " + meta_cols_sql)

# Get dropdown choices
cats = con.execute("""
SELECT DISTINCT COALESCE(company_type, CompanyCategory, type) AS cat
FROM _meta_base WHERE cat IS NOT NULL ORDER BY cat
""").fetchall()
stats = con.execute("""
SELECT DISTINCT COALESCE(company_status, CompanyStatus, status) AS st
FROM _meta_base WHERE st IS NOT NULL ORDER BY st
""").fetchall()

# Update dropdowns with discovered values
if cats:
    st.sidebar.selectbox.__wrapped__  # satisfy linters
    cat_choice = st.sidebar.selectbox("Company category", ["(any)"] + [c[0] for c in cats], index=0)
if stats:
    stat_choice = st.sidebar.selectbox("Company status", ["(any)"] + [s[0] for s in stats], index=0)

# Build the final SQL
# 1) Normalise financials key; keep all financial columns
fin_key = f"""
COALESCE(
    NULLIF(LTRIM(REGEXP_REPLACE(companies_house_registered_number::TEXT,'[^0-9]','','g'),'0'),''),
    NULLIF(LTRIM(REGEXP_REPLACE(company_id::TEXT,'[^0-9]','','g'),'0'),'')
) AS _key
"""

fin_cols_sql = f"SELECT {fin_key}, * EXCLUDE (companies_house_registered_number, company_id) FROM ({fin_sql})"
con.execute("CREATE OR REPLACE TEMP VIEW _fin AS " + fin_cols_sql)

# 2) Prepare metadata with preferred columns + derived fields
meta_prepped_sql = f"""
SELECT
  _key,
  COALESCE(entity_current_legal_name, company_name) AS entity_current_legal_name,
  COALESCE(company_type, CompanyCategory, type) AS company_type,
  COALESCE(company_status, CompanyStatus, status) AS company_status,
  COALESCE(incorporation_date, IncorporationDate, date_of_creation) AS incorporation_date,
  sic_codes
FROM _meta_base
WHERE _key IS NOT NULL
"""
con.execute("CREATE OR REPLACE TEMP VIEW _meta AS " + meta_prepped_sql)

# 3) If latest_toggle, reduce to latest metadata row per company (no reliable timestamp -> just pick any)
con.execute("""
CREATE OR REPLACE TEMP VIEW _meta_uniq AS
SELECT * FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY _key ORDER BY _key) AS rn
  FROM _meta
) t WHERE rn=1
""")

# 4) Build WHERE clause from sidebar
where = []
params: Dict[str, Any] = {}

if q_name.strip():
    where.append("LOWER(entity_current_legal_name) LIKE LOWER($name_pattern)")
    params["name_pattern"] = f"%{q_name.strip()}%"

if q_id.strip():
    where.append("_fin._key = REGEXP_REPLACE($cid,'[^0-9]','','g')")
    params["cid"] = q_id.strip()

if q_sic.strip():
    # match any of the provided codes inside the list `sic_codes`
    # duckdb: list_contains(sic_codes, code)
    sic_parts = re.split(r"[,\s;]+", q_sic.strip())
    sic_parts = [p for p in sic_parts if p]
    ors = []
    for idx, code in enumerate(sic_parts):
        ors.append(f"list_contains(_meta_uniq.sic_codes, '{code}')")
    if ors:
        where.append("(" + " OR ".join(ors) + ")")

if cat_choice and cat_choice != "(any)":
    where.append("_meta_uniq.company_type = $cat")
    params["cat"] = cat_choice

if stat_choice and stat_choice != "(any)":
    where.append("_meta_uniq.company_status = $st")
    params["st"] = stat_choice

# Incorporation date range
def valid_date(s: str) -> bool:
    return bool(re.match(r"^\d{4}-\d{2}-\d{2}$", s))
if date_from and valid_date(date_from):
    where.append("try_strict_cast(_meta_uniq.incorporation_date AS DATE) >= try_strict_cast($dfrom AS DATE)")
    params["dfrom"] = date_from
if date_to and valid_date(date_to):
    where.append("try_strict_cast(_meta_uniq.incorporation_date AS DATE) <= try_strict_cast($dto AS DATE)")
    params["dto"] = date_to

# Section filter (A–U) derived from first matching SIC in the list
if wanted_section and wanted_section != "(any)":
    # UNNEST codes and check any mapped section equals wanted
    sec_case = sql_section_case("c")
    where.append(f"""
EXISTS (
  SELECT 1 FROM UNNEST(_meta_uniq.sic_codes) AS c
  WHERE {sec_case} = $sec
)
""")
    params["sec"] = wanted_section

# Numeric filters
for col in chosen_numeric:
    mn = st.sidebar.text_input(f"{col} min", "")
    mx = st.sidebar.text_input(f"{col} max", "")
    if mn.strip():
        where.append(f"try_strict_cast(_fin.{col} AS DOUBLE) >= try_strict_cast($mn_{col} AS DOUBLE)")
        params[f"mn_{col}"] = mn.strip()
    if mx.strip():
        where.append(f"try_strict_cast(_fin.{col} AS DOUBLE) <= try_strict_cast($mx_{col} AS DOUBLE)")
        params[f"mx_{col}"] = mx.strip()

where_sql = ("WHERE " + " AND ".join(where)) if where else ""

# 5) Final query
base_join = """
FROM _fin
JOIN _meta_uniq ON _fin._key = _meta_uniq._key
"""

# If showing latest per company: take *latest* financial row per company by best-effort date,
# otherwise keep all rows.
if latest_toggle:
    con.execute("""
    CREATE OR REPLACE TEMP VIEW _fin_latest AS
    SELECT * FROM (
      SELECT *,
             ROW_NUMBER() OVER (
               PARTITION BY _key
               ORDER BY COALESCE(balance_sheet_date, period_end, date) DESC NULLS LAST
             ) AS rn
      FROM _fin
    ) t WHERE rn=1
    """)
    base_join = """
    FROM _fin_latest AS _fin
    JOIN _meta_uniq ON _fin._key = _meta_uniq._key
    """

# Column selection: include everything (minus temp columns)
select_cols = """
  _fin._key AS company_id,
  entity_current_legal_name,
  company_type,
  company_status,
  incorporation_date,
  sic_codes,
  _fin.*
"""

sql = f"""
SELECT {select_cols}
{base_join}
{where_sql}
LIMIT {int(row_cap)}
"""

with st.spinner("Querying releases…"):
    df = con.execute(sql, params).fetch_df()

st.success(f"Results: {len(df):,} rows • Source: {'latest per company' if latest_toggle else 'all filings'} across all releases")
st.dataframe(df, use_container_width=True)

st.download_button("Download CSV (shown rows)", df.to_csv(index=False).encode("utf-8"),
                   file_name="companies_filtered.csv", mime="text/csv")
