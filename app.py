# app.py
from __future__ import annotations
import os, re, json, sys
from functools import lru_cache
from typing import Dict, Any, Optional, Tuple, List

import streamlit as st
import requests

# --- Try duckdb early & fail friendly ----------------------------------------
try:
    import duckdb  # noqa: F401
except Exception as e:
    st.error(
        "DuckDB isn’t available in this runtime.\n\n"
        f"Python: {sys.version}\n"
        f"Exception: {e}\n\n"
        "Fixes:\n"
        "• Streamlit Cloud → App settings → Python version = 3.12\n"
        "• requirements.txt includes: duckdb==0.10.3\n"
        "• Reboot app after saving."
    )
    st.stop()

# --- Config -------------------------------------------------------------------
GH_REPO = "elerk1505/companies_house_data"
FIN_NAME = "financials.parquet"
META_NAME = "metadata.parquet"

# Optional token for higher GitHub rate limits
try:
    GITHUB_TOKEN = os.getenv("GITHUB_TOKEN") or st.secrets.get("GITHUB_TOKEN", "")
except Exception:
    GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")

NUMERIC_COLS: List[str] = [
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

# --- Page ---------------------------------------------------------------------
st.set_page_config("Companies House – Search", layout="wide")
st.title("Companies House – Search across all releases")
st.caption("Search and filter **all published releases** directly from GitHub using DuckDB HTTPFS.")

# --- GitHub helpers -----------------------------------------------------------
def gh_session() -> requests.Session:
    s = requests.Session()
    if GITHUB_TOKEN:
        s.headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    s.headers["User-Agent"] = "CH-Finder/1.0"
    s.headers["Accept"] = "application/vnd.github+json"
    return s

@lru_cache(maxsize=1)
def list_releases(repo: str) -> list[dict]:
    url = f"https://api.github.com/repos/{repo}/releases?per_page=100"
    r = gh_session().get(url, timeout=60)
    r.raise_for_status()
    return r.json()

def pick_assets(rel: dict) -> Tuple[Optional[str], Optional[str]]:
    fin_url = meta_url = None
    for a in rel.get("assets", []):
        n = a.get("name", "")
        if n == FIN_NAME:  fin_url  = a.get("browser_download_url")
        if n == META_NAME: meta_url = a.get("browser_download_url")
    return fin_url, meta_url

@st.cache_data(show_spinner=True)
def build_manifest(repo: str) -> Dict[str, list[str]]:
    fins, metas = [], []
    for r in list_releases(repo):
        fin, meta = pick_assets(r)
        if fin:  fins.append(fin)
        if meta: metas.append(meta)
    if not fins or not metas:
        raise RuntimeError("No financials or metadata assets found in releases.")
    return {"fin_urls": fins, "meta_urls": metas}

# --- DuckDB connection --------------------------------------------------------
@st.cache_resource(show_spinner=False)
def connect_duckdb():
    import duckdb
    from duckdb import CatalogException
    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    try: con.execute("SET enable_http_metadata_cache=true;")
    except CatalogException: pass
    try: con.execute("SET http_metadata_cache_size='512MB';")
    except CatalogException:
        try: con.execute("SET parquet_metadata_cache=true;")
        except CatalogException: pass
    return con

def sic_section_case(col: str) -> str:
    return f"""
    CASE WHEN try_strict_cast({col} AS INT) IS NULL THEN NULL
      ELSE CASE
        WHEN try_strict_cast({col} AS INT)/100 BETWEEN 1 AND 3  THEN 'A'
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

# --- Inputs -------------------------------------------------------------------
st.sidebar.header("Search")
q_name = st.sidebar.text_input("Company name contains", "")
q_id   = st.sidebar.text_input("Company ID (digits only)", "")
q_sic  = st.sidebar.text_input("SIC codes include (comma/space separated)", "")

st.sidebar.header("Categorical")
wanted_section = st.sidebar.selectbox("SIC section (A–U)", ["(any)"] + list("ABCDEFGHIJKLMNOPQRSTU"))
cat_choice     = st.sidebar.selectbox("Company category", ["(discovering…)"])
stat_choice    = st.sidebar.selectbox("Company status", ["(discovering…)"])

st.sidebar.header("Incorporation date")
date_from = st.sidebar.text_input("From (YYYY-MM-DD)", "")
date_to   = st.sidebar.text_input("To (YYYY-MM-DD)", "")

st.sidebar.header("Numeric filters")
row_cap = st.sidebar.number_input("Max rows to show", 100, 200_000, 10_000, step=1000)

# --- Build data model ---------------------------------------------------------
con = connect_duckdb()
# --- Make reruns idempotent: drop any old views first -------------------------
def reset_views(con):
    # Drop in reverse dependency order; CASCADE handles nested deps safely
    for v in ["_fin_latest", "_fin", "_meta_uniq", "_meta", "_meta_raw"]:
        con.execute(f"DROP VIEW IF EXISTS {v} CASCADE")

reset_views(con)
manifest = build_manifest(GH_REPO)

fin_sql  = f"SELECT * FROM read_parquet({json.dumps(manifest['fin_urls'])})"
meta_sql = f"SELECT * FROM read_parquet({json.dumps(manifest['meta_urls'])})"

# 1) FINANCIALS view with normalized key
fin_view = f"""
SELECT
  COALESCE(
    NULLIF(LTRIM(REGEXP_REPLACE(companies_house_registered_number::TEXT,'[^0-9]','','g'),'0'),''),
    NULLIF(LTRIM(REGEXP_REPLACE(company_id::TEXT,'[^0-9]','','g'),'0'),'')
  ) AS _key,
  * EXCLUDE (companies_house_registered_number, company_id)
FROM ({fin_sql})
"""
con.execute("CREATE OR REPLACE TEMP VIEW _fin AS " + fin_view)

# 2) METADATA: create raw view, inspect columns, then safely build SELECT
con.execute("CREATE OR REPLACE TEMP VIEW _meta_raw AS " + f"SELECT * FROM ({meta_sql})")
meta_cols = {r[1] for r in con.execute("PRAGMA table_info('_meta_raw')").fetchall()}

def present(*cands: str) -> list[str]:
    return [c for c in cands if c in meta_cols]

def mk_key_expr(cols: list[str]) -> str:
    if not cols:
        # hard stop – we can't join without a key
        return None
    parts = [f"NULLIF(LTRIM(REGEXP_REPLACE({c}::TEXT,'[^0-9]','','g'),'0'),'')" for c in cols]
    return "COALESCE(" + ",".join(parts) + ") AS _key"

key_cols = present("companies_house_registered_number","CompanyNumber","company_number")
key_expr = mk_key_expr(key_cols)
if key_expr is None:
    st.error("No company number column found in metadata assets (expected one of: "
             "`companies_house_registered_number`, `CompanyNumber`, `company_number`).")
    st.stop()

name_cols   = present("entity_current_legal_name","company_name")
ctype_cols  = present("company_type","CompanyCategory","type")
cstat_cols  = present("company_status","CompanyStatus","status")
inc_cols    = present("incorporation_date","IncorporationDate","date_of_creation")
sic_cols    = present("sic_codes")

def mk_coalesce(cols: list[str], alias: str) -> str:
    if not cols:
        return f"NULL AS {alias}"
    return "COALESCE(" + ",".join(cols) + f") AS {alias}"

meta_prepped_sql = f"""
SELECT
  {key_expr},
  {mk_coalesce(name_cols,  'entity_current_legal_name')},
  {mk_coalesce(ctype_cols, 'company_type')},
  {mk_coalesce(cstat_cols, 'company_status')},
  {mk_coalesce(inc_cols,   'incorporation_date')},
  {('sic_codes' if sic_cols else 'NULL')} AS sic_codes
FROM _meta_raw
WHERE _key IS NOT NULL
"""
con.execute("CREATE OR REPLACE TEMP VIEW _meta AS " + meta_prepped_sql)

# One metadata row per company
con.execute("""
CREATE OR REPLACE TEMP VIEW _meta_uniq AS
SELECT * FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY _key ORDER BY _key) AS rn
  FROM _meta
) t WHERE rn=1
""")

# Discover financial columns to enable/disable numeric filters
fin_cols = {r[1] for r in con.execute("PRAGMA table_info('_fin')").fetchall()}

# Fill dropdowns from metadata
cats = [r[0] for r in con.execute("SELECT DISTINCT company_type FROM _meta_uniq WHERE company_type IS NOT NULL ORDER BY 1").fetchall()]
sts  = [r[0] for r in con.execute("SELECT DISTINCT company_status FROM _meta_uniq WHERE company_status IS NOT NULL ORDER BY 1").fetchall()]
cat_choice = st.sidebar.selectbox("Company category", ["(any)"] + cats, index=0)
stat_choice = st.sidebar.selectbox("Company status", ["(any)"] + sts, index=0)

# --- WHERE builder ------------------------------------------------------------
where: List[str] = []
params: Dict[str, Any] = {}

if q_name.strip():
    where.append("LOWER(entity_current_legal_name) LIKE LOWER($name_p)")
    params["name_p"] = f"%{q_name.strip()}%"

if q_id.strip():
    where.append("_fin._key = REGEXP_REPLACE($cid,'[^0-9]','','g')")
    params["cid"] = q_id.strip()

if q_sic.strip():
    codes = [c for c in re.split(r"[,\s;]+", q_sic.strip()) if c]
    if codes:
        ors = [f"list_contains(_meta_uniq.sic_codes, '{c}')" for c in codes]
        where.append("(" + " OR ".join(ors) + ")")

if cat_choice != "(any)":
    where.append("_meta_uniq.company_type = $cat")
    params["cat"] = cat_choice

if stat_choice != "(any)":
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

if wanted_section != "(any)":
    sec = sic_section_case("c")
    where.append(f"EXISTS (SELECT 1 FROM UNNEST(_meta_uniq.sic_codes) AS c WHERE {sec} = $sec)")
    params["sec"] = wanted_section

# Numeric filters (render all; disable if missing; only add SQL if present + filled)
specs = []
for col in NUMERIC_COLS:
    present = col in fin_cols
    c1, c2 = st.sidebar.columns(2)
    mn = c1.text_input(f"{col} min", key=f"mn_{col}", disabled=not present,
                       help=None if present else "Column not present in current data")
    mx = c2.text_input(f"{col} max", key=f"mx_{col}", disabled=not present,
                       help=None if present else "Column not present in current data")
    specs.append((col, present, mn, mx))

for col, present, mn, mx in specs:
    if not present: 
        continue
    if mn.strip():
        where.append(f"try_strict_cast(_fin.{col} AS DOUBLE) >= try_strict_cast($mn_{col} AS DOUBLE)")
        params[f"mn_{col}"] = mn.strip()
    if mx.strip():
        where.append(f"try_strict_cast(_fin.{col} AS DOUBLE) <= try_strict_cast($mx_{col} AS DOUBLE)")
        params[f"mx_{col}"] = mx.strip()

where_sql = "WHERE " + " AND ".join(where) if where else ""

# Latest financial row per company (best-effort date)
con.execute("""
CREATE TEMP VIEW _fin_latest AS
SELECT * FROM (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY _key
           ORDER BY COALESCE(
             try_cast(balance_sheet_date AS TIMESTAMP),
             try_cast(period_end        AS TIMESTAMP),
             try_cast(date              AS TIMESTAMP),
             TIMESTAMP '1900-01-01 00:00:00'
           ) DESC
         ) AS rn
  FROM _fin
) t WHERE rn=1
""")

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
FROM _fin_latest AS _fin
JOIN _meta_uniq ON _fin._key = _meta_uniq._key
{where_sql}
LIMIT {int(row_cap)}
"""

with st.spinner("Querying all releases…"):
    try:
        df = con.execute(sql, params).fetch_df()
    except Exception as e:
        st.exception(e)
        st.stop()

st.success(f"Results: {len(df):,} companies (latest filing per company)")
st.dataframe(df, use_container_width=True)
st.download_button("Download CSV (shown rows)", df.to_csv(index=False).encode("utf-8"),
                   "companies_filtered.csv", "text/csv")
