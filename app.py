# app.py
from __future__ import annotations
import os, re, json, sys
from functools import lru_cache
from typing import Any, Dict, List, Optional

import streamlit as st
import requests

# ──────────────────────────────────────────────────────────────────────────────
# DuckDB import & hints for the Streamlit Cloud runtime
# ──────────────────────────────────────────────────────────────────────────────
try:
    import duckdb  # noqa: F401
except Exception as e:
    st.error(
        "DuckDB isn’t available in this runtime.\n\n"
        f"Python: {sys.version}\n"
        f"Exception: {e}\n\n"
        "Fixes:\n"
        "• In Streamlit Cloud, set Python to 3.12 and pin duckdb==0.10.3.\n"
        "• requirements.txt should include: streamlit==1.37.0, duckdb==0.10.3, pandas==2.2.2, requests==2.32.3"
    )
    st.stop()

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
GH_REPO_DEFAULT = "elerk1505/companies_house_data"

# Optional token (recommended to avoid GitHub API rate limits)
try:
    GITHUB_TOKEN = os.getenv("GITHUB_TOKEN") or st.secrets.get("GITHUB_TOKEN", "")
except Exception:
    GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")

# All numeric columns the user wants to be able to filter by — we’ll only
# expose those that actually exist in the joined result set.
NUMERIC_CANDIDATES = [
    "average_number_employees_during_period",
    "tangible_fixed_assets", "debtors", "cash_bank_in_hand", "current_assets",
    "creditors_due_within_one_year", "creditors_due_after_one_year",
    "net_current_assets_liabilities",
    "total_assets_less_current_liabilities",
    "net_assets_liabilities_including_pension_asset_liability",
    "called_up_share_capital", "profit_loss_account_reserve", "shareholder_funds",
    "turnover_gross_operating_revenue", "other_operating_income", "cost_sales",
    "gross_profit_loss", "administrative_expenses", "raw_materials_consumables",
    "staff_costs",
    "depreciation_other_amounts_written_off_tangible_intangible_fixed_assets",
    "other_operating_charges_format2", "operating_profit_loss",
    "profit_loss_on_ordinary_activities_before_tax",
    "tax_on_profit_or_loss_on_ordinary_activities",
    "profit_loss_for_period",
]

# ──────────────────────────────────────────────────────────────────────────────
# Streamlit UI
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config("Companies House – Search", layout="wide")
st.title("Companies House – Search across all releases")
st.caption("Queries all public releases in the GitHub repo and scans Parquet **in-place** via DuckDB httpfs. Always up-to-date — no manual uploads.")

repo = GH_REPO_DEFAULT
st.sidebar.subheader("Options")
latest_toggle = st.sidebar.toggle(
    "Show latest record per company (recommended)",
    value=True,
    help="If off, queries span ALL filings across all releases."
)
row_cap = st.sidebar.number_input("Max rows to show", 100, 200_000, 10_000, step=1000)

st.sidebar.subheader("Search")
q_name = st.sidebar.text_input("Company name contains", "")
q_id   = st.sidebar.text_input("Company ID equals (digits only)", "")
q_sic  = st.sidebar.text_input("SIC codes include (comma/space separated)", "")

st.sidebar.subheader("Dropdown filters")
wanted_section = st.sidebar.selectbox("SIC section (A–U)", ["(any)"] + list("ABCDEFGHIJKLMNOPQRSTU"))
cat_choice = st.sidebar.selectbox("Company category", ["(any)"])
stat_choice = st.sidebar.selectbox("Company status", ["(any)"])

st.sidebar.subheader("Numeric filters")
chosen_numeric = st.sidebar.multiselect("Pick numeric columns", NUMERIC_CANDIDATES, default=[])

st.sidebar.subheader("Incorporation date")
date_from = st.sidebar.text_input("From (YYYY-MM-DD)", "")
date_to   = st.sidebar.text_input("To (YYYY-MM-DD)", "")

# ──────────────────────────────────────────────────────────────────────────────
# GitHub helpers
# ──────────────────────────────────────────────────────────────────────────────
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

def tag_parts(text: str) -> Optional[tuple[int, str, str]]:
    """
    Parse flexible forms:
      - data-2025-H1-financials / data_2024_H2_metadata
      - Financials 2023 H1 / Metadata 2022 H2
    Returns (year, 'H1'|'H2', 'financials'|'metadata') or None.
    """
    if not text:
        return None
    t = text.strip()

    m = re.search(r'(?i)(?:^|[\s_-])(\d{4})[\s_-]*(H[12])[\s_-]*(financials?|metadata)(?:$|[\s_-])', t)
    if m:
        year = int(m.group(1))
        half = m.group(2).upper()
        kind = "financials" if m.group(3).lower().startswith("financ") else "metadata"
        return (year, half, kind)

    m = re.search(r'(?i)\b(financials?|metadata)\b.*\b(\d{4})\b.*\b(H[12])\b', t)
    if m:
        kind = "financials" if m.group(1).lower().startswith("financ") else "metadata"
        year = int(m.group(2))
        half = m.group(3).upper()
        return (year, half, kind)

    return None

def parquet_asset_urls(rel: dict) -> list[str]:
    urls = []
    for a in rel.get("assets", []):
        nm = (a.get("name") or "").lower()
        if nm.endswith(".parquet"):
            urls.append(a.get("browser_download_url"))
    return urls

@st.cache_data(show_spinner=True)
def build_asset_lists(repo: str) -> dict:
    """
    Returns: {'financials': [...], 'metadata': [...]}
    Aggregates across ALL releases; classification by tag/title, fallback by filename.
    """
    rels = list_releases(repo)
    fins, metas = [], []

    for r in rels:
        label = r.get("tag_name") or r.get("name") or ""
        cls = tag_parts(label)
        kind_hint = cls[2] if cls else None

        urls = parquet_asset_urls(r)
        if not urls:
            continue

        for url in urls:
            put = None
            if kind_hint in ("financials", "metadata"):
                put = kind_hint
            else:
                low = url.lower()
                if "financ" in low:
                    put = "financials"
                elif "meta" in low:
                    put = "metadata"

            if put == "financials":
                fins.append(url)
            elif put == "metadata":
                metas.append(url)
            else:
                # fallback balancing
                if not fins and metas:
                    fins.append(url)
                elif fins and not metas:
                    metas.append(url)
                else:
                    metas.append(url)

    fins = sorted(set(fins))
    metas = sorted(set(metas))
    return {"financials": fins, "metadata": metas}

# ──────────────────────────────────────────────────────────────────────────────
# DuckDB connection & helpers
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def connect_duckdb():
    import duckdb
    from duckdb import CatalogException
    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
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

def build_url_tables_from_lists(assets: dict) -> tuple[str, str]:
    fin_urls = assets.get("financials", [])
    meta_urls = assets.get("metadata", [])

    if not fin_urls and not meta_urls:
        st.warning("No financials or metadata assets discovered in releases.")
        st.stop()
    if not fin_urls:
        st.warning("Found metadata releases but no financials yet. Add financial Parquet to run queries.")
        st.stop()
    if not meta_urls:
        st.warning("Found financial releases but no metadata yet. Add metadata Parquet to enable joins.")
        st.stop()

    fin_sql  = f"SELECT * FROM read_parquet({json.dumps(fin_urls)})"
    meta_sql = f"SELECT * FROM read_parquet({json.dumps(meta_urls)})"
    return fin_sql, meta_sql

def sql_section_case(col: str) -> str:
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

# ──────────────────────────────────────────────────────────────────────────────
# Build SQL sources
# ──────────────────────────────────────────────────────────────────────────────
con = connect_duckdb()
assets = build_asset_lists(repo)
fin_sql, meta_sql = build_url_tables_from_lists(assets)

# Reset views safely between reruns (connection is cached)
for v in ["_meta_uniq", "_meta", "_meta_raw", "_fin_latest", "_fin"]:
    con.execute(f"DROP VIEW IF EXISTS {v} CASCADE;")

# 1) FINANCIALS: normalize id key, keep all columns
fin_key = """
COALESCE(
  NULLIF(LTRIM(REGEXP_REPLACE(companies_house_registered_number::TEXT,'[^0-9]','','g'),'0'),''),
  NULLIF(LTRIM(REGEXP_REPLACE(company_id::TEXT,'[^0-9]','','g'),'0'),'')
) AS _key
"""
# Avoid duplicate id columns after we add _key
fin_view = f"SELECT {fin_key}, * EXCLUDE (companies_house_registered_number, company_id) FROM ({fin_sql})"
con.execute("CREATE OR REPLACE TEMP VIEW _fin AS " + fin_view)

# 2) METADATA: create raw view, inspect columns, then safely select with COALESCE
con.execute("CREATE OR REPLACE TEMP VIEW _meta_raw AS " + f"SELECT * FROM ({meta_sql})")
meta_cols = {r[1] for r in con.execute("PRAGMA table_info('_meta_raw')").fetchall()}

def present(*cands: str) -> list[str]:
    """Return subset of candidates that exist in _meta_raw, in order."""
    return [c for c in cands if c in meta_cols]

# Build key and preferred columns defensively
key_exprs = []
for c in present("companies_house_registered_number", "CompanyNumber", "company_number"):
    key_exprs.append(f"NULLIF(LTRIM(REGEXP_REPLACE({c}::TEXT,'[^0-9]','','g'),'0'),'')")
_key_expr = "COALESCE(" + ", ".join(key_exprs) + ")" if key_exprs else "NULL"

name_exprs = present("entity_current_legal_name", "company_name")
name_sql = "COALESCE(" + ", ".join(name_exprs) + ")" if name_exprs else "NULL"

cat_exprs = present("company_type", "CompanyCategory", "type")
cat_sql = "COALESCE(" + ", ".join(cat_exprs) + ")" if cat_exprs else "NULL"

st_exprs = present("company_status", "CompanyStatus", "status")
st_sql = "COALESCE(" + ", ".join(st_exprs) + ")" if st_exprs else "NULL"

inc_exprs = present("incorporation_date", "IncorporationDate", "date_of_creation")
inc_sql = "COALESCE(" + ", ".join(inc_exprs) + ")" if inc_exprs else "NULL"

sic_col = "sic_codes" if "sic_codes" in meta_cols else "NULL"

meta_base = f"""
SELECT
  {_key_expr} AS _key,
  {name_sql}  AS entity_current_legal_name,
  {cat_sql}   AS company_type,
  {st_sql}    AS company_status,
  {inc_sql}   AS incorporation_date,
  {sic_col}   AS sic_codes
FROM _meta_raw
WHERE {_key_expr} IS NOT NULL
"""
con.execute("CREATE OR REPLACE TEMP VIEW _meta AS " + meta_base)

# Distinct one metadata row per company (no timestamp available → arbitrary stable pick)
con.execute("""
CREATE OR REPLACE TEMP VIEW _meta_uniq AS
SELECT * FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY _key ORDER BY _key) AS rn
  FROM _meta
) t WHERE rn=1
""")

# Populate category & status dropdowns
cats = [r[0] for r in con.execute("SELECT DISTINCT company_type FROM _meta_uniq WHERE company_type IS NOT NULL ORDER BY 1").fetchall()]
stats = [r[0] for r in con.execute("SELECT DISTINCT company_status FROM _meta_uniq WHERE company_status IS NOT NULL ORDER BY 1").fetchall()]
if cats:
    cat_choice = st.sidebar.selectbox("Company category", ["(any)"] + cats, index=0)
if stats:
    stat_choice = st.sidebar.selectbox("Company status", ["(any)"] + stats, index=0)

# Latest financial row per company by best-effort date
con.execute("""
CREATE OR REPLACE TEMP VIEW _fin_latest AS
SELECT * FROM (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY _key
           ORDER BY COALESCE(
                    try_cast(balance_sheet_date AS TIMESTAMP),
                    try_cast(period_end        AS TIMESTAMP),
                    try_cast(date              AS TIMESTAMP)
                  ) DESC NULLS LAST
         ) AS rn
  FROM _fin
) t WHERE rn=1
""")

# ──────────────────────────────────────────────────────────────────────────────
# WHERE clause assembly
# ──────────────────────────────────────────────────────────────────────────────
where: List[str] = []
params: Dict[str, Any] = {}

if q_name.strip():
    where.append("LOWER(_meta_uniq.entity_current_legal_name) LIKE LOWER($name_pattern)")
    params["name_pattern"] = f"%{q_name.strip()}%"

if q_id.strip():
    where.append("_fin._key = REGEXP_REPLACE($cid,'[^0-9]','','g')")
    params["cid"] = q_id.strip()

if q_sic.strip() and "sic_codes" in meta_cols:
    parts = [p for p in re.split(r"[,\s;]+", q_sic.strip()) if p]
    if parts:
        ors = [f"list_contains(_meta_uniq.sic_codes, '{p}')" for p in parts]
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

if wanted_section and wanted_section != "(any)":
    sec_case = sql_section_case("c")
    where.append(f"""
EXISTS (
  SELECT 1 FROM UNNEST(_meta_uniq.sic_codes) AS c
  WHERE {sec_case} = $sec
)
""")
    params["sec"] = wanted_section

# Only include numeric filters that really exist in _fin
existing_fin_cols = {r[1] for r in con.execute("PRAGMA table_info('_fin')").fetchall()}
for col in chosen_numeric:
    if col not in existing_fin_cols:
        continue
    mn = st.sidebar.text_input(f"{col} min", "")
    mx = st.sidebar.text_input(f"{col} max", "")
    if mn.strip():
        where.append(f"try_strict_cast(_fin.{col} AS DOUBLE) >= try_strict_cast($mn_{col} AS DOUBLE)")
        params[f"mn_{col}"] = mn.strip()
    if mx.strip():
        where.append(f"try_strict_cast(_fin.{col} AS DOUBLE) <= try_strict_cast($mx_{col} AS DOUBLE)")
        params[f"mx_{col}"] = mx.strip()

where_sql = ("WHERE " + " AND ".join(where)) if where else ""

# Choose source for financials based on toggle
fin_source = "_fin_latest" if latest_toggle else "_fin"

# Avoid ambiguous columns by qualifying the metadata name column explicitly
select_cols = f"""
  _fin._key AS company_id,
  _meta_uniq.entity_current_legal_name AS entity_current_legal_name,
  _meta_uniq.company_type,
  _meta_uniq.company_status,
  _meta_uniq.incorporation_date,
  _meta_uniq.sic_codes,
  _fin.*
"""

sql = f"""
SELECT {select_cols}
FROM {fin_source} AS _fin
JOIN _meta_uniq ON _fin._key = _meta_uniq._key
{where_sql}
LIMIT {int(row_cap)}
"""

with st.spinner("Querying releases…"):
    df = con.execute(sql, params).fetch_df()

st.success(
    f"Results: {len(df):,} rows • Source: "
    f"{'latest per company' if latest_toggle else 'all filings'} • "
    f"Financial files: {len(assets['financials'])}, Metadata files: {len(assets['metadata'])}"
)
st.dataframe(df, use_container_width=True)

st.download_button(
    "Download CSV (shown rows)",
    df.to_csv(index=False).encode("utf-8"),
    file_name="companies_filtered.csv",
    mime="text/csv",
)
