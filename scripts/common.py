# scripts/common.py
from __future__ import annotations

import os
import time
import mimetypes
from typing import Iterable, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

# ----------------------------- GitHub API -----------------------------
GITHUB_API = "https://api.github.com"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "CompaniesHouseFinder/1.0"})

def _gh_token_repo() -> tuple[str, str]:
    token = os.environ.get("GITHUB_TOKEN")
    repo = os.environ.get("GH_REPO")
    if not token or not repo:
        raise RuntimeError("GITHUB_TOKEN and GH_REPO must be set (owner/repo).")
    return token, repo

def _gh(method: str, url: str, token: str, **kwargs) -> requests.Response:
    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {token}"
    headers["Accept"] = "application/vnd.github+json"
    return SESSION.request(method, url, headers=headers, **kwargs)

def gh_release_ensure(tag: str, name: Optional[str] = None) -> dict:
    """Return release JSON for tag, creating the release if it doesn't exist."""
    token, repo = _gh_token_repo()
    r = _gh("GET", f"{GITHUB_API}/repos/{repo}/releases/tags/{tag}", token)
    if r.status_code == 404:
        payload = {"tag_name": tag, "name": name or tag, "draft": False, "prerelease": False}
        r2 = _gh("POST", f"{GITHUB_API}/repos/{repo}/releases", token, json=payload)
        r2.raise_for_status()
        return r2.json()
    r.raise_for_status()
    return r.json()

def gh_release_refresh(tag: str) -> dict:
    token, repo = _gh_token_repo()
    r = _gh("GET", f"{GITHUB_API}/repos/{repo}/releases/tags/{tag}", token)
    r.raise_for_status()
    return r.json()

def gh_release_find_asset(rel: dict, name: str) -> Optional[dict]:
    for a in rel.get("assets") or []:
        if a.get("name") == name:
            return a
    return None

def gh_release_download_asset(asset: dict, dest_path: str) -> None:
    url = asset["browser_download_url"]
    with SESSION.get(url, stream=True) as r, open(dest_path, "wb") as f:
        r.raise_for_status()
        for chunk in r.iter_content(chunk_size=1 << 20):
            if chunk:
                f.write(chunk)

def gh_release_upload_or_replace_asset(rel: dict, file_path: str, name: str) -> dict:
    """
    Upload file as a release asset, replacing existing asset of the same name.
    - Ignore 404 on DELETE (already gone)
    - Refresh release after delete to avoid stale asset lists
    - Retry upload once; tolerate 422 race
    """
    token, repo = _gh_token_repo()

    # best-effort delete
    existing = gh_release_find_asset(rel, name)
    if existing:
        del_url = f"{GITHUB_API}/repos/{repo}/releases/assets/{existing['id']}"
        dr = _gh("DELETE", del_url, token)
        if dr.status_code not in (204, 404):
            dr.raise_for_status()
        rel = gh_release_refresh(rel["tag_name"])

    # upload
    upload_url = rel["upload_url"].split("{", 1)[0]
    mime = mimetypes.guess_type(file_path)[0] or "application/octet-stream"
    with open(file_path, "rb") as f:
        up = SESSION.post(
            f"{upload_url}?name={name}",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": mime,
                "Accept": "application/vnd.github+json",
            },
            data=f,
        )

    if up.status_code in (200, 201):
        return gh_release_refresh(rel["tag_name"])
    if up.status_code == 422:  # race with another runner
        return gh_release_refresh(rel["tag_name"])

    time.sleep(2)
    with open(file_path, "rb") as f:
        up2 = SESSION.post(
            f"{upload_url}?name={name}",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": mime,
                "Accept": "application/vnd.github+json",
            },
            data=f,
        )
    if up2.status_code not in (200, 201, 422):
        up2.raise_for_status()
    return gh_release_refresh(rel["tag_name"])

# ----------------------------- Parquet I/O ----------------------------

def _to_arrow_table(df: pd.DataFrame) -> pa.Table:
    """
    Convert DataFrame to Arrow Table with safe handling for list/object columns.
    """
    arrays = {}
    for c in df.columns:
        s = df[c]
        if s.dtype == "object" and any(isinstance(v, list) for v in s.dropna().head(10)):
            arrays[c] = pa.array(
                [v if isinstance(v, list) else (None if pd.isna(v) else [str(v)]) for v in s],
                type=pa.large_list(pa.string()),
            )
        else:
            arrays[c] = pa.array(s.astype("object").where(pd.notna(s), None))
    return pa.table(arrays)

def append_parquet(path: str, df_new: pd.DataFrame, subset_keys: Iterable[str]) -> None:
    """
    Append rows to Parquet at `path`, de-duplicating by `subset_keys`.
    Creates the file if it doesn't exist or is empty/corrupt.
    """
    # Normalize date-like fields to ISO strings (consistent across writers)
    for c in df_new.columns:
        if c.endswith("_date") or c in ("incorporation_date",):
            df_new[c] = pd.to_datetime(df_new[c], errors="coerce").dt.strftime("%Y-%m-%d")

    # Try to read existing parquet; start fresh if missing/empty/corrupt
    must_fresh = True
    if os.path.exists(path) and os.path.getsize(path) > 0:
        try:
            df_old = pd.read_parquet(path)
            df_all = pd.concat([df_old, df_new], ignore_index=True)
            must_fresh = False
        except Exception as e:
            print(f"[warn] could not read existing parquet {path} ({e}); recreating fresh")
    if must_fresh:
        df_all = df_new.copy()

    df_all = df_all.drop_duplicates(subset=list(subset_keys), keep="last")

    table = _to_arrow_table(df_all)
    pq.write_table(table, path, compression="snappy")

# ------------------------ Date / Tag utilities ------------------------

def half_from_date(d) -> str | None:
    """
    Return 'H1' if month <= 6 else 'H2'.
    Accepts datetime/date/Timestamp or string; returns None if unknown.
    """
    if pd.isna(d):
        return None
    m = getattr(d, "month", None)
    if not m:
        try:
            dt = pd.to_datetime(d, errors="coerce")
            if pd.isna(dt):
                return None
            m = dt.month
        except Exception:
            return None
    return "H1" if m <= 6 else "H2"

def tag_for_financials(year: int, half: str) -> str:
    return f"data-{year}-{half}-financials"

def tag_for_metadata(year: int, half: str) -> str:
    return f"data-{year}-{half}-metadata"
