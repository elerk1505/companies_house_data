from __future__ import annotations

import os
import io
import json
import time
import mimetypes
from typing import Iterable, List, Optional

import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

GITHUB_API = "https://api.github.com"

# Single session for all HTTP
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "CompaniesHouseFinder/1.0"})


# ----------------------------- GitHub API -----------------------------

def _gh_token_repo() -> tuple[str, str]:
    token = os.environ.get("GITHUB_TOKEN")
    repo = os.environ.get("GH_REPO")
    if not token or not repo:
        raise RuntimeError("GITHUB_TOKEN and GH_REPO environment variables must be set")
    return token, repo

def _gh(method: str, url: str, token: str, **kwargs) -> requests.Response:
    headers = kwargs.pop("headers", {})
    headers["Authorization"] = f"Bearer {token}"
    headers["Accept"] = "application/vnd.github+json"
    return SESSION.request(method, url, headers=headers, **kwargs)

def gh_release_ensure(tag: str, name: Optional[str] = None) -> dict:
    """
    Ensure a release exists for tag; create if missing. Returns release JSON.
    """
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
    Upload file as release asset, replacing existing asset of the same name.

    - Ignores 404 during delete (asset may already be gone / concurrent replace)
    - Refreshes release before upload to avoid stale asset lists
    - Returns the fresh release JSON
    """
    token, repo = _gh_token_repo()

    # Best-effort delete of existing asset
    existing = gh_release_find_asset(rel, name)
    if existing:
        del_url = f"{GITHUB_API}/repos/{repo}/releases/assets/{existing['id']}"
        dr = _gh("DELETE", del_url, token)
        # 204 = deleted, 404 = already gone; anything else raise
        if dr.status_code not in (204, 404):
            dr.raise_for_status()
        # refresh after delete
        rel = gh_release_refresh(rel["tag_name"])

    # Upload
    upload_url = rel["upload_url"].split("{", 1)[0]
    mime = mimetypes.guess_type(file_path)[0] or "application/octet-stream"
    with open(file_path, "rb") as f:
        ur = SESSION.post(
            f"{upload_url}?name={name}",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": mime,
                "Accept": "application/vnd.github+json",
            },
            data=f,
        )
    if ur.status_code not in (200, 201):
        # If another runner uploaded faster, GitHub may return 422; refresh and continue
        if ur.status_code != 422:
            ur.raise_for_status()
    return gh_release_refresh(rel["tag_name"])
# ---------------------------------------------------------------------


# ----------------------------- Parquet I/O ----------------------------

def _to_arrow_table(df: pd.DataFrame) -> pa.Table:
    """
    Convert DataFrame to Arrow Table with light normalization so mixed types
    (e.g., list/object) don't crash Parquet writes.
    """
    # Ensure list-like columns are stored as large_list<item=utf8>
    arrays = {}
    for c in df.columns:
        s = df[c]
        # Lists -> convert to string list safely
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
    Creates the file if it doesn't exist.
    """
    # Normalize some common columns to string to avoid dtype conflicts
    for c in df_new.columns:
        if c.endswith("_date") or c in ("incorporation_date",):
            # store dates as ISO text consistently
            df_new[c] = pd.to_datetime(df_new[c], errors="coerce").dt.strftime("%Y-%m-%d")
    # Read existing if present
    if os.path.exists(path):
        df_old = pd.read_parquet(path)
        df_all = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df_all = df_new.copy()

    df_all = df_all.drop_duplicates(subset=list(subset_keys), keep="last")

    # Write with pyarrow
    table = _to_arrow_table(df_all)
    pq.write_table(table, path, compression="snappy")
