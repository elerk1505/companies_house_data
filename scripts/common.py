from __future__ import annotations
import os, io, re, json, time, zipfile, hashlib, tempfile, datetime as dt
from typing import List, Dict, Optional
import pandas as pd
import requests

GH_REPO = os.getenv("GH_REPO", "OWNER/REPO")  # e.g. 'ellaerkman/ixbrl-financials-db'
GH_TOKEN = os.getenv("GITHUB_TOKEN", "")
SESSION = requests.Session()
if GH_TOKEN:
    SESSION.headers.update({"Authorization": f"Bearer {GH_TOKEN}", "Accept": "application/vnd.github+json"})
SESSION.headers.update({"User-Agent": "Allosaurus/1.0"})

def _gh_api(url: str, method: str = "GET", **kwargs):
    r = SESSION.request(method, url, timeout=120, **kwargs)
    if not r.ok:
        raise RuntimeError(f"GitHub API error {r.status_code}: {r.text[:400]}")
    return r

def gh_release_get(tag: str) -> Dict | None:
    url = f"https://api.github.com/repos/{GH_REPO}/releases/tags/{tag}"
    r = SESSION.get(url, timeout=60)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()

def gh_release_ensure(tag: str, name: Optional[str] = None) -> Dict:
    rel = gh_release_get(tag)
    if rel:
        return rel
    url = f"https://api.github.com/repos/{GH_REPO}/releases"
    body = {"tag_name": tag, "name": name or tag, "draft": False, "prerelease": False}
    return _gh_api(url, method="POST", json=body).json()

def gh_release_find_asset(release: Dict, filename: str) -> Dict | None:
    for a in release.get("assets", []):
        if a.get("name") == filename:
            return a
    return None

def gh_release_download_asset(asset: Dict, dest_path: str) -> Optional[str]:
    url = asset.get("browser_download_url")
    if not url:
        return None
    r = SESSION.get(url, timeout=600)
    r.raise_for_status()
    with open(dest_path, "wb") as f:
        f.write(r.content)
    return dest_path

def gh_release_upload_or_replace_asset(release: Dict, filepath: str, name: Optional[str] = None):
    # delete existing asset with same name (GitHub requires delete before replace)
    fname = name or os.path.basename(filepath)
    exist = gh_release_find_asset(release, fname)
    if exist:
        del_url = exist["url"]  # API URL
        _gh_api(del_url, method="DELETE")
    # upload
    upload_url = release["upload_url"].split("{", 1)[0] + f"?name={fname}"
    with open(filepath, "rb") as f:
        data = f.read()
    headers = {"Content-Type": "application/octet-stream"}
    _gh_api(upload_url, method="POST", data=data, headers=headers)

# Helpers

def half_from_date(d: dt.date) -> str:
    return "H1" if d.month <= 6 else "H2"

def tag_for_financials(year: int, half: str) -> str:
    return f"data-{year}-{half}-financials"

def tag_for_metadata(year: int, half: str) -> str:
    return f"data-{year}-{half}-metadata"

# Idempotent append + de-dupe

def append_parquet(output_path: str, new_df: pd.DataFrame, subset_keys: List[str]):
    if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
        old = pd.read_parquet(output_path)
        df = pd.concat([old, new_df], ignore_index=True)
    else:
        df = new_df.copy()
    df.drop_duplicates(subset=subset_keys, keep="last", inplace=True)
    df.to_parquet(output_path, index=False)
