import argparse
import csv
import hashlib
import io
import json
import re
import sys
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

SOURCE_URL = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"

BAD_CHAR = {
    "'",
    "’",
    "‘",
    "‛",
    "′",
    "`",
    "´",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "cms-retrieval/1.0",
            "Accept": "*/*",
        }
    )
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=16, pool_maxsize=16)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def convert_to_snake_case(name: str) -> str:
    text = unicodedata.normalize("NFKC", name or "")
    text = "".join(ch for ch in text if ch not in BAD_CHAR)
    text = text.replace("–", "-").replace("—", "-")
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "column"


def good_filename(name: str) -> str:
    stem = Path(name).name
    stem = re.sub(r"[^A-Za-z0-9._-]+", "_", stem)
    return stem or "dataset.csv"


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)
        handle.write("\n")
    tmp.replace(path)


def fetch_datasets(
    session: requests.Session,
    theme: str,
) -> list[dict[str, Any]]:
    response = session.get(SOURCE_URL, timeout=120)
    response.raise_for_status()
    items = response.json()
    if not isinstance(items, list):
        raise RuntimeError("Unexpected metastore payload; expected a JSON array.")

    matched: list[dict[str, Any]] = []
    for item in items:
        themes = item.get("theme") or []
        if isinstance(themes, str):
            themes = [themes]
        if theme in themes:
            matched.append(item)

    return matched


def find_csv_distribution(item: dict[str, Any]) -> dict[str, Any] | None:
    for dist in item.get("distribution") or []:
        media = (dist.get("mediaType") or "").lower()
        url = dist.get("downloadURL") or ""
        if media == "text/csv" or url.lower().endswith(".csv"):
            return dist
    dists = item.get("distribution") or []
    return dists[0] if dists else None


def calculate_file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 256), b""):
            digest.update(chunk)
    return digest.hexdigest()


def fix_header(source: Path, destination: Path) -> list[str]:
    """Rewrite only the header row to snake_case; stream the rest unchanged."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    tmp = destination.with_suffix(destination.suffix + ".tmp")

    with source.open("rb") as raw_in:
        text_in = io.TextIOWrapper(raw_in, encoding="utf-8", newline="", errors="replace")
        orig_header_line = text_in.readline()
        parsed = next(csv.reader([orig_header_line])) if orig_header_line else []
        new_header = [convert_to_snake_case(col) for col in parsed]

        with tmp.open("w", encoding="utf-8", newline="") as text_out:
            writer = csv.writer(text_out, lineterminator="\n")
            writer.writerow(new_header)
            while True:
                chunk = text_in.read(1024 * 1024)
                if not chunk:
                    break
                text_out.write(chunk)
        # Detach so closing text_in does not close the underlying file twice.
        text_in.detach()

    tmp.replace(destination)
    return new_header


def download_qualified(
    item: dict[str, Any],
    download_url: str,
    dest: Path,
    prior: dict[str, Any] | None,
    force: bool,
) -> tuple[bool, str]:
    if force:
        return True, "force"
    if prior is None or not dest.exists():
        return True, "new_or_missing_local_file"

    prior_modified = prior.get("modified")
    current_modified = item.get("modified")
    if current_modified and prior_modified and current_modified > prior_modified:
        return True, f"metastore_modified {prior_modified} -> {current_modified}"

    if prior.get("download_url") != download_url:
        return True, "download_url_changed"

    prior_released = prior.get("released")
    current_released = item.get("released")
    if current_released and prior_released and current_released > prior_released:
        return True, f"released {prior_released} -> {current_released}"

    return False, "unchanged"


def download_path(
    session: requests.Session,
    url: str,
    dest: Path,
    etag: str | None,
    last_modified_http: str | None,
) -> tuple[int, dict[str, str]]:

    headers: dict[str, str] = {}
    if dest.exists() and etag:
        headers["If-None-Match"] = etag
    elif dest.exists() and last_modified_http:
        headers["If-Modified-Since"] = last_modified_http

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")

    with session.get(url, stream=True, timeout=180, headers=headers) as response:
        if response.status_code == 304:
            return 304, dict(response.headers)
        response.raise_for_status()
        with tmp.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 256):
                if chunk:
                    handle.write(chunk)
        tmp.replace(dest)
        return response.status_code, dict(response.headers)


def process_dataset(
    session: requests.Session,
    item: dict[str, Any],
    paths: dict[str, Path],
    prior: dict[str, Any] | None,
    force: bool,
) -> dict[str, Any]:
    identifier = item.get("identifier") or "unknown"
    title = item.get("title") or identifier
    dist = find_csv_distribution(item)
    if not dist or not dist.get("downloadURL"):
        return {
            "identifier": identifier,
            "title": title,
            "status": "skipped",
            "reason": "CSV error",
        }

    download_url = dist["downloadURL"]
    original_name = Path(urlparse(download_url).path).name or f"{identifier}.csv"
    dest_name = f"{identifier}__{good_filename(original_name)}"
    dest = paths["completed"] / dest_name
    raw_dest = paths["raw"] / dest_name

    needed, reason = download_qualified(item, download_url, dest, prior, force)
    result: dict[str, Any] = {
        "identifier": identifier,
        "title": title,
        "modified": item.get("modified"),
        "released": item.get("released"),
        "download_url": download_url,
        "output_file": str(dest),
        "status": "skipped",
        "reason": reason,
    }

    if not needed:
        result["snake_case_columns"] = (prior or {}).get("snake_case_columns", [])
        result["bytes"] = (prior or {}).get("bytes")
        return result

    status_code, resp_headers = download_path(
        session,
        download_url,
        raw_dest,
        etag=(prior or {}).get("etag"),
        last_modified_http=(prior or {}).get("http_last_modified"),
    )

    if status_code == 304 and dest.exists():
        result["status"] = "not_modified_http"
        result["reason"] = "HTTP 304"
        result["snake_case_columns"] = (prior or {}).get("snake_case_columns", [])
        result["bytes"] = dest.stat().st_size
        result["etag"] = (prior or {}).get("etag")
        result["http_last_modified"] = (prior or {}).get("http_last_modified")
        result["sha256"] = (prior or {}).get("sha256")
        return result

    columns = fix_header(raw_dest, dest)
    result.update(
        {
            "status": "downloaded",
            "http_status": status_code,
            "bytes": dest.stat().st_size,
            "sha256": calculate_file_sha256(dest),
            "etag": resp_headers.get("ETag") or resp_headers.get("Etag"),
            "http_last_modified": resp_headers.get("Last-Modified"),
            "snake_case_columns": columns,
            "column_count": len(columns),
        }
    )
    return result


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download CMS Provider Data Catalog datasets for a theme and normalize CSV headers to snake_case."
    )
    parser.add_argument(
        "--data-dir",
        default="./working_data",
        help="directory",
    )
    parser.add_argument(
        "--theme",
        default="Hospitals",
        help='Metastore theme to download (default: "Hospitals")',
    )
    parser.add_argument("--workers", type=int, default=8, help="Parallel download workers (default: 8)")
    parser.add_argument("--force", action="store_true", help="Re-download every matching dataset")
    parser.add_argument("--keep-raw", action="store_true", help="Keep unmodified source CSVs under data-dir/raw")
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional cap on the number of matching datasets (0 = all). Useful for a smoke test.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    data_dir = Path(args.data_dir).expanduser().resolve()
    paths = {
        "root": data_dir,
        "completed": data_dir / "completed",
        "raw": data_dir / "raw",
        "state": data_dir / "latest" / "current_state.json",
        "runs": data_dir / "latest" / "run_data.json",
    }
    paths["completed"].mkdir(parents=True, exist_ok=True)
    paths["raw"].mkdir(parents=True, exist_ok=True)

    run_started = utc_now()

    state = load_json(paths["state"], default={"datasets": {}})
    prior_map: dict[str, Any] = state.get("datasets") or {}

    session = build_session()
    try:
        datasets = fetch_datasets(session, args.theme)
        if args.limit and args.limit > 0:
            datasets = datasets[: args.limit]
    except Exception:
        return 1

    dataset_results: list[dict[str, Any]] = []

    def worker(item: dict[str, Any]) -> dict[str, Any]:
        ident = item.get("identifier") or ""
        return process_dataset(
            session=session,
            item=item,
            paths=paths,
            prior=prior_map.get(ident),
            force=args.force,
        )

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        pending = [pool.submit(worker, item) for item in datasets]
        for future in as_completed(pending):
            try:
                result = future.result()
            except Exception as exc:
                result = {"identifier": "unknown", "status": "error", "reason": str(exc)}
            dataset_results.append(result)

    if not args.keep_raw:
        for raw_file in paths["raw"].glob("*"):
            if raw_file.is_file():
                raw_file.unlink()

    new_state = {
        "updated_at": utc_now(),
        "theme": args.theme,
        "datasets": dict(prior_map),
    }
    for result in dataset_results:
        ident = result.get("identifier")
        if not ident or result.get("status") in {"error", "skipped_no_csv"}:
            continue
        existing = new_state["datasets"].get(ident, {})
        if result.get("status") in {"downloaded", "not_modified_http"}:
            existing.update(
                {
                    "identifier": ident,
                    "title": result.get("title"),
                    "modified": result.get("modified"),
                    "released": result.get("released"),
                    "download_url": result.get("download_url"),
                    "output_file": result.get("output_file"),
                    "etag": result.get("etag"),
                    "http_last_modified": result.get("http_last_modified"),
                    "sha256": result.get("sha256"),
                    "bytes": result.get("bytes"),
                    "snake_case_columns": result.get("snake_case_columns"),
                    "last_downloaded_at": (
                        utc_now()
                        if result.get("status") == "downloaded"
                        else existing.get("last_downloaded_at")
                    ),
                }
            )
            new_state["datasets"][ident] = existing
    save_json(paths["state"], new_state)

    counts: dict[str, int] = {}
    for result in dataset_results:
        counts[result.get("status", "unknown")] = counts.get(result.get("status", "unknown"), 0) + 1

    run_record = {
        "started_at": run_started,
        "finished_at": utc_now(),
        "theme": args.theme,
        "force": args.force,
        "dataset_count": len(datasets),
        "status_counts": counts,
        "results": [
            {
                "identifier": r.get("identifier"),
                "title": r.get("title"),
                "status": r.get("status"),
                "reason": r.get("reason"),
                "modified": r.get("modified"),
                "output_file": r.get("output_file"),
                "bytes": r.get("bytes"),
                "column_count": r.get("column_count") or len(r.get("snake_case_columns") or []),
            }
            for r in sorted(dataset_results, key=lambda x: (x.get("title") or "", x.get("identifier") or ""))
        ],
    }
    history = load_json(paths["runs"], default=[])
    if not isinstance(history, list):
        history = []
    history.append(run_record)
    save_json(paths["runs"], history)
    save_json(data_dir / "latest" / "last_run.json", run_record)

    print("Results")
    print("-------")
    for key, value in sorted(counts.items()):
        print(f"  {key}: {value}")
    print(f"\ncompleted CSVs : {paths['completed']}")
    print(f"metadata  : {data_dir / 'latest' / 'last_run.json'}")
    print(f"Dataset Latest  : {paths['state']}")
    return 0 if counts.get("error", 0) == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
