#!/usr/bin/env python3
"""
siberguvenlik.gov.tr blocklist fetcher.

Pulls the full address index from the public API, keeps a local database of all
records (with their original dates), and regenerates aged blocklists on every run:

    full-domains.txt / full-ips.txt
    days-30-domains.txt / days-30-ips.txt
    days-60-domains.txt / days-60-ips.txt
    days-90-domains.txt / days-90-ips.txt
    days-120-domains.txt / days-120-ips.txt

Behaviour
---------
* First run  (no full lists yet)  -> wipe state and do a FULL crawl of every page,
  waiting MIN_DELAY..MAX_DELAY seconds between pages so the API is not hammered.
* The full crawl is RESUMABLE and TIME-BUDGETED. If TIME_BUDGET_SECONDS is hit
  before the crawl finishes, progress is checkpointed and the process exits with
  code 10 (== "run me again to continue").
* Once the full crawl is complete, every run does a fast INCREMENTAL update:
  only the newest pages are fetched until a record we already have is reached.
  Lists are then regenerated so that, e.g., an entry that turns 31 days old drops
  out of days-30 but is still present in days-60 / days-90 / days-120 / full.

The lists are *derived* from the database + the current clock on every run, so
ageing happens automatically without ever losing an entry from the wider windows.
"""

from __future__ import annotations

import json
import os
import random
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# --------------------------------------------------------------------------- #
# Configuration (all overridable via environment variables)
# --------------------------------------------------------------------------- #
API_URL = os.environ.get(
    "API_URL", "https://siberguvenlik.gov.tr/api/address/index"
)

# Ordinary modern browser User-Agent.
USER_AGENT = os.environ.get(
    "USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
)

DATA_DIR = Path(os.environ.get("DATA_DIR", "data"))

# Delays between page requests during the (large) FULL crawl.
MIN_DELAY = float(os.environ.get("MIN_DELAY", "10"))
MAX_DELAY = float(os.environ.get("MAX_DELAY", "50"))

# Delays during the small INCREMENTAL update (only a few pages).
INC_MIN_DELAY = float(os.environ.get("INC_MIN_DELAY", "3"))
INC_MAX_DELAY = float(os.environ.get("INC_MAX_DELAY", "10"))

# Stop and checkpoint the full crawl after this many seconds (0 = unlimited).
TIME_BUDGET_SECONDS = float(os.environ.get("TIME_BUDGET_SECONDS", "3300"))

# Safety cap for the incremental update so it can never run away.
INCREMENTAL_MAX_PAGES = int(os.environ.get("INCREMENTAL_MAX_PAGES", "200"))

# Ageing windows (days).
WINDOWS = [30, 60, 90, 120]

REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "30"))

# Files
DB_FILE = DATA_DIR / "database.jsonl"     # one JSON record per line, sorted by id
STATE_FILE = DATA_DIR / "_state.json"     # crawl metadata
FULL_DOMAINS = DATA_DIR / "full-domains.txt"
FULL_IPS = DATA_DIR / "full-ips.txt"

EXIT_OK = 0          # nothing more to do until next scheduled run
EXIT_CONTINUE = 10   # full crawl still in progress -> run again immediately


# --------------------------------------------------------------------------- #
# HTTP
# --------------------------------------------------------------------------- #
def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    return s


def fetch_page(session: requests.Session, page: int) -> dict:
    """Fetch a single page. `page` is 1-indexed as the API expects."""
    resp = session.get(
        API_URL, params={"page": page}, timeout=REQUEST_TIMEOUT
    )
    resp.raise_for_status()
    return resp.json()


# --------------------------------------------------------------------------- #
# Database (id -> record)
# --------------------------------------------------------------------------- #
def load_db() -> dict[int, dict]:
    db: dict[int, dict] = {}
    if not DB_FILE.exists():
        return db
    with DB_FILE.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                db[int(rec["id"])] = rec
            except (ValueError, KeyError):
                continue
    return db


def save_db(db: dict[int, dict]) -> None:
    tmp = DB_FILE.with_suffix(".jsonl.tmp")
    with tmp.open("w", encoding="utf-8", newline="\n") as fh:
        for _id in sorted(db.keys()):
            rec = db[_id]
            fh.write(
                json.dumps(
                    {
                        "id": rec["id"],
                        "url": rec["url"],
                        "type": rec.get("type", "domain"),
                        "date": rec.get("date", ""),
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
    tmp.replace(DB_FILE)


def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except ValueError:
            pass
    return {
        "full_crawl_complete": False,
        "next_page": 1,
        "total_pages": None,
        "last_run": None,
        "last_max_id": None,
    }


def save_state(state: dict) -> None:
    tmp = STATE_FILE.with_suffix(".json.tmp")
    tmp.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
        newline="\n",
    )
    tmp.replace(STATE_FILE)


def store_records(db: dict[int, dict], models: list[dict]) -> int:
    """Insert/update records, return how many were brand new."""
    new = 0
    for m in models:
        try:
            _id = int(m["id"])
        except (KeyError, ValueError, TypeError):
            continue
        if _id not in db:
            new += 1
        db[_id] = {
            "id": _id,
            "url": m.get("url", ""),
            "type": m.get("type", "domain"),
            "date": m.get("date", ""),
        }
    return new


# --------------------------------------------------------------------------- #
# List generation
# --------------------------------------------------------------------------- #
def parse_date(value: str) -> datetime | None:
    if not value:
        return None
    value = value.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def write_list(path: Path, entries: set[str], is_ip: bool) -> None:
    def ip_key(s: str):
        parts = s.split(".")
        try:
            return tuple(int(p) for p in parts)
        except ValueError:
            return (s,)

    ordered = sorted(entries, key=ip_key if is_ip else str.lower)
    tmp = path.with_suffix(path.suffix + ".tmp")
    # newline="\n" forces clean LF endings on every platform (no CRLF on Windows)
    # so firewalls reading the lists get one bare domain/IP per line.
    tmp.write_text(
        "\n".join(ordered) + ("\n" if ordered else ""),
        encoding="utf-8",
        newline="\n",
    )
    tmp.replace(path)


def generate_lists(db: dict[int, dict]) -> dict[str, int]:
    """Regenerate every output list from the database using the current clock."""
    now = datetime.now()
    cutoffs = {w: now - timedelta(days=w) for w in WINDOWS}

    full_dom: set[str] = set()
    full_ip: set[str] = set()
    win_dom: dict[int, set[str]] = {w: set() for w in WINDOWS}
    win_ip: dict[int, set[str]] = {w: set() for w in WINDOWS}

    for rec in db.values():
        url = (rec.get("url") or "").strip()
        if not url:
            continue
        is_ip = rec.get("type") == "ip"
        (full_ip if is_ip else full_dom).add(url)

        dt = parse_date(rec.get("date", ""))
        if dt is None:
            continue
        for w, cutoff in cutoffs.items():
            if dt >= cutoff:
                (win_ip if is_ip else win_dom)[w].add(url)

    write_list(FULL_DOMAINS, full_dom, is_ip=False)
    write_list(FULL_IPS, full_ip, is_ip=True)
    for w in WINDOWS:
        write_list(DATA_DIR / f"days-{w}-domains.txt", win_dom[w], is_ip=False)
        write_list(DATA_DIR / f"days-{w}-ips.txt", win_ip[w], is_ip=True)

    stats = {"full_domains": len(full_dom), "full_ips": len(full_ip)}
    for w in WINDOWS:
        stats[f"days-{w}-domains"] = len(win_dom[w])
        stats[f"days-{w}-ips"] = len(win_ip[w])
    return stats


# --------------------------------------------------------------------------- #
# First-run detection / reset
# --------------------------------------------------------------------------- #
def file_empty(path: Path) -> bool:
    return (not path.exists()) or path.stat().st_size == 0


def is_first_run() -> bool:
    """First run == we have produced no full lists yet (per the spec)."""
    return file_empty(FULL_DOMAINS) and file_empty(FULL_IPS)


def reset_everything() -> None:
    print("[reset] full lists are empty -> wiping state and re-crawling from scratch")
    for p in list(DATA_DIR.glob("*.txt")) + [DB_FILE, STATE_FILE]:
        try:
            p.unlink()
        except FileNotFoundError:
            pass


# --------------------------------------------------------------------------- #
# Crawl modes
# --------------------------------------------------------------------------- #
def sleep_between(lo: float, hi: float) -> None:
    delay = random.uniform(lo, hi)
    print(f"      sleeping {delay:.1f}s", flush=True)
    time.sleep(delay)


def run_full_crawl(session, db, state) -> int:
    start = time.monotonic()
    page = int(state.get("next_page") or 1)
    print(f"[full] resuming full crawl at page {page}")

    while True:
        if TIME_BUDGET_SECONDS and (time.monotonic() - start) >= TIME_BUDGET_SECONDS:
            print("[full] time budget reached -> checkpointing")
            state["next_page"] = page
            save_db(db)
            save_state(state)
            generate_lists(db)
            return EXIT_CONTINUE

        try:
            data = fetch_page(session, page)
        except requests.RequestException as exc:
            print(f"[full] page {page} failed: {exc} -> checkpoint & retry next run")
            state["next_page"] = page
            save_db(db)
            save_state(state)
            generate_lists(db)
            return EXIT_CONTINUE

        total_pages = data.get("pageCount")
        if total_pages:
            state["total_pages"] = total_pages
        models = data.get("models", [])
        new = store_records(db, models)
        print(
            f"[full] page {page}/{state.get('total_pages')} "
            f"records={len(models)} new={new} db={len(db)}",
            flush=True,
        )

        # End of data: empty page or we passed the last page.
        if not models or (state.get("total_pages") and page >= state["total_pages"]):
            state["full_crawl_complete"] = True
            state["next_page"] = 1
            if db:
                state["last_max_id"] = max(db.keys())
            save_db(db)
            save_state(state)
            stats = generate_lists(db)
            print(f"[full] crawl COMPLETE. lists: {stats}")
            return EXIT_OK

        page += 1
        # periodic checkpoint so a crash never loses more than ~25 pages
        if page % 25 == 0:
            state["next_page"] = page
            save_db(db)
            save_state(state)

        sleep_between(MIN_DELAY, MAX_DELAY)


def run_incremental(session, db, state) -> int:
    print("[incr] incremental update (newest pages only)")
    known_max = state.get("last_max_id") or (max(db.keys()) if db else 0)
    total_new = 0
    page = 1

    while page <= INCREMENTAL_MAX_PAGES:
        try:
            data = fetch_page(session, page)
        except requests.RequestException as exc:
            print(f"[incr] page {page} failed: {exc} -> stopping, will retry next run")
            break

        total_pages = data.get("pageCount")
        if total_pages:
            state["total_pages"] = total_pages
        models = data.get("models", [])
        if not models:
            break

        new = store_records(db, models)
        total_new += new
        page_min_id = min(int(m["id"]) for m in models if "id" in m)
        print(f"[incr] page {page} new={new} (min id={page_min_id}, known_max={known_max})")

        # Records are newest-first; once this page is entirely at/under what we
        # already had, there is nothing newer beyond it.
        if page_min_id <= known_max:
            break
        page += 1
        sleep_between(INC_MIN_DELAY, INC_MAX_DELAY)

    if db:
        state["last_max_id"] = max(db.keys())
    save_db(db)
    save_state(state)
    stats = generate_lists(db)
    print(f"[incr] done. {total_new} new records. lists: {stats}")
    return EXIT_OK


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main() -> int:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if is_first_run():
        reset_everything()

    db = load_db()
    state = load_state()
    state["last_run"] = datetime.now().isoformat(timespec="seconds")

    session = make_session()

    if not state.get("full_crawl_complete"):
        code = run_full_crawl(session, db, state)
    else:
        code = run_incremental(session, db, state)

    return code


if __name__ == "__main__":
    sys.exit(main())
