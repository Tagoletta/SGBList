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

# Re-crawl the whole index every N days to detect entries the source has REMOVED
# (delisted). Removals can only be found by a full pass, never by the incremental
# update. Set to 0 to disable periodic re-syncs.
FULL_RESYNC_DAYS = float(os.environ.get("FULL_RESYNC_DAYS", "7"))

# Self-heal guard: if the crawl is flagged "complete" but the database holds less
# than this fraction of the source's reported totalCount, the seed clearly didn't
# really finish, so restart the full crawl instead of doing an incremental update.
SEED_COMPLETE_FRACTION = float(os.environ.get("SEED_COMPLETE_FRACTION", "0.8"))

# Ageing windows (days).
WINDOWS = [30, 60, 90, 120]

REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "30"))

# Files
DB_FILE = DATA_DIR / "database.jsonl"     # one JSON record per line, sorted by id
STATE_FILE = DATA_DIR / "_state.json"     # crawl metadata
FULL_DOMAINS = DATA_DIR / "full-domains.txt"
FULL_IPS = DATA_DIR / "full-ips.txt"
REMOVED_LOG = DATA_DIR / "removed.log"   # append-only log of source delistings

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
            out = {
                "id": rec["id"],
                "url": rec["url"],
                "type": rec.get("type", "domain"),
                "date": rec.get("date", ""),
            }
            # "p" = the full-crawl pass in which we last saw this record at the
            # source; used to detect removals. Omitted when unknown (legacy rows).
            if rec.get("p") is not None:
                out["p"] = rec["p"]
            fh.write(json.dumps(out, ensure_ascii=False) + "\n")
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
        "pass_id": 1,
        "last_full_completed": None,
    }


def save_state(state: dict) -> None:
    tmp = STATE_FILE.with_suffix(".json.tmp")
    tmp.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
        newline="\n",
    )
    tmp.replace(STATE_FILE)


def store_records(db: dict[int, dict], models: list[dict], pass_id: int) -> int:
    """Insert/update records, stamping each with the current pass_id (so we can
    later tell which records the source still lists). Returns how many were new."""
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
            "p": pass_id,
        }
    return new


def log_removals(records: list[dict]) -> None:
    """Append delisted entries to the removals log (one tab-separated line each)."""
    ts = datetime.now().isoformat(timespec="seconds")
    lines = [
        f"{ts}\tREMOVED\t{rec.get('url', '')}\ttype={rec.get('type', '')}\t"
        f"id={rec.get('id')}\tadded={rec.get('date', '')}"
        for rec in records
    ]
    with REMOVED_LOG.open("a", encoding="utf-8", newline="\n") as fh:
        fh.write("\n".join(lines) + "\n")


def sweep_removed(db: dict[int, dict], pass_id: int) -> int:
    """After a full pass, drop records not seen this pass (i.e. removed at the
    source), logging each one. Records with an unknown pass are left untouched."""
    stale = [
        rec
        for rec in db.values()
        if rec.get("p") is not None and int(rec["p"]) < pass_id
    ]
    if not stale:
        return 0
    log_removals(stale)
    for rec in stale:
        db.pop(int(rec["id"]), None)
    print(f"[removed] {len(stale)} delisted entries dropped -> {REMOVED_LOG.name}")
    return len(stale)


def full_resync_due(state: dict) -> bool:
    """True if it's time for a periodic full re-crawl to catch source removals."""
    if FULL_RESYNC_DAYS <= 0:
        return False
    last = state.get("last_full_completed")
    if not last:
        return False
    try:
        last_dt = datetime.fromisoformat(last)
    except ValueError:
        return False
    return datetime.now() - last_dt >= timedelta(days=FULL_RESYNC_DAYS)


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
    pass_id = int(state["pass_id"])
    print(f"[full] resuming full crawl (pass {pass_id}) at page {page}")

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
        if data.get("totalCount"):
            state["total_count"] = data["totalCount"]
        models = data.get("models", [])

        total_pages_known = state.get("total_pages")
        reached_end = bool(total_pages_known) and page >= total_pages_known

        # An empty page BEFORE the real last page is a transient API hiccup, NOT
        # the end of the data. Treating it as the end is what truncated an early
        # seed — so checkpoint and retry this page on the next run instead.
        if not models and total_pages_known and not reached_end:
            print(
                f"[full] page {page} returned 0 records mid-crawl "
                f"(of {total_pages_known}) -> checkpoint & retry next run"
            )
            state["next_page"] = page
            save_db(db)
            save_state(state)
            generate_lists(db)
            return EXIT_CONTINUE

        new = store_records(db, models, pass_id)
        print(
            f"[full] page {page}/{state.get('total_pages')} "
            f"records={len(models)} new={new} db={len(db)}",
            flush=True,
        )

        # Real end of data: reached the last page (or empty with no page count).
        if reached_end or not models:
            removed = sweep_removed(db, pass_id)
            state["full_crawl_complete"] = True
            state["next_page"] = 1
            state["last_full_completed"] = datetime.now().isoformat(timespec="seconds")
            if db:
                state["last_max_id"] = max(db.keys())
            save_db(db)
            save_state(state)
            stats = generate_lists(db)
            print(
                f"[full] crawl COMPLETE (pass {pass_id}). "
                f"removed {removed} delisted. lists: {stats}"
            )
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
        total_count = data.get("totalCount")
        if total_count:
            state["total_count"] = total_count

        # Self-heal: if a previous "completed" seed was actually truncated, the
        # database will be far smaller than the source -> redo the full crawl.
        if (
            page == 1
            and total_count
            and len(db) < SEED_COMPLETE_FRACTION * total_count
        ):
            print(
                f"[incr] db has {len(db)} records but source reports {total_count}"
                f" -> seed was incomplete, switching to a full (re)crawl"
            )
            state["full_crawl_complete"] = False
            state["next_page"] = 1
            return run_full_crawl(session, db, state)

        models = data.get("models", [])
        if not models:
            break

        new = store_records(db, models, int(state["pass_id"]))
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
    if not state.get("pass_id"):
        state["pass_id"] = 1
    state["last_run"] = datetime.now().isoformat(timespec="seconds")

    session = make_session()

    if not state.get("full_crawl_complete"):
        # (Re)seed still in progress — keep crawling the whole index.
        code = run_full_crawl(session, db, state)
    elif full_resync_due(state):
        # Periodic full re-crawl so we notice entries removed at the source.
        state["pass_id"] = int(state["pass_id"]) + 1
        state["full_crawl_complete"] = False
        state["next_page"] = 1
        print(
            f"[resync] {FULL_RESYNC_DAYS}d since last full crawl -> "
            f"starting full pass {state['pass_id']}"
        )
        code = run_full_crawl(session, db, state)
    else:
        code = run_incremental(session, db, state)

    return code


if __name__ == "__main__":
    sys.exit(main())
