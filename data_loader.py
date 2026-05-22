"""
data_loader.py — Real-World Task Payload Dataset for SyncFlow
=============================================================

SyncFlow is a distributed microservices platform — it processes tasks,
not domain-specific sensor data. This loader provides real-world benchmark
workloads to stress-test and demonstrate the system under realistic load.

DATASET 1 — GitHub Archive (GH Archive) — Real GitHub event stream
  Source  : https://www.gharchive.org/
  Format  : NDJSON (one JSON event per line), gzipped hourly
  License : Public Domain
  Size    : ~50GB/day (we use a small slice: ~100K events)
  Why     : Real software engineering events (push, PR, issue, release)
            become SyncFlow tasks — demonstrates real heterogeneous workloads.

DATASET 2 — CommonCrawl WAT Metadata
  Source  : https://commoncrawl.org/the-data/get-started/
  Format  : JSON metadata records
  License : Public Domain
  Why     : Real web crawl metadata tasks — content-type, status codes,
            URL patterns — realistic async processing workload.

DATASET 3 — Wikimedia Recent Changes Stream (Live SSE)
  Source  : https://stream.wikimedia.org/v2/stream/recentchange
  Format  : Server-Sent Events (SSE), JSON payload
  License : CC BY-SA
  Why     : Live stream of Wikipedia edits — each edit becomes a
            SyncFlow task, demonstrating real-time ingestion throughput.

AUTO-DOWNLOAD: GH Archive slice is fetched automatically (~5MB sample).
"""

from __future__ import annotations

import gzip
import io
import json
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, List

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ── GitHub Archive ─────────────────────────────────────────────────────────────

def download_gharchive_hour(year: int = 2024, month: int = 1,
                              day: int = 1, hour: int = 0) -> Path:
    """
    Download one hour of GitHub Archive events (~3–8 MB compressed).
    Full archive: https://www.gharchive.org/
    """
    fname  = f"{year}-{month:02d}-{day:02d}-{hour}.json.gz"
    url    = f"https://data.gharchive.org/{fname}"
    target = DATA_DIR / fname

    if target.exists():
        print(f"[GHArchive] Already downloaded: {fname}")
        return target

    print(f"[GHArchive] Downloading {url} …")
    try:
        urllib.request.urlretrieve(url, target)
        print(f"[GHArchive] Saved to {target}  ({target.stat().st_size // 1024} KB)")
    except Exception as e:
        raise RuntimeError(f"Download failed: {e}\nManual: wget {url} -O {target}")
    return target


def iter_gharchive(path: Path, max_events: int = 5000) -> Iterator[dict]:
    """Yield parsed GitHub events from a .json.gz archive file."""
    count = 0
    with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as f:
        for line in f:
            if count >= max_events:
                break
            try:
                yield json.loads(line)
                count += 1
            except json.JSONDecodeError:
                continue


def gharchive_to_tasks(events_iter: Iterator[dict]) -> List[dict]:
    """
    Convert GitHub Archive events to SyncFlow task payloads.

    Event types → Task types:
      PushEvent         → git_index
      PullRequestEvent  → code_review
      IssuesEvent       → ticket_triage
      ReleaseEvent      → deploy_notify
      WatchEvent        → analytics_ingest
    """
    TYPE_MAP = {
        "PushEvent":       "git_index",
        "PullRequestEvent":"code_review",
        "IssuesEvent":     "ticket_triage",
        "ReleaseEvent":    "deploy_notify",
        "WatchEvent":      "analytics_ingest",
    }
    tasks = []
    for event in events_iter:
        task_type = TYPE_MAP.get(event.get("type"), "generic_event")
        tasks.append({
            "type":    task_type,
            "payload": {
                "event_id":   event.get("id"),
                "actor":      event.get("actor", {}).get("login"),
                "repo":       event.get("repo", {}).get("name"),
                "event_type": event.get("type"),
                "created_at": event.get("created_at"),
            },
        })
    return tasks


def load_gharchive_tasks(max_events: int = 2000,
                          year: int = 2024, month: int = 1,
                          day: int = 1, hour: int = 0) -> List[dict]:
    """
    Download and convert GitHub Archive events to SyncFlow tasks.
    Returns list of task dicts ready to POST to /tasks endpoint.
    """
    path  = download_gharchive_hour(year, month, day, hour)
    events = list(iter_gharchive(path, max_events))
    tasks  = gharchive_to_tasks(iter(events))
    print(f"[GHArchive] {len(tasks)} tasks from {len(events)} events")

    # Show task type distribution
    from collections import Counter
    dist = Counter(t["type"] for t in tasks)
    for task_type, count in dist.most_common():
        print(f"  {task_type:25s}: {count:5d}")
    return tasks


# ── Wikimedia Live Stream ──────────────────────────────────────────────────────

def stream_wikimedia_tasks(max_events: int = 100) -> List[dict]:
    """
    Fetch real Wikipedia recent-change events from Wikimedia SSE stream.
    Each edit becomes a SyncFlow 'wiki_index' task.

    Stream: https://stream.wikimedia.org/v2/stream/recentchange
    """
    import urllib.request
    url = "https://stream.wikimedia.org/v2/stream/recentchange"
    tasks = []
    print(f"[Wikimedia] Streaming up to {max_events} recent changes …")
    try:
        req = urllib.request.Request(url, headers={"Accept": "text/event-stream"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            for line in resp:
                if len(tasks) >= max_events:
                    break
                line = line.decode("utf-8").strip()
                if not line.startswith("data:"):
                    continue
                try:
                    data = json.loads(line[5:].strip())
                    tasks.append({
                        "type": "wiki_index",
                        "payload": {
                            "title":    data.get("title"),
                            "wiki":     data.get("wiki"),
                            "user":     data.get("user"),
                            "revision": data.get("revision", {}),
                            "timestamp":data.get("timestamp"),
                        },
                    })
                except Exception:
                    continue
    except Exception as e:
        print(f"[Wikimedia] Stream error: {e}")
    print(f"[Wikimedia] Collected {len(tasks)} wiki_index tasks")
    return tasks


# ── Unified Loader ─────────────────────────────────────────────────────────────

def load_benchmark_tasks(source: str = "gharchive",
                          max_tasks: int = 1000) -> List[dict]:
    """
    Load real-world benchmark tasks for SyncFlow load testing.

    Args:
        source    : "gharchive" | "wikimedia"
        max_tasks : maximum tasks to load

    Returns:
        List of task dicts → POST each to http://localhost:8080/tasks
    """
    if source == "gharchive":
        return load_gharchive_tasks(max_events=max_tasks)
    elif source == "wikimedia":
        return stream_wikimedia_tasks(max_events=max_tasks)
    else:
        raise ValueError(f"Unknown source '{source}'. Choose: gharchive | wikimedia")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="SyncFlow Real-World Task Loader")
    parser.add_argument("--source", default="gharchive",
                        choices=["gharchive", "wikimedia"],
                        help="Dataset source")
    parser.add_argument("--max",    type=int, default=500,
                        help="Max tasks to load")
    parser.add_argument("--push",   action="store_true",
                        help="POST tasks to running SyncFlow gateway")
    parser.add_argument("--api",    default="http://localhost:8080",
                        help="SyncFlow gateway URL")
    args = parser.parse_args()

    tasks = load_benchmark_tasks(source=args.source, max_tasks=args.max)
    print(f"\nLoaded {len(tasks)} tasks.")

    if args.push:
        import requests
        print(f"\nPOSTing to {args.api}/tasks …")
        ok = 0
        for task in tasks:
            try:
                r = requests.post(f"{args.api}/tasks", json=task, timeout=2)
                if r.status_code == 202:
                    ok += 1
            except Exception:
                pass
        print(f"  Enqueued {ok}/{len(tasks)} tasks successfully.")
