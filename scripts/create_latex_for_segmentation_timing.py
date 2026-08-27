"""
Scrape flow-prd timing data for the LaTeX table.

Targets:
  - nersc_forge_recon_multisegment_flow
  - alcf_forge_recon_multisegment_flow
  - new_file_832

Timing sources per label:

  Source = "log"  → actual HPC wall-clock time from job output re-logged by controller
  Source = "task" → Prefect @task run duration (includes overhead: queue wait, staging, etc.)

  Data Transfers (NERSC)   task  transfer_data_to_nersc @task in new_832_file_flow
                           log   timestamp delta between 'Copying raw data to NERSC' and
                                 'Transfer to NERSC' log lines in nersc_forge_recon_multisegment_flow
  Data Transfers (ALCF)    log   timestamp delta between
                                 'Transferring raw data to ALCF: ...' and
                                 'Transfer to ALCF successful.' in alcf_forge_recon_multisegment_flow
  Data Transfers (NERSC)   log   timestamp delta between NERSC transfer start/end lines
                                 in nersc_forge_recon_multisegment_flow (confirm with --dump-transfer-logs)
  Reconstruction (NERSC)   log   "  RECONSTRUCTION:      Ns  <-- actual recon time"
  SAM3 (NERSC)             log   "  Total time: Xm Ys (Zs)"  from _fetch_seg_timing_from_output
                           task  nersc_segmentation_task (fallback if log not matched)
  DINOv3 (NERSC)           task  nersc_segmentation_dino_task  (no HPC timing logged)
  Combine (NERSC)          task  nersc_combine_segmentations_task  (no HPC timing logged)
  Reconstruction (ALCF)    log   "Total duration of the reconstruction task: N.NN seconds."
  SAM3 (ALCF)              log   "Total duration of the segmentation task: N.NN seconds."
  DINOv3 (ALCF)            log   "Total duration of the segmentation_dino task: N.NN seconds."
  Combine (ALCF)           log   "Total duration of the combine_segmentations task: N.NN seconds."

Usage:
    python scrape_flow_prd_timing.py --after 2026-02-24 --before 2026-02-26 --latex
    python scrape_flow_prd_timing.py --check
"""

import argparse
import os
import re
import statistics
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import httpx
from dotenv import load_dotenv

load_dotenv()

SERVER_URL = "https://flow-prd.als.lbl.gov"

RECON_DEPLOYMENTS = {
    "nersc_forge_recon_multisegment_flow",
    "alcf_forge_recon_multisegment_flow",
}
TRANSFER_DEPLOYMENTS: set = set()  # unused; kept for compat

# Dispatcher runs process_new_832_file_task inline (not via run_deployment),
# so NERSC transfer log brackets appear in dispatcher flow run logs.
DISPATCHER_DEPLOYMENTS = {
    "run_832_dispatcher",
}

TABLE_ROWS = [
    "Data Transfers (NERSC)",
    "Data Transfers (ALCF)",
    "Reconstruction (NERSC)",
    "SAM3 (NERSC)",
    "DINOv3 (NERSC)",
    "Combine (NERSC)",
    "Reconstruction (ALCF)",
    "SAM3 (ALCF)",
    "DINOv3 (ALCF)",
    "Combine (ALCF)",
]

TABLE_METADATA = {
    # facility, resources, task_type
    "Data Transfers (NERSC)":  ("NERSC", "ESNet/Globus",  "Transfer"),
    "Data Transfers (ALCF)":   ("ALCF",  "ESNet/Globus",  "Transfer"),
    "Reconstruction (NERSC)":  ("NERSC", "16 CPU Nodes",  "Compute"),
    "SAM3 (NERSC)":            ("NERSC", "42 GPU Nodes",  "Compute"),
    "DINOv3 (NERSC)":          ("NERSC", "8 GPU Nodes",   "Compute"),
    "Combine (NERSC)":         ("NERSC", "8 CPU Nodes",   "Compute"),
    "Reconstruction (ALCF)":   ("ALCF",  "8 CPU Nodes",   "Compute"),
    "SAM3 (ALCF)":             ("ALCF",  "4 GPU Nodes",   "Compute"),
    "DINOv3 (ALCF)":           ("ALCF",  "4 GPU Nodes",   "Compute"),
    "Combine (ALCF)":          ("ALCF",  "2 CPU Nodes",   "Compute"),
}

# Grouped layout for the LaTeX table.
# Each entry: (display_name, task_type, [row_label, ...])
# Groups with 2 rows get \multirow{2}{*}{name}; single-row groups get plain text.
TABLE_GROUPS = [
    ("Data Transfers", "Transfer", [
        "Data Transfers (NERSC)",
        "Data Transfers (ALCF)",
    ]),
    ("Reconstruction",  "Compute", [
        "Reconstruction (NERSC)",
        "Reconstruction (ALCF)",
    ]),
    ("SAM3",            "Compute", [
        "SAM3 (NERSC)",
        "SAM3 (ALCF)",
    ]),
    ("DINOv3",          "Compute", [
        "DINOv3 (NERSC)",
        "DINOv3 (ALCF)",
    ]),
    ("Combine",         "Compute", [
        "Combine (NERSC)",
        "Combine (ALCF)",
    ]),
]

# ---------------------------------------------------------------------------
# Log patterns — yield seconds via named group "secs"
#
# NERSC (nersc_hpc_controller.py):
#   Reconstruction:
#     logger.info(f"  RECONSTRUCTION:      {timing.get('reconstruction','N/A')}s  <-- actual recon time")
#   SAM3:
#     _fetch_seg_timing_from_output() re-logs SLURM stdout verbatim:
#     logger.info(f"  {line}")  where line = "Total time: 5m 23s (323s)"
#     Raw seconds are in the parentheses.
#   DINO / Combine: no HPC timing logged → fall back to Prefect @task duration.
#
# ALCF (alcf_hpc_controller.py):
#   All four tasks use _wait_for_globus_compute_future(future, task_name, ...):
#     logger.info(f"Total duration of the {task_name} task: {elapsed_time:.2f} seconds.")
#   task_name: "reconstruction" | "segmentation" | "segmentation_dino" | "combine_segmentations"
# ---------------------------------------------------------------------------

LOG_PATTERNS: dict[str, re.Pattern] = {
    "Reconstruction (NERSC)": re.compile(
        r"RECONSTRUCTION:\s+(?P<secs>[\d.]+)s\s+<-- actual recon time"
    ),
    "SAM3 (NERSC)": re.compile(
        r"Total time:\s+\d+m\s+\d+s\s+\((?P<secs>\d+)s\)"
    ),
    "Reconstruction (ALCF)": re.compile(
        r"Total duration of the reconstruction task:\s+(?P<secs>[\d.]+)\s+seconds\."
    ),
    "SAM3 (ALCF)": re.compile(
        r"Total duration of the segmentation task:\s+(?P<secs>[\d.]+)\s+seconds\."
    ),
    "DINOv3 (ALCF)": re.compile(
        r"Total duration of the segmentation_dino task:\s+(?P<secs>[\d.]+)\s+seconds\."
    ),
    "Combine (ALCF)": re.compile(
        r"Total duration of the combine_segmentations task:\s+(?P<secs>[\d.]+)\s+seconds\."
    ),
}

TRANSFER_BRACKET_PATTERNS: dict[str, tuple[re.Pattern, re.Pattern]] = {
    # alcf_forge_recon_multisegment_flow (confirmed from log dump):
    #   start: logger.info(f"Transferring raw data to ALCF: {data832_raw_path}")
    #   end:   logger.info("Transfer to ALCF successful.")
    "Data Transfers (ALCF)": (
        re.compile(r"Transferring raw data to ALCF", re.IGNORECASE),
        re.compile(r"Transfer to ALCF successful", re.IGNORECASE),
    ),
    # run_832_dispatcher → process_new_832_file_task → transfer_data_to_nersc (move.py):
    #   start: logger.info(f"Transferring {file_path} from data832 to nersc")
    #   end:   logger.info(f"File successfully transferred from data832 to NERSC ...")
    "Data Transfers (NERSC)": (
        re.compile(r"Transferring .* from data832 to nersc", re.IGNORECASE),
        re.compile(r"File successfully transferred from data832 to NERSC", re.IGNORECASE),
    ),
}

# For these labels, prefer log-extracted HPC time over Prefect @task wall time.
LOG_PREFERRED = set(LOG_PATTERNS.keys())

# ---------------------------------------------------------------------------
# Prefect @task classification
# transfer_spot_to_data  → spot storage → ALCF  (alcf_forge_recon_multisegment_flow STEP 1)
# transfer_data_to_nersc → data832      → NERSC (nersc_forge_recon_multisegment_flow)
# ---------------------------------------------------------------------------
TASK_LABEL_MAP = [
    ("nersc_segmentation_dino_task", "DINOv3 (NERSC)"),
    ("nersc_segmentation_task",      "SAM3 (NERSC)"),
    ("nersc_combine_segmentations",  "Combine (NERSC)"),
    ("alcf_segmentation_dino_task",  "DINOv3 (ALCF)"),
    ("alcf_segmentation_task",       "SAM3 (ALCF)"),
    ("transfer_data_to_nersc",       "Data Transfers (NERSC)"),
]

RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
DIM = "\033[2m"
BOLD = "\033[1m"
RESET = "\033[0m"


# ── Data types ─────────────────────────────────────────────────────────────────

@dataclass
class TimingStats:
    label: str
    durations: list[float] = field(default_factory=list)
    source: str = "task"  # "task" | "log" | "mixed"

    @property
    def count(self): return len(self.durations)

    @property
    def mean_minutes(self): return statistics.mean(self.durations) / 60 if self.durations else None

    @property
    def median_minutes(self): return statistics.median(self.durations) / 60 if self.durations else None

    @property
    def stdev_minutes(self): return statistics.stdev(self.durations) / 60 if len(self.durations) > 1 else None

    @property
    def min_minutes(self): return min(self.durations) / 60 if self.durations else None

    @property
    def max_minutes(self): return max(self.durations) / 60 if self.durations else None


# ── HTTP client ────────────────────────────────────────────────────────────────

def get_client() -> httpx.Client:
    token = os.environ.get("PREFECT_API_KEY")
    if not token:
        raise SystemExit("PREFECT_API_KEY not set.")
    return httpx.Client(
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
        follow_redirects=True,
    )


def check_connectivity(client: httpx.Client) -> None:
    resp = client.get(f"{SERVER_URL}/api/health")
    print(f"  {GREEN}✓ health: {resp.status_code}{RESET}")
    resp = client.get(f"{SERVER_URL}/api/me")
    if resp.status_code == 200:
        print(f"  {GREEN}✓ auth OK (user: {resp.json().get('name', 'unknown')}){RESET}")
    elif resp.status_code == 404:
        resp2 = client.post(f"{SERVER_URL}/api/deployments/filter", json={"limit": 1})
        if resp2.status_code != 200:
            raise SystemExit(f"Auth failed: {resp2.status_code}")
        print(f"  {GREEN}✓ auth OK{RESET}")
    else:
        raise SystemExit(f"Auth failed: {resp.status_code} {resp.text[:200]}")


# ── Paginated API fetchers ─────────────────────────────────────────────────────

def get_deployments(client: httpx.Client, target_names: set) -> list:
    """
    Fetch deployments whose name matches any entry in target_names.

    Prefect deployment names can take several forms:
      - exact flow name:          "new_832_file_flow"
      - flow/deployment:          "new_832_file_flow/default"
      - arbitrary deployment name set by the operator

    We match if the deployment name exactly equals a target OR contains a target
    as a substring (handles "flow_name/deployment_name" format).
    All deployment names are printed when any target is not found so the user can
    identify the correct name to put in TRANSFER_DEPLOYMENTS / RECON_DEPLOYMENTS.
    """
    resp = client.post(f"{SERVER_URL}/api/deployments/filter", json={"limit": 200})
    resp.raise_for_status()
    all_deps = resp.json()

    def _matches(dep_name: str) -> bool:
        return any(
            t == dep_name or t in dep_name or dep_name in t
            for t in target_names
        )

    matched = [d for d in all_deps if _matches(d["name"])]
    missing = target_names - {t for d in matched for t in target_names if t in d["name"] or t == d["name"]}

    if missing:
        print(f"  {YELLOW}⚠ deployments not found for: {missing}{RESET}")
        print(f"  {DIM}  All deployment names on this server:{RESET}")
        for d in sorted(all_deps, key=lambda x: x["name"]):
            print(f"  {DIM}    {d['name']!r}{RESET}")
    return matched


def get_flow_runs(
    client: httpx.Client,
    deployment_ids: list,
    after: datetime,
    before: datetime,
    states: list[str] | None = None,
) -> list:
    """
    Fetch COMPLETED flow runs for the given deployments and date range.
    State filter is applied at the API level so failed/cancelled runs are
    never fetched, avoiding wasted task_run and log API calls.
    Pass states=[] to retrieve all states (e.g. for diagnostics).
    """
    if states is None:
        states = ["COMPLETED"]

    all_runs, offset = [], 0
    while True:
        flow_filter: dict = {
            "start_time": {
                "after_":  after.isoformat(),
                "before_": before.isoformat(),
            },
            "deployment_id": {"any_": deployment_ids},
        }
        if states:
            flow_filter["state"] = {"type": {"any_": states}}

        resp = client.post(
            f"{SERVER_URL}/api/flow_runs/filter",
            json={"flow_runs": flow_filter, "limit": 200, "offset": offset},
        )
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        all_runs.extend(batch)
        print(f"  {DIM}fetched {len(all_runs)} flow runs...{RESET}", end="\r", flush=True)
        if len(batch) < 200:
            break
        offset += 200
    print(f"  {DIM}fetched {len(all_runs)} flow runs{RESET}                    ")
    return all_runs


def get_task_runs(client: httpx.Client, flow_run_id: str) -> list:
    all_trs, offset = [], 0
    while True:
        resp = client.post(
            f"{SERVER_URL}/api/task_runs/filter",
            json={
                "flow_runs": {"id": {"any_": [flow_run_id]}},
                "limit": 200,
                "offset": offset,
            },
        )
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        all_trs.extend(batch)
        if len(batch) < 200:
            break
        offset += 200
    return all_trs


def get_logs_for_flow_run(client: httpx.Client, flow_run_id: str) -> list[dict]:
    """
    Fetch all log entries for a flow run via POST /api/logs/filter.
    Returns a list of {"message": str, "timestamp": str} dicts.
    Captures output from both @task functions AND inline flow code.
    Returns [] if the endpoint is unavailable.
    """
    all_entries, offset = [], 0
    while True:
        resp = client.post(
            f"{SERVER_URL}/api/logs/filter",
            json={
                "logs": {"flow_run_id": {"any_": [flow_run_id]}},
                "limit": 200,
                "offset": offset,
            },
        )
        if resp.status_code != 200:
            return all_entries
        batch = resp.json()
        if not batch:
            break
        all_entries.extend(
            {"message": e.get("message", ""), "timestamp": e.get("timestamp", "")}
            for e in batch
        )
        if len(batch) < 200:
            break
        offset += 200
    return all_entries


# ── Helpers ────────────────────────────────────────────────────────────────────

def dur_sec(obj: dict) -> Optional[float]:
    trt = obj.get("total_run_time")
    if trt and trt > 0:
        return float(trt)
    start, end = obj.get("start_time"), obj.get("end_time")
    if not start or not end:
        return None
    try:
        return max(0.0, (
            datetime.fromisoformat(end) - datetime.fromisoformat(start)
        ).total_seconds())
    except (ValueError, TypeError):
        return None


def classify_task(name: str) -> Optional[str]:
    n = name.lower()
    for key, label in TASK_LABEL_MAP:
        if key in n:
            return label
    return None


def strip_hash(name: str) -> str:
    """Remove Prefect's 3-char hex suffix (e.g. 'my_task-a3f' -> 'my_task')."""
    parts = name.rsplit("-", 1)
    if len(parts) == 2 and len(parts[1]) == 3 and all(
        c in "0123456789abcdef" for c in parts[1]
    ):
        return parts[0]
    return name


def extract_log_timings(entries: list[dict]) -> dict[str, float]:
    """
    Scan all log entries for known HPC timing patterns.
    Returns {label: seconds}. Takes the FIRST match per label.
    """
    found: dict[str, float] = {}
    for entry in entries:
        msg = entry["message"]
        for label, pattern in LOG_PATTERNS.items():
            if label not in found:
                m = pattern.search(msg)
                if m:
                    found[label] = float(m.group("secs"))
    return found


def _parse_ts(ts: str) -> Optional[datetime]:
    """Parse an ISO timestamp string; return None on failure."""
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def extract_transfer_timings(
    entries: list[dict],
    debug: bool = False,
) -> dict[str, float]:
    """
    Compute Globus transfer duration by bracketing start/end log messages
    using their timestamps. Works on recon flow logs where transfer_controller.copy()
    is called inline (no @task decorator, so no Prefect task run timing available).

    Entries must have "message" and "timestamp" keys (ISO string from Prefect logs API).
    Returns {label: seconds} for any transfer brackets successfully matched.

    Set debug=True to print every entry considered for each label (for pattern tuning).
    """
    found: dict[str, float] = {}

    # Check whether any entry has a usable timestamp at all
    ts_available = any(_parse_ts(e.get("timestamp", "")) is not None for e in entries)

    for label, (start_pat, end_pat) in TRANSFER_BRACKET_PATTERNS.items():
        t_start: Optional[datetime] = None
        for entry in entries:
            msg = entry["message"]
            ts = entry.get("timestamp", "")
            if debug:
                print(f"    [{label}] ts={ts[:19]}  msg={msg[:80]}")
            if t_start is None and start_pat.search(msg):
                t_start = _parse_ts(ts)
                if debug:
                    print(f"    ^^ START matched (ts_parsed={t_start})")
                if not ts_available:
                    # No timestamps in this log stream — can't bracket
                    break
            elif t_start is not None and end_pat.search(msg):
                t_end = _parse_ts(ts)
                if debug:
                    print(f"    ^^ END matched (ts_parsed={t_end})")
                if t_end is not None and t_start is not None:
                    delta = (t_end - t_start).total_seconds()
                    if delta > 0:
                        found[label] = delta
                break

    if not ts_available and entries:
        # Surface this once so the user knows why transfer timing is missing
        found["_no_timestamps"] = 0.0  # sentinel, filtered out by caller

    return found


# ── Main scraping ──────────────────────────────────────────────────────────────

def scrape_timing(after: datetime, before: datetime) -> dict[str, TimingStats]:
    stats: dict[str, TimingStats] = {label: TimingStats(label) for label in TABLE_ROWS}
    log_hits:   defaultdict[str, int] = defaultdict(int)
    log_misses: defaultdict[str, int] = defaultdict(int)
    logs_api_available: Optional[bool] = None

    with get_client() as client:
        print(f"\n{BOLD}Connecting to {SERVER_URL}{RESET}")
        check_connectivity(client)

        print(f"\n{BOLD}Fetching target deployments...{RESET}")
        all_deps = get_deployments(client, RECON_DEPLOYMENTS | DISPATCHER_DEPLOYMENTS)
        dep_name_map = {d["id"]: d["name"] for d in all_deps}
        recon_dep_ids = [d["id"] for d in all_deps if d["name"] in RECON_DEPLOYMENTS]

        # ── Recon / segmentation flows ─────────────────────────────────────────
        if recon_dep_ids:
            print(f"\nFetching recon/seg flow runs {after.date()} → {before.date()}...")
            flow_runs = get_flow_runs(client, recon_dep_ids, after=after, before=before)
            print(f"  {GREEN}{len(flow_runs)} COMPLETED runs{RESET}")

            for i, fr in enumerate(flow_runs):
                frid = fr["id"]
                dep_name = dep_name_map.get(fr.get("deployment_id", ""), "unknown")
                is_nersc = "nersc" in dep_name
                is_alcf = "alcf" in dep_name
                start = (fr.get("start_time") or "")[:16]
                total = fr.get("total_run_time", 0) or 0
                print(
                    f"  [{i+1}/{len(flow_runs)}] {DIM}{dep_name} | {start} | "
                    f"{total/60:.1f}min{RESET}    ",
                    end="\r", flush=True,
                )

                # ── Step 1: flow logs → actual HPC wall-clock times ────────────
                entries = get_logs_for_flow_run(client, frid)
                if logs_api_available is None:
                    logs_api_available = bool(entries)

                log_timings = extract_log_timings(entries) if entries else {}
                transfer_timings = extract_transfer_timings(entries) if entries else {}

                for label, secs in log_timings.items():
                    stats[label].durations.append(secs)
                    stats[label].source = "log"
                    log_hits[label] += 1

                no_ts = transfer_timings.pop("_no_timestamps", None)
                if no_ts is not None and logs_api_available:
                    print(f"  {YELLOW}⚠ Log entries have no timestamps — transfer bracket timing unavailable{RESET}")
                for label, secs in transfer_timings.items():
                    stats[label].durations.append(secs)
                    stats[label].source = "log"

                for label in LOG_PATTERNS:
                    if label not in log_timings:
                        if is_nersc and "NERSC" in label:
                            log_misses[label] += 1
                        elif is_alcf and "ALCF" in label:
                            log_misses[label] += 1

                # Track ALCF transfer bracket hits/misses (NERSC is @task-based)
                for label in TRANSFER_BRACKET_PATTERNS:
                    if label not in transfer_timings:
                        if is_alcf and "ALCF" in label:
                            log_misses[label] += 1
                    else:
                        log_hits[label] += 1

                # ── Step 2: Prefect @task durations ───────────────────────────
                for tr in get_task_runs(client, frid):
                    if tr.get("state_type") != "COMPLETED":
                        continue
                    tr_name = tr.get("name") or tr.get("task_key") or ""
                    label = classify_task(tr_name)
                    if not label:
                        continue
                    if label in LOG_PREFERRED and label in log_timings:
                        continue  # already have cleaner HPC-level timing
                    d = dur_sec(tr)
                    if d and d > 0:
                        stats[label].durations.append(d)
                        if stats[label].source == "log":
                            stats[label].source = "mixed"

            print(f"\n  Done.{' ' * 60}")

            avail_str = (
                "logs API available" if logs_api_available
                else "logs API unavailable — all timing from Prefect task runs"
                if logs_api_available is False
                else "no flows processed"
            )
            print(f"\n  {BOLD}Log-based HPC timing extraction ({avail_str}):{RESET}")
            for label in list(LOG_PATTERNS) + list(TRANSFER_BRACKET_PATTERNS):  # NERSC transfer via @task
                hits = log_hits.get(label, 0)
                misses = log_misses.get(label, 0)
                total_relevant = hits + misses
                if total_relevant == 0:
                    print(f"    {DIM}  {label:<34} (no relevant flow runs){RESET}")
                elif hits > 0:
                    pct = 100 * hits // total_relevant
                    print(f"    {GREEN}✓{RESET} {label:<34} {hits}/{total_relevant} runs ({pct}%)")
                else:
                    print(f"    {YELLOW}✗{RESET} {label:<34} pattern not matched in {misses} runs")

        # ── Dispatcher flows (run_832_dispatcher) ──────────────────────────────
        # process_new_832_file_task is called directly inside the dispatcher flow
        # (not via run_deployment), so its logs appear in the dispatcher flow run.
        # NERSC transfer timing extracted via log brackets:
        #   start: f"Transferring {file_path} from data832 to nersc"
        #   end:   f"File successfully transferred from data832 to NERSC ..."
        dispatch_dep_ids = [d["id"] for d in all_deps if d["name"] in DISPATCHER_DEPLOYMENTS]
        if dispatch_dep_ids:
            print(f"\nFetching dispatcher flow runs {after.date()} → {before.date()}...")
            flow_runs = get_flow_runs(client, dispatch_dep_ids, after=after, before=before)
            print(f"  {GREEN}{len(flow_runs)} COMPLETED runs{RESET}")

            nersc_transfer_hits = 0
            for i, fr in enumerate(flow_runs):
                frid = fr["id"]
                print(f"  [{i+1}/{len(flow_runs)}] {DIM}run_832_dispatcher{RESET}    ",
                      end="\r", flush=True)

                entries = get_logs_for_flow_run(client, frid)
                transfer_timings = extract_transfer_timings(entries) if entries else {}
                transfer_timings.pop("_no_timestamps", None)
                for label, secs in transfer_timings.items():
                    if secs > 0:
                        stats[label].durations.append(secs)
                        stats[label].source = "log"
                        if "NERSC" in label:
                            nersc_transfer_hits += 1

            n_disp = len(flow_runs)
            hit_str = f"{GREEN}✓{RESET}" if nersc_transfer_hits == n_disp else f"{YELLOW}✗{RESET}"
            print(f"\n  {hit_str} Data Transfers (NERSC) log bracket  {nersc_transfer_hits}/{n_disp} runs")
            print(f"\n  Done.{' ' * 60}")

    return stats


# ── Output ─────────────────────────────────────────────────────────────────────

SOURCE_TAG = {
    "log":   f"{DIM}[HPC log]{RESET}",
    "task":  f"{DIM}[Prefect task]{RESET}",
    "mixed": f"{YELLOW}[mixed log+task]{RESET}",
}


def print_summary(stats: dict[str, TimingStats]) -> None:
    print(f"\n{'=' * 80}")
    print(f"{BOLD}TIMING SUMMARY{RESET}")
    print("=" * 80)
    for label in TABLE_ROWS:
        s = stats[label]
        if s.count == 0:
            print(f"  {DIM}{label:<34} no data{RESET}")
        else:
            stdev_str = f"  σ={s.stdev_minutes:.1f}" if s.stdev_minutes else ""
            src_tag = SOURCE_TAG.get(s.source, "")
            print(
                f"  {BOLD}{label:<34}{RESET}"
                f"  mean={s.mean_minutes:.1f}min"
                f"  median={s.median_minutes:.1f}min"
                f"  [{s.min_minutes:.1f}–{s.max_minutes:.1f}]"
                f"{stdev_str}  n={s.count}  {src_tag}"
            )


def print_latex_table(stats: dict[str, TimingStats]) -> None:
    """
    Emit a grouped LaTeX table: tasks as \\multirow blocks, facility as sub-rows.
    Requires \\usepackage{multirow} in the preamble.
    """

    def time_cell(label: str) -> str:
        s = stats[label]
        if s.count == 0:
            return "--"
        t = f"{s.mean_minutes:.1f}"
        if s.stdev_minutes is not None:
            t += f" $\\pm$ {s.stdev_minutes:.1f}"
        t += f" (n={s.count})"
        return t

    lines = [
        "",
        r"% ── LaTeX Table ──────────────────────────────────────────────────────",
        r"\begin{table}[h]",
        r"\centering",
        r"\begin{tabular}{lllll}",
        r"\hline",
        r"\textbf{Task} & \textbf{Facility} & \textbf{Resources} & \textbf{Type} & \textbf{Time (min)} \\",
        r"\hline",
    ]

    for group_name, task_type, row_labels in TABLE_GROUPS:
        n = len(row_labels)
        for idx, label in enumerate(row_labels):
            facility, resources, _ = TABLE_METADATA[label]
            tc = time_cell(label)

            if n == 1:
                task_col = group_name
            elif idx == 0:
                # First row: emit \multirow on its own line, continuation indented
                lines.append(f"\\multirow{{{n}}}{{*}}{{{group_name}}}")
                task_col = "               "
            else:
                task_col = "               "

            lines.append(
                f"    {task_col}"
                f" & {facility:<6}"
                f" & {resources:<13}"
                f" & {task_type:<8}"
                f" & {tc} \\\\"
            )

        lines.append(r"\hline")

    lines += [
        r"\end{tabular}",
        (
            r"\caption{Summary of computational tasks, resources, and wall-clock execution times"
            r" (mean $\pm$ std). Each GPU node has 4 NVIDIA A100 GPUs."
            r" CPU nodes on ALCF Polaris have a single 64 core AMD EPYC ``Milan'' processor,"
            r" and CPU nodes on NERSC Perlmutter have two 64 core AMD EPYC 7763 processors.}"
        ),
        r"\label{tab:TimingResults}",
        r"\end{table}",
    ]

    print("\n".join(lines))


def _dump_transfer_logs(after: datetime, before: datetime) -> None:
    """
    Print log messages from the first available recon flow run.
    Use this to identify the exact phrasing of transfer start/end lines
    so TRANSFER_BRACKET_PATTERNS can be tuned if needed.
    """
    with get_client() as client:
        all_deps = get_deployments(client, RECON_DEPLOYMENTS)
        dep_ids = [d["id"] for d in all_deps]
        if not dep_ids:
            print("No recon deployments found.")
            return
        runs = get_flow_runs(client, dep_ids, after=after, before=before)
        if not runs:
            print("No completed runs found in date range.")
            return
        fr = runs[0]
        dep_name = fr.get("deployment_id", "?")
        print(f"\nDumping logs for flow run {fr['id']} ({dep_name[:60]}):")
        entries = get_logs_for_flow_run(client, fr["id"])
        if not entries:
            print("  No log entries returned (API may not support /api/logs/filter).")
            return
        transfer_keywords = re.compile(
            r"(transfer|copy|globus|ALCF|NERSC|raw|scratch)", re.IGNORECASE
        )
        hits = [e for e in entries if transfer_keywords.search(e["message"])]

        # Also run bracket extractor in debug mode
        print("\n  Running bracket extractor in debug mode:")
        extract_transfer_timings(entries[:200], debug=True)
        print(f"  {len(entries)} total entries, {len(hits)} match transfer keywords:\n")
        for e in hits[:60]:
            ts = e["timestamp"][:19]
            msg = e["message"][:120]
            print(f"  {ts}  {msg}")


def parse_date(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape flow-prd forge recon/segment/transfer timing"
    )
    parser.add_argument("--after",  "-a", default="2026-02-24",
                        help="Start date inclusive (YYYY-MM-DD)")
    parser.add_argument("--before", "-b", default="2026-02-26",
                        help="End date exclusive (YYYY-MM-DD)")
    parser.add_argument("--latex",  "-l", action="store_true",
                        help="Print populated LaTeX table")
    parser.add_argument("--check",  "-c", action="store_true",
                        help="Connectivity check only")
    parser.add_argument("--dump-transfer-logs", action="store_true",
                        help="Print first 40 log messages from one recon flow run "
                             "(use to diagnose transfer bracket pattern mismatches)")
    args = parser.parse_args()

    key = os.environ.get("PREFECT_API_KEY")
    if key:
        print(f"{DIM}PREFECT_API_KEY: {len(key)} chars, starts '{key[:6]}...'{RESET}")
    else:
        raise SystemExit(f"{RED}PREFECT_API_KEY not set in environment or .env{RESET}")

    if args.check:
        with get_client() as client:
            print(f"\n{BOLD}Checking {SERVER_URL}{RESET}")
            check_connectivity(client)
        return

    if args.dump_transfer_logs:
        _dump_transfer_logs(
            after=parse_date(args.after),
            before=parse_date(args.before),
        )
        return

    stats = scrape_timing(
        after=parse_date(args.after),
        before=parse_date(args.before),
    )
    print_summary(stats)
    if args.latex:
        print_latex_table(stats)
    else:
        print(f"\n{DIM}Tip: add --latex to print the populated LaTeX table.{RESET}")


if __name__ == "__main__":
    main()
