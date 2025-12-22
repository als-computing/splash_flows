"""
Prefect Server Status Checker

Checks the health and run status of multiple self-hosted Prefect servers,
displaying deployment-level summaries with failure rates and recent failure times.

Environment Variables:
    PREFECT_API_KEY     API key for splash_auth servers (BL7011, BL832)
    KC_USERNAME         Keycloak username for keycloak-protected servers
    KC_PASSWORD         Keycloak password for keycloak-protected servers
These can be set in a .env file in the same directory.

Usage:
    python prefect_status.py                # Last 24 hours (default)
    python prefect_status.py --hours 168     # Last 7 days
    python prefect_status.py -H 72          # Last 3 days
"""

import argparse
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from enum import Enum
import httpx
import os


load_dotenv()


# ANSI color codes
class Color:
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    DIM = "\033[2m"
    BOLD = "\033[1m"
    RESET = "\033[0m"


class AuthType(Enum):
    """
    Enumeration of authentication types.
    """
    SPLASH_AUTH = "splash_auth"
    KEYCLOAK = "keycloak"


class PrefectServer(Enum):
    """
    Enumeration of Prefect servers with their URLs and auth types.
    """
    dichroism = ("https://flow-dichroism.als.lbl.gov", AuthType.KEYCLOAK)
    BL7011 = ("https://flow-xpcs.als.lbl.gov", AuthType.SPLASH_AUTH)
    BL733 = ("https://flow-733.als.lbl.gov", AuthType.KEYCLOAK)
    BL832 = ("https://flow-prd.als.lbl.gov", AuthType.SPLASH_AUTH)
    BL931 = ("https://flow-931.als.lbl.gov", AuthType.KEYCLOAK)

    @property
    def url(self) -> str:
        """
        Get the URL of the server.

        :return: Server URL.
        """
        return self.value[0]

    @property
    def auth_type(self) -> AuthType:
        """
        Get the authentication type for the server.

        :return: AuthType of the server.
        """
        return self.value[1]


class StateType(Enum):
    """
    Enumeration of possible run states.
    """
    SCHEDULED = "SCHEDULED"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    CANCELLING = "CANCELLING"
    CRASHED = "CRASHED"
    PAUSED = "PAUSED"


BAD_STATES = {StateType.FAILED, StateType.CRASHED, StateType.CANCELLED}


@dataclass
class DeploymentSummary:
    """
    Summary of a deployment's run statuses.

    :param name: Name of the deployment.
    :param counts: Dictionary mapping StateType to count of runs.
    :param last_failure_time: Datetime of the last failure, if any.
    """
    name: str
    counts: dict[StateType, int] = field(default_factory=dict)
    last_failure_time: datetime | None = None

    @property
    def total(self) -> int:
        """
        Total number of runs.

        :return: Total run count.
        """
        return sum(self.counts.values())

    @property
    def failure_count(self) -> int:
        """
        Number of failed runs.
        :return: Count of failed runs.
        """
        return sum(self.counts.get(s, 0) for s in BAD_STATES)

    @property
    def healthy(self) -> bool:
        """
        Whether the deployment is healthy (no failures).
        :return: True if healthy, False otherwise.
        """
        return self.failure_count == 0

    def format_time_ago(self, dt: datetime) -> str:
        """
        Format a datetime as 'X hours/days ago'.

        :param dt: Datetime to format.
        :return: Formatted string.
        """
        delta = datetime.now(timezone.utc) - dt
        hours = delta.total_seconds() / 3600
        if hours < 1:
            minutes = int(delta.total_seconds() / 60)
            return f"{minutes}m ago"
        elif hours < 24:
            return f"{int(hours)}h ago"
        else:
            days = int(hours / 24)
            return f"{days}d ago"

    def __str__(self) -> str:
        """
        String representation of the deployment summary.

        :return: Formatted string.
        """
        if self.total == 0:
            return f"{self.name}: {Color.DIM}no runs{Color.RESET}"

        parts = []
        for state, count in self.counts.items():
            if count > 0:
                if state in BAD_STATES:
                    pct = count / self.total * 100
                    parts.append(f"{Color.RED}{state.value.lower()}: {count} ({pct:.1f}%){Color.RESET}")
                elif state == StateType.COMPLETED:
                    parts.append(f"{Color.GREEN}{state.value.lower()}: {count}{Color.RESET}")
                elif state == StateType.RUNNING:
                    parts.append(f"{Color.BLUE}{state.value.lower()}: {count}{Color.RESET}")
                elif state == StateType.SCHEDULED:
                    parts.append(f"{Color.DIM}{state.value.lower()}: {count}{Color.RESET}")
                else:
                    parts.append(f"{state.value.lower()}: {count}")

        result = f"{self.name}: {', '.join(parts)}"

        if self.last_failure_time:
            result += f" {Color.DIM}(last failure: {self.format_time_ago(self.last_failure_time)}){Color.RESET}"

        return result


@dataclass
class ServerSummary:
    """
    Summary of a Prefect server's health and deployment statuses.

    :param server: PrefectServer instance.
    :param healthy: Whether the server is considered healthy.
    :param reachable: Whether the server is reachable.
    :param auth_ok: Whether authentication is successful.
    :param deployments: List of DeploymentSummary instances.
    """
    server: PrefectServer
    healthy: bool
    reachable: bool
    auth_ok: bool
    deployments: list[DeploymentSummary] = field(default_factory=list)
    error: str | None = None

    @property
    def total_runs(self) -> int:
        """
        Total number of runs across all deployments.

        :return: Total run count.
        """
        return sum(d.total for d in self.deployments)

    @property
    def total_failures(self) -> int:
        """
        Total number of failed runs across all deployments.

        :return: Total failure count.
        """
        return sum(d.failure_count for d in self.deployments)


def get_keycloak_token() -> str:
    """
    Fetch a fresh Keycloak access token.

    :return: Access token string.
    """
    resp = httpx.post(
        "https://comp-auth.als.lbl.gov/realms/als-computing/protocol/openid-connect/token",
        data={
            "client_id": "als-computing-api",
            "grant_type": "password",
            "username": os.environ["KC_USERNAME"],
            "password": os.environ["KC_PASSWORD"],
            "scope": "openid email profile",
        },
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def get_client(server: PrefectServer) -> httpx.Client:
    """
    Create an HTTP client with the appropriate auth for the server.

    :param server: PrefectServer to connect to.
    :return: Configured httpx.Client.
    """
    if server.auth_type == AuthType.SPLASH_AUTH:
        token = os.environ.get("PREFECT_API_KEY")
        if not token:
            raise ValueError("PREFECT_API_KEY not set in environment")
    else:
        token = get_keycloak_token()

    return httpx.Client(
        headers={"Authorization": f"Bearer {token}"},
        timeout=10,
    )


def format_hours(hours: int) -> str:
    """
    Format hours as a human-readable string.

    :param hours: Number of hours.
    :return: Formatted string.
    """
    if hours < 24:
        return f"last {hours}h"
    elif hours % 24 == 0:
        days = hours // 24
        return f"last {days}d"
    else:
        days = hours / 24
        return f"last {days:.1f}d"


def check_server_health(server: PrefectServer) -> tuple[bool, bool]:
    """
    Check if a Prefect server is reachable. Returns (reachable, auth_ok).

    :param server: PrefectServer to check.
    :return: Tuple indicating if the server is reachable and if auth is OK.
    """
    try:
        with get_client(server) as client:
            resp = client.get(f"{server.url}/api/health", follow_redirects=False)

        if resp.status_code == 200:
            return True, True
        elif resp.status_code in (302, 307):
            return True, False
        else:
            return False, False
    except httpx.RequestError:
        return False, False


def get_deployment_summaries(server: PrefectServer, hours: int = 24) -> list[DeploymentSummary]:
    """
    Get run summary per deployment for the last N hours.

    :param server: PrefectServer to query.
    :param hours: Number of hours to look back.
    :return: List of DeploymentSummary objects.
    """
    since = datetime.now(timezone.utc) - timedelta(hours=hours)

    with get_client(server) as client:
        resp = client.post(f"{server.url}/api/deployments/filter", json={"limit": 200})
        resp.raise_for_status()
        deployments = resp.json()

        # Paginate through all flow runs with progress
        all_runs = []
        offset = 0
        print(f"  {Color.DIM}fetching runs...{Color.RESET}", end="", flush=True)
        while True:
            resp = client.post(
                f"{server.url}/api/flow_runs/filter",
                json={
                    "flow_runs": {
                        "start_time": {"after_": since.isoformat()}
                    },
                    "limit": 200,
                    "offset": offset,
                },
            )
            resp.raise_for_status()
            batch = resp.json()
            if not batch:
                break
            all_runs.extend(batch)
            print(f"\r  {Color.DIM}fetching runs... {len(all_runs)}{Color.RESET}", end="", flush=True)
            if len(batch) < 200:
                break
            offset += 200
        print(f"\r  {Color.DIM}fetched {len(all_runs)} runs{Color.RESET}      ")

    # Group runs by deployment
    runs_by_deployment: dict[str, list[dict]] = {}
    for run in all_runs:
        dep_id = run.get("deployment_id")
        if dep_id:
            runs_by_deployment.setdefault(dep_id, []).append(run)

    summaries = []
    for dep in deployments:
        dep_id = dep["id"]
        dep_name = dep["name"]
        runs = runs_by_deployment.get(dep_id, [])

        counts = {}
        last_failure_time = None

        for state_type in StateType:
            matching_runs = [r for r in runs if r["state_type"] == state_type.value]
            count = len(matching_runs)
            if count > 0:
                counts[state_type] = count

                # Track most recent failure time
                if state_type in BAD_STATES:
                    for r in matching_runs:
                        end_time = r.get("end_time") or r.get("start_time")
                        if end_time:
                            try:
                                dt = datetime.fromisoformat(end_time)
                                if last_failure_time is None or dt > last_failure_time:
                                    last_failure_time = dt
                            except (ValueError, TypeError):
                                pass

        summary = DeploymentSummary(dep_name, counts, last_failure_time)
        summaries.append(summary)

    # Sort: unhealthy first, then by failure count descending
    summaries.sort(key=lambda s: (-int(not s.healthy), -s.failure_count, s.name))

    return summaries


def get_server_summary(server: PrefectServer, hours: int = 24) -> ServerSummary:
    """
    Get full summary for a server.

    :param server: PrefectServer to check.
    :param hours: Number of hours to look back for run data.
    :return: ServerSummary object.
    """
    print(f"\n{Color.BOLD}{server.name}{Color.RESET} ({server.url}) [{format_hours(hours)}]")
    print("-" * 50)

    reachable, auth_ok = check_server_health(server)

    if not reachable:
        print(f"  {Color.RED}✗ unreachable{Color.RESET}")
        return ServerSummary(server, healthy=False, reachable=False, auth_ok=False)

    if not auth_ok:
        print(f"  {Color.YELLOW}⚠ auth rejected{Color.RESET}")
        return ServerSummary(server, healthy=False, reachable=True, auth_ok=False)

    try:
        deployments = get_deployment_summaries(server, hours)

        # Server summary line
        total_runs = sum(d.total for d in deployments)
        total_failures = sum(d.failure_count for d in deployments)
        unhealthy_count = sum(1 for d in deployments if not d.healthy)

        if total_failures > 0:
            failure_pct = total_failures / total_runs * 100 if total_runs > 0 else 0
            print(f"  {Color.RED}{total_runs} runs, {total_failures} failures ({failure_pct:.1f}%), \
                  {unhealthy_count} unhealthy deployments{Color.RESET}")
        else:
            print(f"  {Color.GREEN}{total_runs} runs, all healthy{Color.RESET}")

        # Per-deployment breakdown
        for summary in deployments:
            status = f"{Color.GREEN}✓{Color.RESET}" if summary.healthy else f"{Color.RED}✗{Color.RESET}"
            print(f"  {status} {summary}")

        healthy = all(d.healthy for d in deployments)
        return ServerSummary(server, healthy=healthy, reachable=True, auth_ok=True, deployments=deployments)

    except httpx.HTTPStatusError as e:
        error_msg = f"{e.response.status_code} - {e.response.text[:100]}"
        print(f"  {Color.RED}✗ Error: {error_msg}{Color.RESET}")
        return ServerSummary(server, healthy=False, reachable=True, auth_ok=True, error=error_msg)


def get_all_servers_summary(hours: int = 24) -> dict[PrefectServer, ServerSummary]:
    """
    Get summaries for all configured Prefect servers.

    :param hours: Number of hours to look back for run data.
    :return: Dictionary mapping PrefectServer to ServerSummary.
    """
    summaries = {}
    for server in PrefectServer:
        summaries[server] = get_server_summary(server, hours)

    # Overall summary
    print(f"\n{'=' * 50}")
    print(f"{Color.BOLD}OVERALL SUMMARY{Color.RESET}")
    print("=" * 50)

    total_servers = len(summaries)
    healthy_servers = sum(1 for s in summaries.values() if s.healthy)
    unreachable = sum(1 for s in summaries.values() if not s.reachable)
    auth_issues = sum(1 for s in summaries.values() if s.reachable and not s.auth_ok)

    total_runs = sum(s.total_runs for s in summaries.values())
    total_failures = sum(s.total_failures for s in summaries.values())
    failure_pct = total_failures / total_runs * 100 if total_runs > 0 else 0

    # Server status
    status_parts = [f"{healthy_servers}/{total_servers} servers healthy"]
    if unreachable:
        status_parts.append(f"{Color.RED}{unreachable} unreachable{Color.RESET}")
    if auth_issues:
        status_parts.append(f"{Color.YELLOW}{auth_issues} auth issues{Color.RESET}")
    print(", ".join(status_parts))

    # Run stats
    if total_runs > 0:
        if total_failures > 0:
            print(f"{total_runs:,} total runs, {Color.RED}{total_failures:,} failures ({failure_pct:.1f}%){Color.RESET}")
        else:
            print(f"{Color.GREEN}{total_runs:,} total runs, all successful{Color.RESET}")

    return summaries


def main():
    parser = argparse.ArgumentParser(description="Check Prefect server status")
    parser.add_argument(
        "--hours", "-H",
        type=int,
        default=24,
        help="Number of hours to look back (default: 24, i.e. 1 day)"
    )
    args = parser.parse_args()

    get_all_servers_summary(hours=args.hours)


if __name__ == "__main__":
    main()
