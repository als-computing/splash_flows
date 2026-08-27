#!/usr/bin/env python
"""
Compare files between two Globus endpoints and identify missing files.
Optionally re-submit new_file_832 Prefect flows for any missing files.

Usage:
    # Check for missing files
    python -m scripts.check_globus_endpoint_sync \
        --source spot832 \
        --source-path /raw/ALS-12717_bessire \
        --dest data832 \
        --dest-path /data/raw/ALS-12717_bessire

    # Check and recover (dry run first)
    python -m scripts.check_globus_endpoint_sync \
        --source spot832 \
        --source-path /raw/ALS-12717_bessire \
        --dest data832 \
        --dest-path /data/raw/ALS-12717_bessire \
        --recover \
        --dry-run

    # Check and recover (actually submit flows)
    python -m scripts.check_globus_endpoint_sync \
        --source spot832 \
        --source-path /raw/ALS-12717_bessire \
        --dest data832 \
        --dest-path /data/raw/ALS-12717_bessire \
        --recover \
        --output missing_ALS-12717_bessire.txt

    # Recover without NERSC transfer (e.g. export-controlled data)
    python -m scripts.check_globus_endpoint_sync \
        --source spot832 \
        --source-path /raw/ALS-12717_bessire \
        --dest data832 \
        --dest-path /data/raw/ALS-12717_bessire \
        --recover \
        --no-nersc

    # Using raw UUIDs (for endpoints not in config.yml)
    python -m scripts.check_globus_endpoint_sync \
        --source-uuid 44ae904c-ab64-4145-a8f0-7287de38324d \
        --source-path /raw/ALS-12717_bessire \
        --dest-uuid 75b478b2-37af-46df-bfbd-71ed692c6506 \
        --dest-path /data/raw/ALS-12717_bessire

    # List available endpoints
    python -m scripts.check_globus_endpoint_sync --list-endpoints

Example output:
    ============================================================
    Source: spot832 (/raw/ALS-12717_bessire)
    Destination: data832 (/data/raw/ALS-12717_bessire)
    ============================================================
    Files on source:      21
    Files on destination: 19
    Missing from dest:    2
    ============================================================

    Missing files:
      scan_003/scan_003.h5
      scan_007/scan_007.h5

    ============================================================
    Submitting 2 flow(s) to 'process-new-832-file-flow/new_file_832' ...
    ============================================================
    Submitted: 2
    Failed:    0
    ============================================================
"""
import asyncio
from dotenv import load_dotenv
import json
import logging
import os
from pathlib import Path
from typing import Optional
import uuid

import globus_sdk
import typer

from orchestration.config import get_config
from orchestration.globus.transfer import build_endpoints

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

TOKEN_FILE = Path.home() / ".globus_sync_check_tokens.json"

# Deployment name as registered in prefect.yaml
DEPLOYMENT_NAME = "new_832_file_flow/new_file_832"

# spot832 mount prefix stripped by process_new_832_file_task via split("/global")[1]
SPOT832_GLOBAL_PREFIX = "/global"


def get_transfer_client() -> globus_sdk.TransferClient:
    """Get a Globus TransferClient.

    Uses confidential client if GLOBUS_CLIENT_ID and GLOBUS_CLIENT_SECRET are set,
    otherwise uses cached tokens or prompts for browser-based login.

    Returns:
        Authenticated TransferClient.
    """
    client_id = os.getenv("GLOBUS_CLIENT_ID")
    client_secret = os.getenv("GLOBUS_CLIENT_SECRET")
    scopes = "urn:globus:auth:scope:transfer.api.globus.org:all"

    if client_id and client_secret:
        logger.info("Using confidential client credentials")
        confidential_client = globus_sdk.ConfidentialAppAuthClient(client_id, client_secret)
        authorizer = globus_sdk.ClientCredentialsAuthorizer(confidential_client, scopes)
        return globus_sdk.TransferClient(authorizer=authorizer)

    native_client_id = client_id or "61338d24-54d5-408f-a10d-66c06b59f6d2"
    client = globus_sdk.NativeAppAuthClient(native_client_id)

    if TOKEN_FILE.exists():
        try:
            with open(TOKEN_FILE) as f:
                tokens = json.load(f)
            transfer_tokens = tokens.get("transfer.api.globus.org", {})
            if transfer_tokens.get("refresh_token"):
                logger.info("Using cached tokens")
                authorizer = globus_sdk.RefreshTokenAuthorizer(
                    transfer_tokens["refresh_token"],
                    client,
                    access_token=transfer_tokens.get("access_token"),
                    expires_at=transfer_tokens.get("expires_at_seconds"),
                    on_refresh=_save_tokens,
                )
                return globus_sdk.TransferClient(authorizer=authorizer)
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Could not load cached tokens: {e}")

    logger.info("No cached tokens, using browser login")
    client.oauth2_start_flow(refresh_tokens=True, requested_scopes=scopes)
    authorize_url = client.oauth2_get_authorize_url()

    print(f"\nPlease visit this URL to authenticate:\n\n{authorize_url}\n")
    auth_code = input("Enter the authorization code: ").strip()

    token_response = client.oauth2_exchange_code_for_tokens(auth_code)
    _save_tokens(token_response)

    transfer_tokens = token_response.by_resource_server["transfer.api.globus.org"]
    authorizer = globus_sdk.RefreshTokenAuthorizer(
        transfer_tokens["refresh_token"],
        client,
        access_token=transfer_tokens["access_token"],
        expires_at=transfer_tokens["expires_at_seconds"],
        on_refresh=_save_tokens,
    )
    return globus_sdk.TransferClient(authorizer=authorizer)


def _save_tokens(token_response) -> None:
    """Save tokens to file for reuse.

    Args:
        token_response: The token response from Globus SDK.
    """
    if hasattr(token_response, "by_resource_server"):
        tokens = token_response.by_resource_server
    else:
        tokens = token_response
    with open(TOKEN_FILE, "w") as f:
        json.dump(tokens, f)
    TOKEN_FILE.chmod(0o600)


def _looks_like_uuid(val: str) -> bool:
    try:
        uuid.UUID(val)
        return True
    except ValueError:
        return False


def resolve_endpoint_uuid(name_or_uuid: str) -> str:
    """Resolve an endpoint name from config.yml to its UUID.

    If the value already looks like a UUID, it is returned as-is.

    Args:
        name_or_uuid: Endpoint name from config.yml, or a raw UUID string.

    Returns:
        Endpoint UUID string.

    Raises:
        ValueError: If the name is not found in config.yml.
    """
    if _looks_like_uuid(name_or_uuid):
        return name_or_uuid

    config = get_config()
    endpoints = build_endpoints(config)
    if name_or_uuid not in endpoints:
        available = ", ".join(sorted(endpoints.keys()))
        raise ValueError(
            f"Endpoint '{name_or_uuid}' not found in config.yml. "
            f"Available endpoints: {available}"
        )
    return endpoints[name_or_uuid].uuid


def list_files_recursive(
    tc: globus_sdk.TransferClient,
    endpoint_uuid: str,
    path: str,
    _relative_base: str = "",
) -> set[str]:
    """Recursively list all files on an endpoint, returning relative paths.

    Args:
        tc: Globus TransferClient.
        endpoint_uuid: The endpoint UUID.
        path: Absolute path on the endpoint to scan.
        _relative_base: Internal use for building relative paths.

    Returns:
        Set of relative file paths (relative to the initial ``path``).
    """
    files: set[str] = set()
    try:
        contents = tc.operation_ls(endpoint_uuid, path=path)
        for obj in contents:
            rel_path = f"{_relative_base}/{obj['name']}" if _relative_base else obj["name"]

            if obj["type"] == "file":
                files.add(rel_path)
            elif obj["type"] == "dir":
                subdir_path = f"{path.rstrip('/')}/{obj['name']}"
                files.update(
                    list_files_recursive(tc, endpoint_uuid, subdir_path, rel_path)
                )
    except globus_sdk.GlobusAPIError as err:
        logger.error(f"Error listing {path}: {err.message}")

    return files


def print_endpoints() -> None:
    """List all endpoints defined in config.yml."""
    config = get_config()
    endpoints = build_endpoints(config)

    print(f"\n{'Endpoint Name':<30} {'UUID':<40} {'Root Path'}")
    print("-" * 100)
    for name, ep in sorted(endpoints.items()):
        print(f"{name:<30} {ep.uuid:<40} {ep.root_path}")


def _build_flow_path(source_path: str, relative_file: str) -> str:
    """Build the full path expected by ``process_new_832_file_flow``.

    ``process_new_832_file_task`` strips the ``/global`` prefix via
    ``file_path.split("/global")[1]``, so paths must include it.

    Args:
        source_path: Absolute source path on spot832 (e.g. ``/raw/ALS-12717_bessire``).
        relative_file: Relative file path from the sync check
            (e.g. ``scan_003/scan_003.h5``).

    Returns:
        Path string with ``/global`` prefix
        (e.g. ``/global/raw/ALS-12717_bessire/scan_003/scan_003.h5``).
    """
    source_path = source_path.rstrip("/")
    relative_file = relative_file.lstrip("/")
    return f"{SPOT832_GLOBAL_PREFIX}{source_path}/{relative_file}"


async def _submit_batch(
    batch: list[str],
    send_to_nersc: bool,
    is_export_control: bool,
) -> list[bool]:
    """Submit a single batch of flows concurrently.

    Args:
        batch: File paths to submit in this batch.
        send_to_nersc: Whether to transfer to NERSC and ingest into SciCat.
        is_export_control: Whether to skip NERSC/SciCat for export-controlled data.

    Returns:
        List of booleans indicating success per file.
    """
    from prefect.deployments import run_deployment

    async def _submit(file_path: str) -> bool:
        parts = Path(file_path).parts
        flow_run_name = "/".join(parts[3:]) if len(parts) >= 5 else Path(file_path).name
        try:
            await run_deployment(
                name=DEPLOYMENT_NAME,
                parameters={
                    "file_path": file_path,
                    "send_to_nersc": send_to_nersc,
                    "is_export_control": is_export_control,
                },
                flow_run_name=flow_run_name,
                timeout=0,
            )
            logger.info(f"Submitted: {file_path}")
            return True
        except Exception as e:
            logger.warning(f"Failed to submit {file_path}: {type(e).__name__}: {e!r}")
            return False

    return list(await asyncio.gather(*[_submit(fp) for fp in batch]))


def _submit_all(
    flow_paths: list[str],
    send_to_nersc: bool,
    is_export_control: bool,
    concurrency: int,
    batch_wait_seconds: int,
) -> tuple[int, int]:
    """Submit all recovery flows in batches, waiting between each.

    Args:
        flow_paths: List of full file paths to submit.
        send_to_nersc: Whether to transfer to NERSC and ingest into SciCat.
        is_export_control: Whether to skip NERSC/SciCat for export-controlled data.
        concurrency: Number of flows per batch.
        batch_wait_seconds: Seconds to wait between batches.

    Returns:
        Tuple of (submitted_count, failed_count).
    """
    import time

    batches = [flow_paths[i:i + concurrency] for i in range(0, len(flow_paths), concurrency)]
    all_results: list[bool] = []

    for batch_num, batch in enumerate(batches, start=1):
        print(f"  Batch {batch_num}/{len(batches)}: submitting {len(batch)} flow(s) ...")
        results = asyncio.run(_submit_batch(batch, send_to_nersc, is_export_control))
        all_results.extend(results)

        if batch_num < len(batches):
            print(f"  Waiting {batch_wait_seconds}s before next batch ...")
            time.sleep(batch_wait_seconds)

    submitted = sum(1 for r in all_results if r)
    return submitted, len(all_results) - submitted


def main(
    source: Optional[str] = typer.Option(
        None, "--source", "-s", help="Source endpoint name from config.yml"
    ),
    source_uuid: Optional[str] = typer.Option(
        None, "--source-uuid", help="Source endpoint UUID (alternative to --source)"
    ),
    source_path: Optional[str] = typer.Option(
        None, "--source-path", help="Path on source endpoint"
    ),
    dest: Optional[str] = typer.Option(
        None, "--dest", "-d", help="Destination endpoint name from config.yml"
    ),
    dest_uuid: Optional[str] = typer.Option(
        None, "--dest-uuid", help="Destination endpoint UUID (alternative to --dest)"
    ),
    dest_path: Optional[str] = typer.Option(
        None, "--dest-path", help="Path on destination endpoint"
    ),
    output_file: Optional[str] = typer.Option(
        None, "--output", "-o", help="Write missing files to this file (one per line)"
    ),
    show_matching: bool = typer.Option(
        False, "--show-matching", "-m", help="Also print files that exist on both endpoints"
    ),
    list_endpoints: bool = typer.Option(
        False, "--list-endpoints", help="List available endpoints from config.yml and exit"
    ),
    logout: bool = typer.Option(
        False, "--logout", help="Remove cached tokens and exit"
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Show detailed logging output"
    ),
    # --- Recovery options ---
    recover: bool = typer.Option(
        False, "--recover", "-r", help="Submit new_file_832 Prefect flows for missing files"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show what would be submitted without actually submitting (requires --recover)"
    ),
    send_to_nersc: bool = typer.Option(
        True, "--nersc/--no-nersc", help="Transfer to NERSC and ingest into SciCat (default: True)"
    ),
    is_export_control: bool = typer.Option(
        False, "--export-control", help="Skip NERSC and SciCat for export-controlled data"
    ),
    concurrency: int = typer.Option(
        5, "--concurrency", "-c", min=1, max=20,
        help="Files per batch (default: 5)"
    ),
    batch_wait: int = typer.Option(
        45, "--batch-wait",
        help="Seconds to wait between batches (default: 45)"
    ),
    extensions: Optional[list[str]] = typer.Option(
        [".h5"], "--ext",
        help="Only consider files with these extensions (default: .h5). "
             "Pass multiple times for multiple types, e.g. --ext .h5 --ext .nxs"
    ),
    test_one: bool = typer.Option(
        False, "--test-one",
        help="Submit only the first missing file, to verify the flow works before running all."
    ),
) -> Optional[list[str]]:
    """Compare files between source and destination Globus endpoints.

    Reports files that exist on source but are missing from destination.
    Optionally re-submits ``new_file_832`` Prefect flows for each missing file
    via ``--recover``.

    Authentication: Uses GLOBUS_CLIENT_ID/GLOBUS_CLIENT_SECRET if both are set,
    otherwise uses cached tokens or prompts for browser login.

    Args:
        source: Source endpoint name from config.yml.
        source_uuid: Source endpoint UUID (alternative to ``--source``).
        source_path: Path on source endpoint.
        dest: Destination endpoint name from config.yml.
        dest_uuid: Destination endpoint UUID (alternative to ``--dest``).
        dest_path: Path on destination endpoint.
        output_file: Write missing file paths to this file (one per line).
        show_matching: Also print files that exist on both endpoints.
        list_endpoints: List available endpoints from config.yml and exit.
        logout: Remove cached tokens and exit.
        verbose: Show detailed logging output.
        recover: Submit ``new_file_832`` flows for each missing file.
        dry_run: Preview recovery submissions without actually submitting.
        send_to_nersc: Transfer to NERSC and ingest into SciCat.
        is_export_control: Skip NERSC/SciCat for export-controlled data.
        concurrency: Max simultaneous Prefect deployment submissions.
        extensions: Only include files whose suffix matches one of these (e.g. ``[".h5"]``).

    Returns:
        List of missing file paths, or None if listing endpoints or logging out.
    """
    log_level = logging.INFO if verbose else logging.WARNING
    logging.basicConfig(level=log_level, format="%(levelname)s: %(message)s", force=True)

    if logout:
        if TOKEN_FILE.exists():
            TOKEN_FILE.unlink()
            print("Logged out (removed cached tokens)")
        else:
            print("No cached tokens to remove")
        return None

    if list_endpoints:
        print_endpoints()
        return None

    if not source_path:
        raise typer.BadParameter("--source-path is required")
    if not dest_path:
        raise typer.BadParameter("--dest-path is required")

    if source:
        src_uuid = resolve_endpoint_uuid(source)
    elif source_uuid:
        src_uuid = source_uuid
    else:
        raise typer.BadParameter("Either --source or --source-uuid is required")

    if dest:
        dst_uuid = resolve_endpoint_uuid(dest)
    elif dest_uuid:
        dst_uuid = dest_uuid
    else:
        raise typer.BadParameter("Either --dest or --dest-uuid is required")

    tc = get_transfer_client()

    if not source_path.startswith("/"):
        source_path = "/" + source_path
    if not dest_path.startswith("/"):
        dest_path = "/" + dest_path

    logger.info(f"Scanning source: {source or src_uuid} at {source_path}")
    source_files = list_files_recursive(tc, src_uuid, source_path)
    logger.info(f"Found {len(source_files)} files on source")

    logger.info(f"Scanning destination: {dest or dst_uuid} at {dest_path}")
    dest_files = list_files_recursive(tc, dst_uuid, dest_path)
    logger.info(f"Found {len(dest_files)} files on destination")

    # Filter by extension if specified
    if extensions:
        exts = {e if e.startswith(".") else f".{e}" for e in extensions}
        source_files = {f for f in source_files if Path(f).suffix in exts}
        dest_files = {f for f in dest_files if Path(f).suffix in exts}
        logger.info(f"After extension filter {exts}: {len(source_files)} source, {len(dest_files)} dest")

    missing = sorted(source_files - dest_files)
    matching = sorted(source_files & dest_files)

    print(f"\n{'=' * 60}")
    print(f"Source: {source or src_uuid} ({source_path})")
    print(f"Destination: {dest or dst_uuid} ({dest_path})")
    print(f"{'=' * 60}")
    print(f"Files on source:      {len(source_files)}")
    print(f"Files on destination: {len(dest_files)}")
    print(f"Missing from dest:    {len(missing)}")
    print(f"{'=' * 60}")

    if show_matching and matching:
        print("\nMatching files:")
        for f in matching:
            print(f"  ✓ {f}")

    if missing:
        print("\nMissing files:")
        for f in missing:
            print(f"  {f}")

        if output_file:
            Path(output_file).write_text("\n".join(missing))
            print(f"\nWrote {len(missing)} paths to {output_file}")
    else:
        print("\n✓ All files are synced! No missing files found.")

    # --- Recovery ---
    if recover and missing:
        flow_paths = [_build_flow_path(source_path, f) for f in missing]

        if test_one:
            flow_paths = flow_paths[:1]
            print(f"\n[--test-one] Limiting to first missing file: {flow_paths[0]}")

        print(f"\n{'=' * 60}")
        if dry_run:
            print(f"[DRY RUN] Would submit {len(flow_paths)} flow(s) to '{DEPLOYMENT_NAME}'")
            print(f"  send_to_nersc={send_to_nersc}, is_export_control={is_export_control}")
            print(f"{'=' * 60}")
            for fp in flow_paths:
                print(f"  → {fp}")
        else:
            print(
                f"Submitting {len(flow_paths)} flow(s) to '{DEPLOYMENT_NAME}' "
                f"({len(flow_paths) // concurrency + (1 if len(flow_paths) % concurrency else 0)} "
                f"batch(es) of {concurrency}, {batch_wait}s wait between batches) ..."
            )
            print(f"{'=' * 60}")
            submitted, failed = _submit_all(
                flow_paths, send_to_nersc, is_export_control, concurrency, batch_wait
            )
            print(f"Submitted: {submitted}")
            if failed:
                print(f"Failed:    {failed}  (check logs above)")
            print(f"{'=' * 60}")

    return missing


if __name__ == "__main__":
    typer.run(main)
