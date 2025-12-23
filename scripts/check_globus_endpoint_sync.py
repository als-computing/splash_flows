#!/usr/bin/env python
"""
Compare files between two Globus endpoints and identify missing files.

This script lists files on a source and destination endpoint, then reports
which files exist on source but not on destination.

Usage:
    # Using endpoint names from config.yml
    python -m scripts.sync_check_globus \
        --source spot832 \
        --source-path /raw/2024/01 \
        --dest data832 \
        --dest-path /raw/2024/01

    # Using raw UUIDs (for endpoints not in config.yml)
    python -m scripts.sync_check_globus \
        --source-uuid abc123-def456 \
        --source-path /raw/2024/01 \
        --dest-uuid xyz789-... \
        --dest-path /raw/2024/01

    # Save missing files to a file
    python -m scripts.sync_check_globus \
        --source spot832 --source-path /raw/2024/01 \
        --dest data832 --dest-path /raw/2024/01 \
        --output missing_files.txt

    # List available endpoints
    python -m scripts.sync_check_globus --list-endpoints

 Example:
 python -m scripts.check_globus_endpoint_sync \
    --source spot832 --source-path raw/_bls-00739_parkinson \
    --dest data832_raw --dest-path data/raw/_bls-00739_parkinson

============================================================
Source: spot832 (/raw/_bls-00739_parkinson)
Destination: data832_raw (/data/raw/_bls-00739_parkinson)
============================================================
Files on source:      21
Files on destination: 21
Missing from dest:    0
============================================================

✓ All files are synced! No missing files found.
"""
from dotenv import load_dotenv
import json
import logging
import os
from pathlib import Path
from typing import Set, List, Optional
import uuid

import globus_sdk
import typer

from orchestration.config import get_config
from orchestration.globus.transfer import build_endpoints

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

TOKEN_FILE = Path.home() / ".globus_sync_check_tokens.json"


def get_transfer_client() -> globus_sdk.TransferClient:
    """
    Get a Globus TransferClient.

    Uses confidential client if GLOBUS_CLIENT_ID and GLOBUS_CLIENT_SECRET are set,
    otherwise uses cached tokens or prompts for browser-based login.

    :returns: Authenticated TransferClient
    """
    client_id = os.getenv("GLOBUS_CLIENT_ID")
    client_secret = os.getenv("GLOBUS_CLIENT_SECRET")
    scopes = "urn:globus:auth:scope:transfer.api.globus.org:all"

    # If we have both client ID and secret, use confidential client
    if client_id and client_secret:
        logger.info("Using confidential client credentials")
        confidential_client = globus_sdk.ConfidentialAppAuthClient(client_id, client_secret)
        authorizer = globus_sdk.ClientCredentialsAuthorizer(confidential_client, scopes)
        return globus_sdk.TransferClient(authorizer=authorizer)

    # Otherwise, use native app auth with browser login
    native_client_id = client_id or "61338d24-54d5-408f-a10d-66c06b59f6d2"  # Default Globus native client ID
    client = globus_sdk.NativeAppAuthClient(native_client_id)

    # Check for cached tokens
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

    # No valid cached tokens, do browser login
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
    """
    Save tokens to file for reuse.

    :param token_response: The token response from Globus SDK
    :returns: None
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
    """
    Resolve an endpoint name from config.yml to its UUID.
    If it looks like a UUID already, return as-is.

    :param name_or_uuid: Endpoint name or UUID
    :returns: Endpoint UUID
    :raises ValueError: If name not found in config.yml
    """
    # If it contains dashes and is long, assume it's already a UUID
    if _looks_like_uuid(name_or_uuid):
        return name_or_uuid

    # Otherwise, look up in config
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
) -> Set[str]:
    """
    Recursively list all files on an endpoint, returning relative paths.

    :param tc: Globus TransferClient
    :param endpoint_uuid: The endpoint UUID
    :param path: Absolute path on the endpoint to scan
    :param _relative_base: Internal use for building relative paths

    :returns: Set of relative file paths (relative to the initial path)
    """
    files = set()
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
    """
    List all endpoints defined in config.yml.

    :returns: None
    """
    config = get_config()
    endpoints = build_endpoints(config)

    print(f"\n{'Endpoint Name':<30} {'UUID':<40} {'Root Path'}")
    print("-" * 100)
    for name, ep in sorted(endpoints.items()):
        print(f"{name:<30} {ep.uuid:<40} {ep.root_path}")


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
) -> Optional[List[str]]:
    """
    Compare files between source and destination Globus endpoints.

    Reports files that exist on source but are missing from destination.

    Authentication: Uses GLOBUS_CLIENT_ID/GLOBUS_CLIENT_SECRET if both are set,
    otherwise uses cached tokens or prompts for browser login.

    :param source: Source endpoint name from config.yml
    :param source_uuid: Source endpoint UUID (alternative to --source)
    :param source_path: Path on source endpoint
    :param dest: Destination endpoint name from config.yml
    :param dest_uuid: Destination endpoint UUID (alternative to --dest)
    :param dest_path: Path on destination endpoint
    :param output_file: Write missing files to this file (one per line)
    :param show_matching: Also print files that exist on both endpoints
    :param list_endpoints: List available endpoints from config.yml and exit
    :param logout: Remove cached tokens and exit
    :returns: List of missing file paths, or None if listing endpoints or logging out
    """
    # Setup logging
    log_level = logging.INFO if verbose else logging.WARNING
    logging.basicConfig(level=log_level, format="%(levelname)s: %(message)s", force=True)

    # Handle --logout flag
    if logout:
        if TOKEN_FILE.exists():
            TOKEN_FILE.unlink()
            print("Logged out (removed cached tokens)")
        else:
            print("No cached tokens to remove")
        return None

    # Handle --list-endpoints flag
    if list_endpoints:
        print_endpoints()
        return None

    # Validate required options for comparison
    if not source_path:
        raise typer.BadParameter("--source-path is required")
    if not dest_path:
        raise typer.BadParameter("--dest-path is required")

    # Resolve endpoint UUIDs
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

    # Initialize transfer client
    tc = get_transfer_client()

    # Ensure paths start with / (Globus prepends /~/ to relative paths)
    if not source_path.startswith("/"):
        source_path = "/" + source_path
    if not dest_path.startswith("/"):
        dest_path = "/" + dest_path

    # List files on both endpoints
    logger.info(f"Scanning source: {source or src_uuid} at {source_path}")
    source_files = list_files_recursive(tc, src_uuid, source_path)
    logger.info(f"Found {len(source_files)} files on source")

    logger.info(f"Scanning destination: {dest or dst_uuid} at {dest_path}")
    dest_files = list_files_recursive(tc, dst_uuid, dest_path)
    logger.info(f"Found {len(dest_files)} files on destination")

    # Find missing and matching files
    missing = sorted(source_files - dest_files)
    matching = sorted(source_files & dest_files)

    # Report results
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
            with open(output_file, "w") as fp:
                fp.write("\n".join(missing))
            print(f"\nWrote {len(missing)} paths to {output_file}")
    else:
        print("\n✓ All files are synced! No missing files found.")

    return missing


if __name__ == "__main__":
    typer.run(main)
