"""Register data files and Zarr/HDF5 stores to the Tiled catalog.

Intended to run on a Ride worker. The file path must be accessible
to the Tiled server's filesystem.
"""
from dotenv import load_dotenv
import os
from pathlib import Path

from prefect import flow, get_run_logger, task
from prefect.utilities.asyncutils import run_coro_as_sync
from tiled.client import from_uri
from tiled.client.register import register


load_dotenv()


@task(name="register-file-to-tiled", task_run_name="register-{path}")
def register_file_to_tiled(
    path: Path | str,
    prefix: str | None = None,
    overwrite: bool = False,
    tags: list[str] | None = None,
) -> None:
    """
    Register a file or Zarr/HDF5 store to the Tiled catalog.

    Args:
        path: The path to the file or Zarr/HDF5 store to register.
        prefix: The sub-path within the Tiled catalog where the entry should be registered.
        overwrite: Whether to overwrite an existing entry with the same key.
        tags: A list of tags to apply to the registered entry.
    Raises:
        RuntimeError: If registration fails for any reason.
    Returns:
        None
    """
    logger = get_run_logger()
    path = Path(path)
    tiled_uri = os.environ["TILED_URI"]

    client = from_uri(tiled_uri)

    logger.info(f"Registering {path} to Tiled catalog at {tiled_uri} with prefix {prefix!r}")
    try:
        run_coro_as_sync(  # Bridge synchronous Prefect task to async Tiled client method
            register(
                node=client,
                path=path,
                prefix=prefix or "/",
                overwrite=overwrite,
            )
        )
    except Exception as e:
        raise RuntimeError(
            f"Failed to register {path} to Tiled catalog at {tiled_uri} "
            f"(prefix={prefix!r}): {e}"
        ) from e

    if not tags:
        return

    # Navigate to prefix node after registration
    node = client
    for segment in (prefix or "").strip("/").split("/"):
        if segment:
            node = node[segment]

    if path.is_dir() and not path.suffix:
        # TIFF directory: Tiled registers each file flat into the prefix node
        for key in node:
            _apply_tags(entry_node=node[key], tags=tags)
    else:
        # .h5 or .zarr: registered as single node, keyed by stem
        # Even on COLLISION the entry exists — just try it directly
        entry_key = path.stem
        logger.info(f"Looking up entry key {entry_key!r} under {prefix!r}")
        try:
            _apply_tags(entry_node=node[entry_key], tags=tags)
        except KeyError:
            # Key not found even after registration — log all available keys to diagnose
            available = sorted(node)
            logger.warning(
                f"Entry {entry_key!r} not found under {prefix!r}. "
                f"Available keys: {available}"
            )


@task(name="apply-tags", task_run_name="apply-tags-{tags}")
def _apply_tags(entry_node, tags: list[str]) -> None:
    """
    Apply tags to a Tiled entry node. If the entry already has tags, merge them with the new tags.

    Args:
        entry_node: The Tiled entry node to which tags should be applied.
        tags: A list of tags to apply to the entry node.
    Raises:
        Exception: If the tagging operation fails for any reason.
    Returns:
        None
    """
    logger = get_run_logger()
    existing_blob = entry_node.access_blob
    existing_tags = (existing_blob or {}).get("tags", [])
    merged_tags = list(set(existing_tags) | set(tags))
    op = "replace" if existing_blob is not None else "add"
    try:
        # entry_node.update_metadata(access_tags=merged_tags)
        entry_node.patch_metadata(
            access_blob_patch=[{"op": op, "path": "", "value": {"tags": merged_tags}}],
        )

        logger.debug(f"Tagged {entry_node.uri} with {merged_tags}")
    except Exception as e:
        logger.debug(f"Could not tag {entry_node.uri}: {e}")


@task(name="check-tiled-tags", task_run_name="check-tags-{path}")
def check_tags(
    path: Path | str,
    prefix: str,
    expected_tags: set[str],
) -> tuple[bool, list[str]]:
    """Check whether a registered dataset has the expected tags applied.

    Navigates to the entry corresponding to ``path`` under ``prefix`` in the
    Tiled catalog and compares its ``access_blob.tags`` against ``expected_tags``.

    For TIFF directories (a directory with no suffix), the first child entry
    under the prefix node is checked, since ``register`` registers each TIFF
    file flat into the prefix node.

    Args:
        path: Path to the file or store that was registered.
        prefix: Sub-path within the Tiled catalog where the entry was registered.
        expected_tags: Tags that must be present on the entry.

    Returns:
        A tuple ``(ok, actual_tags)`` where ``ok`` is True iff every tag in
        ``expected_tags`` is present in the entry's ``access_blob.tags``.

    Raises:
        KeyError: If the entry cannot be located under ``prefix``.
    """
    logger = get_run_logger()
    path = Path(path)
    tiled_uri = os.environ["TILED_URI"]
    client = from_uri(tiled_uri)

    # Navigate to the prefix node
    node = client
    for segment in prefix.strip("/").split("/"):
        if segment:
            node = node[segment]

    # For TIFF directories, register flattens files into the prefix node;
    # for .h5 / .zarr, the entry is keyed by the path stem.
    if path.is_dir() and not path.suffix:
        key = next(iter(node))
        node = node[key]
    else:
        node = node[path.stem]

    actual = node.access_blob.get("tags", []) if node.access_blob else []
    ok = expected_tags <= set(actual)
    logger.info(
        f"{path.name} under {prefix!r}: "
        f"expected={sorted(expected_tags)} actual={actual} ok={ok}"
    )
    return ok, actual


@flow(name="register-to-tiled", flow_run_name="register-{path}")
def register_to_tiled(
    path: Path | str,
    prefix: str | None = None,
    overwrite: bool = False,
    tags: list[str] | None = None,
) -> None:
    """Register a file or Zarr store to the Tiled server.

    Args:
        path: Path to the file or Zarr store (client filesystem).
        prefix: Optional sub-path within the Tiled catalog.
        overwrite: Whether to overwrite existing entries in the Tiled catalog.
        tags: Optional list of tags to apply to the registered entry.
    """
    logger = get_run_logger()
    path = Path(path)
    logger.info(f"Submitting task: register {path} to Tiled (prefix={prefix!r})")
    register_file_to_tiled(path, prefix=prefix, overwrite=overwrite, tags=tags)


if __name__ == "__main__":
    h5 = Path(os.environ["EXAMPLE_H5_PATH"])
    tiffs = Path(os.environ["EXAMPLE_TIFFS_PATH"])
    zarr = Path(os.environ["EXAMPLE_ZARR_PATH"])

    cases = [
        (h5,    "beamlines/bl832/raw/",     {"bl832"}),
        (tiffs, "beamlines/bl832/scratch",  {"bl832", "dabramov"}),
        (zarr,  "beamlines/bl832/scratch",  {"bl832"}),
    ]

    for path, prefix, tags in cases:
        register_to_tiled(path=path, prefix=prefix, tags=list(tags), overwrite=False)

    for path, prefix, expected in cases:
        ok, actual = check_tags(path, prefix, expected)
        print(f"{'✓' if ok else '✗'} {path.name}: expected={sorted(expected)} actual={actual}")
