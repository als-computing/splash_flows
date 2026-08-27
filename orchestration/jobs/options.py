"""Three-layer job option resolution: config defaults → MLflow → Prefect Variable.

Lets beamline-specific code declare a base settings dict, optionally tie it to
an MLflow Model Registry entry, and accept runtime overrides via a Prefect
Variable — all without hardcoding any one beamline or HPC target.
"""

import json
import logging
from typing import Any

from prefect.variables import Variable

from orchestration.config import BeamlineConfig
from orchestration.mlflow import get_checkpoint_info

logger = logging.getLogger(__name__)


def load_job_options(
    variable_name: str,
    config_settings: dict[str, Any],
    config: BeamlineConfig | None = None,
    mlflow_model_name: str | None = None,
    mlflow_checkpoint_key: str | None = None,
) -> dict[str, Any]:
    """Load job options with three-layer resolution: config → MLflow → Prefect Variable.

    Resolution order (later layers win):

    1. ``config_settings`` — authoritative defaults from the config YAML.
    2. MLflow Model Registry — if ``mlflow_model_name`` is provided, all
       ``inference_params`` tags are overlaid onto opts by their config key name.
       ``nersc_path`` is additionally mapped to ``mlflow_checkpoint_key`` if given.
    3. Prefect Variable (``variable_name``) — skipped if absent or ``defaults: true``.
       If ``defaults: false``, provided keys override all lower layers.

    Args:
        variable_name: Name of the Prefect Variable to load.
        config_settings: Settings dict used as base defaults (e.g.
            ``config.nersc_segment_sam3_settings``).
        config: Beamline config instance needed for MLflow lookup. If ``None``,
            the MLflow layer is skipped.
        mlflow_model_name: Registered MLflow model name, e.g. ``'sam3-petiole'``.
            If ``None``, the MLflow layer is skipped.
        mlflow_checkpoint_key: Config key to populate from the MLflow model's
            ``nersc_path`` tag, e.g. ``'finetuned_checkpoint_path'``.

    Returns:
        Resolved options dict ready for use by the caller.
    """
    # ── Layer 1: config defaults ──────────────────────────────────────────────
    opts = dict(config_settings)

    # ── Layer 2: MLflow registry ──────────────────────────────────────────────
    if config is not None and mlflow_model_name:
        try:
            checkpoint_info = get_checkpoint_info(mlflow_model_name, config)
            if checkpoint_info:
                # Map nersc_path to the caller-specified checkpoint key
                if mlflow_checkpoint_key:
                    opts[mlflow_checkpoint_key] = checkpoint_info.nersc_path
                    logger.info(
                        f"MLflow '{mlflow_model_name}': "
                        f"{mlflow_checkpoint_key}={checkpoint_info.nersc_path}"
                    )
                # Overlay all inference params that match existing config keys
                overlaid = []
                for k, v in checkpoint_info.inference_params.items():
                    if k in opts:
                        opts[k] = v
                        overlaid.append(k)
                    else:
                        # Also inject new keys (e.g. alcf_path for future use)
                        opts[k] = v
                logger.info(
                    f"MLflow '{mlflow_model_name}': overlaid params: {overlaid}"
                )
            else:
                logger.info(
                    f"MLflow: no production checkpoint for '{mlflow_model_name}', "
                    "using config defaults."
                )
        except Exception as e:
            logger.warning(
                f"MLflow lookup failed for '{mlflow_model_name}': {e}. "
                "Using config defaults."
            )

    # ── Layer 3: Prefect Variable overrides ───────────────────────────────────
    try:
        options = Variable.get(variable_name, default={"defaults": True}, _sync=True)
        if isinstance(options, str):
            options = json.loads(options)
    except Exception as e:
        logger.warning(f"Could not load '{variable_name}': {e}. Skipping variable overrides.")
        return opts

    if options.get("defaults", True):
        logger.info(f"Prefect Variable '{variable_name}': no overrides.")
        return opts

    overrides = {k: v for k, v in options.items() if k != "defaults"}
    logger.info(f"Prefect Variable '{variable_name}': applying overrides: {list(overrides)}")
    return {**opts, **overrides}
