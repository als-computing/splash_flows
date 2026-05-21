import logging

from orchestration.flows.bl832.config import Config832
from orchestration.flows.bl832.nersc import _load_job_options
from orchestration.mlflow import register_checkpoint

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def register_mlflow_checkpoints():
    config = Config832()

    scripts_dir = "/global/cfs/cdirs/als/data_mover/8.3.2/tomography_segmentation_scripts/"

    register_checkpoint(
        model_name="sam3-petiole",
        nersc_path=f"{scripts_dir}sam3_finetune/sam3/checkpoint_v6.pt",
        alcf_path="/eagle/IRIBeta/als/seg_models/sam3/checkpoint_v6.pt",
        config=config,
        alias="production",
        description="SAM3 v6 fine-tuned on petiole micro-CT data.",
        inference_params={
            # ── paths ──────────────────────────────────────────────────────────
            "original_checkpoint_path":
            f"{scripts_dir}sam3_finetune/sam3/sam3.pt",
            "bpe_path": f"{scripts_dir}sam3_finetune/sam3/bpe_simple_vocab_16e6.txt.gz",
            "conda_env_path": "/global/cfs/cdirs/als/data_mover/8.3.2/envs/sam3-py311",
            "seg_scripts_dir": f"{scripts_dir}inference_latest/forge_feb_seg_model_demo/",
            "checkpoints_dir": f"{scripts_dir}sam3_finetune/sam3/",
            # ── inference hyperparameters ───────────────────────────────────────
            "script_name": "src/inference_v6.py",
            "batch_size": 1,
            "patch_size": 400,
            "confidence": [0.5],       # list → JSON-encoded automatically
            "overlap": 0.25,
            "prompts": [               # list → JSON-encoded automatically
                "Phloem Fibers",
                "Hydrated Xylem vessels",
                "Air-based Pith cells",
                "Dehydrated Xylem vessels",
            ],
        },
    )

    register_checkpoint(
        model_name="dinov3-petiole",
        nersc_path="/global/cfs/cdirs/als/data_mover/8.3.2/tomography_segmentation_scripts/dino/best.ckpt",
        alcf_path="/eagle/IRIBeta/als/seg_models/dino/best.ckpt",
        config=config,
        alias="production",
        description="DINOv3 fine-tuned on petiole micro-CT data.",
        inference_params={
            # ── paths ──────────────────────────────────────────────────────────
            "conda_env_path": "/global/cfs/cdirs/als/data_mover/8.3.2/envs/dino_demo",
            "seg_scripts_dir": f"{scripts_dir}inference_v5_multiseg/forge_feb_seg_model_demo/",
            # ── inference hyperparameters ───────────────────────────────────────
            "script_name": "src.inference_dino_v1",
            "batch_size": 4,
            "nproc_per_node": 4,
        },
    )

    register_checkpoint(
        model_name="dinov3-moon",
        nersc_path="/global/cfs/cdirs/als/data_mover/8.3.2/tomography_segmentation_scripts/dino/best_moon.ckpt",
        alcf_path="/eagle/IRIBeta/als/seg_models/dino/best_moon.ckpt",
        config=config,
        alias="production",
        description="DINOv3 fine-tuned on lunar regolith micro-CT data (ice, particles, pores).",
        inference_params={
            "conda_env_path": "/global/cfs/cdirs/als/data_mover/8.3.2/envs/dino_demo",
            "seg_scripts_dir": f"{scripts_dir}moon_seg/forge_feb_seg_model_demo/",
            "script_name": "src.inference_dino_v2",
            "batch_size": 4,
            "nproc_per_node": 4,
        },
    )

    register_checkpoint(
        model_name="dinov3-leaf",
        nersc_path="/global/cfs/cdirs/als/data_mover/8.3.2/tomography_segmentation_scripts/dino/best_leaf_new.ckpt",
        alcf_path="/eagle/IRIBeta/als/seg_models/dino/best_leaf_new.ckpt",
        config=config,
        alias="production",
        description="DINOv3 fine-tuned on leaf micro-CT data.",
        inference_params={
            "conda_env_path": "/global/cfs/cdirs/als/data_mover/8.3.2/envs/dino_demo",
            "seg_scripts_dir": f"{scripts_dir}leaf_seg/forge_feb_seg_model_demo/",
            "script_name": "src.inference_dino_v4",
            "batch_size": 4,
            "nproc_per_node": 4,
        },
    )


def retrieve_mlflow_params_test() -> bool:
    """Test that _load_job_options correctly pulls inference params from the MLflow registry.

    Verifies the three-layer resolution for both SAM3 and DINOv3:
    - MLflow-registered values override config defaults for model-coupled params.
    - SLURM allocation params (qos, account, etc.) are unchanged from config.
    - List values (confidence, prompts) are correctly deserialized from JSON tags.

    Returns:
        True if all assertions pass, False if any check fails.
    """
    config = Config832()
    all_passed = True

    # ── SAM3 ─────────────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("Testing SAM3 option resolution")
    logger.info("=" * 60)

    sam3_opts = _load_job_options(
        "nersc-segmentation-options",
        config.nersc_segment_sam3_settings,
        config=config,
        mlflow_model_name="sam3-petiole",
        mlflow_checkpoint_key="finetuned_checkpoint_path",
    )

    sam3_checks = {
        # MLflow should have overridden these
        "finetuned_checkpoint_path": (
            lambda v: "checkpoint" in v,
            "finetuned_checkpoint_path should contain 'checkpoint'"
        ),
        "conda_env_path": (
            lambda v: "sam3" in v,
            "conda_env_path should reference sam3 env"
        ),
        "prompts": (
            lambda v: isinstance(v, list) and len(v) > 0,
            "prompts should be a non-empty list (JSON-deserialized)"
        ),
        "confidence": (
            lambda v: isinstance(v, list),
            "confidence should be a list (JSON-deserialized)"
        ),
        "batch_size": (
            lambda v: isinstance(v, int),
            "batch_size should be an int"
        ),
        # SLURM params should still come from config
        "qos": (
            lambda v: v == config.nersc_segment_sam3_settings["qos"],
            "qos should be unchanged from config"
        ),
        "account": (
            lambda v: v == config.nersc_segment_sam3_settings["account"],
            "account should be unchanged from config"
        ),
    }

    for key, (check_fn, message) in sam3_checks.items():
        value = sam3_opts.get(key)
        passed = check_fn(value) if value is not None else False
        status = "✓" if passed else "✗"
        logger.info(f"  [{status}] {key}={value!r}  —  {message}")
        if not passed:
            all_passed = False

    # ── DINOv3 ───────────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("Testing DINOv3 option resolution")
    logger.info("=" * 60)

    dino_opts = _load_job_options(
        "nersc-dinov3-seg-options",
        config.nersc_segment_dinov3_settings,
        config=config,
        mlflow_model_name="dinov3-petiole",
        mlflow_checkpoint_key="dino_checkpoint_path",
    )

    dino_checks = {
        "dino_checkpoint_path": (
            lambda v: v.endswith(".ckpt"),
            "dino_checkpoint_path should end with .ckpt"
        ),
        "conda_env_path": (
            lambda v: len(v) > 0,
            "conda_env_path should be non-empty"
        ),
        "batch_size": (
            lambda v: isinstance(v, int) and v > 0,
            "batch_size should be a positive int"
        ),
        "script_name": (
            lambda v: "dino" in v.lower(),
            "script_name should reference dino"
        ),
        # SLURM params unchanged
        "qos": (
            lambda v: v == config.nersc_segment_dinov3_settings["qos"],
            "qos should be unchanged from config"
        ),
        "num_nodes": (
            lambda v: isinstance(v, int) and v > 0,
            "num_nodes should be a positive int"
        ),
    }

    for key, (check_fn, message) in dino_checks.items():
        value = dino_opts.get(key)
        passed = check_fn(value) if value is not None else False
        status = "✓" if passed else "✗"
        logger.info(f"  [{status}] {key}={value!r}  —  {message}")
        if not passed:
            all_passed = False

    # ── DINOv3-moon ───────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("Testing DINOv3-moon option resolution")
    logger.info("=" * 60)

    moon_opts = _load_job_options(
        "nersc-dinov3-moon-seg-options",
        config.nersc_segment_dinov3_moon_settings,
        config=config,
        mlflow_model_name="dinov3-moon",
        mlflow_checkpoint_key="dino_checkpoint_path",
    )

    moon_checks = {
        "dino_checkpoint_path": (
            lambda v: v.endswith(".ckpt"),
            "dino_checkpoint_path should end with .ckpt"
        ),
        "script_name": (
            lambda v: "v2" in v.lower(),
            "script_name should reference inference_dino_v2"
        ),
        "batch_size": (
            lambda v: isinstance(v, int) and v > 0,
            "batch_size should be a positive int"
        ),
        "qos": (
            lambda v: v == config.nersc_segment_dinov3_moon_settings["qos"],
            "qos should be unchanged from config"
        ),
    }

    for key, (check_fn, message) in moon_checks.items():
        value = moon_opts.get(key)
        passed = check_fn(value) if value is not None else False
        status = "✓" if passed else "✗"
        logger.info(f"  [{status}] {key}={value!r}  —  {message}")
        if not passed:
            all_passed = False

    # ── Summary ───────────────────────────────────────────────────────────────
    logger.info("=" * 60)
    if all_passed:
        logger.info("✓ All MLflow integration checks passed.")
    else:
        logger.error("✗ One or more MLflow integration checks failed.")
    logger.info("=" * 60)

    return all_passed


if __name__ == "__main__":
    register_mlflow_checkpoints()
    retrieve_mlflow_params_test()
