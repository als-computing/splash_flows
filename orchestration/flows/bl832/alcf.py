from concurrent.futures import Future
from pathlib import Path
import time
from typing import Optional

from globus_compute_sdk import Client, Executor
from globus_compute_sdk.serialize import CombinedCode
from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret
from prefect.variables import Variable

from orchestration.flows.bl832.config import Config832
from orchestration.flows.bl832.job_controller import get_controller, HPC, TomographyHPCController
from orchestration.prune_controller import get_prune_controller, PruneMethod
from orchestration.transfer_controller import get_transfer_controller, CopyMethod
# from orchestration.prefect import schedule_prefect_flow
from orchestration.tiled import register_file_to_tiled


class ALCFTomographyHPCController(TomographyHPCController):
    """
    Implementation of TomographyHPCController for ALCF. Methods here leverage Globus Compute for processing tasks.
    There is a @staticmethod wrapper for each compute task submitted via Globus Compute.
    Also, there is a shared wait_for_globus_compute_future method that waits for the task to complete.

    :param TomographyHPCController: Abstract class for tomography HPC controllers.
    """

    def __init__(
        self,
        config: Config832
    ) -> None:
        """
        Initialize the ALCF Tomography HPC Controller.

        :param config: Configuration object for the controller.
        """
        super().__init__(config)
        # Load allocation root from the Prefect JSON block
        # The block must be registered with the name "alcf-allocation-root-path"
        logger = get_run_logger()
        allocation_data = Variable.get("alcf-allocation-root-path", _sync=True)
        self.allocation_root = allocation_data.get("alcf-allocation-root-path")  # eagle/SYNAPS-I/
        if not self.allocation_root:
            raise ValueError("Allocation root not found in JSON block 'alcf-allocation-root-path'")
        logger.info(f"Allocation root loaded: {self.allocation_root}")

    def reconstruct(
        self,
        file_path: str = "",
    ) -> bool:
        """
        Run tomography reconstruction at ALCF through Globus Compute.

        :param file_path : Path to the file to be processed.
        :return: True if the task completed successfully, False otherwise.
        """
        logger = get_run_logger()
        file_name = Path(file_path).stem + ".h5"
        folder_name = Path(file_path).parent.name

        rundir = f"{self.allocation_root}/data/bl832/raw"
        recon_script = f"{self.allocation_root}/reconstruction/scripts/globus_reconstruction_multinode.py"

        gcc = Client(code_serialization_strategy=CombinedCode())

        endpoint_id = Variable.get(
            "alcf-globus-compute-recon-uuid",
            default="4953017e-6127-4587-9ee3-b71db7623122",
            _sync=True
        )

        with Executor(endpoint_id=endpoint_id, client=gcc) as fxe:
            logger.info(f"Running Tomopy reconstruction on {file_name} at ALCF")
            future = fxe.submit(
                self._reconstruct_wrapper_multinode,
                rundir,
                recon_script,
                file_name,
                folder_name
            )
            result = self._wait_for_globus_compute_future(future, "reconstruction", check_interval=10)
            return result

    @staticmethod
    def _reconstruct_wrapper(
        rundir: str = "/eagle/SYNAPS-I/data/bl832/raw",
        script_path: str = "/eagle/SYNAPS-I/reconstruction/scripts/globus_reconstruction.py",
        h5_file_name: str = None,
        folder_path: str = None
    ) -> str:
        """
        Python function that wraps around the application call for Tomopy reconstruction on ALCF

        :param rundir: the directory on the eagle file system (ALCF) where the input data are located
        :param script_path: the path to the script that will run the reconstruction
        :param h5_file_name: the name of the h5 file to be reconstructed
        :param folder_path: the path to the folder containing the h5 file
        :return: confirmation message
        """
        import os
        import subprocess
        import time

        rec_start = time.time()

        # Move to directory where data are located
        os.chdir(rundir)

        # Run reconstruction.py
        command = f"python {script_path} {h5_file_name} {folder_path}"
        recon_res = subprocess.run(command.split(" "), stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        rec_end = time.time()

        print(f"Reconstructed data in {folder_path}/{h5_file_name} in {rec_end-rec_start} seconds;\n {recon_res}")

        return (
            f"Reconstructed data specified in {folder_path} / {h5_file_name} in {rec_end-rec_start} seconds;\n"
            f"{recon_res}"
        )

    @staticmethod
    def _reconstruct_wrapper_multinode(
        rundir: str,
        script_path: str,
        h5_file_name: str,
        folder_path: str,
        node_list: list[str] = None,  # Pass explicitly
        num_nodes: int = 8,
    ) -> str:
        """
        Wrapper function to run Tomopy reconstruction using mpiexec on ALCF across multiple nodes.

        :param rundir: the directory on the eagle file system (ALCF) where the input data are located
        :param script_path: the path to the script that will run the reconstruction
        :param h5_file_name: the name of the h5 file to be reconstructed
        :param folder_path: the path to the folder containing the h5 file
        :param node_list: list of nodes to use for reconstruction (if None, will attempt to read from PBS_NODEFILE)
        :param num_nodes: number of nodes to use for reconstruction (used if node_list is None)
        :return: confirmation message
        """
        import os
        import subprocess
        import time
        import h5py
        import tempfile

        rec_start = time.time()
        os.chdir(rundir)

        # If node_list not provided, try PBS_NODEFILE
        if node_list is None:
            pbs_nodefile = os.environ.get("PBS_NODEFILE")
            if pbs_nodefile and os.path.exists(pbs_nodefile):
                with open(pbs_nodefile, 'r') as f:
                    all_lines = [line.strip() for line in f if line.strip()]
                node_list = list(dict.fromkeys(all_lines))
            else:
                # Fallback: get nodes from PBS_NODENUM or assume localhost
                node_list = ["localhost"]

        num_nodes = len(node_list)
        print("=== RECON DEBUG ===")
        print(f"Using {num_nodes} nodes: {node_list}")

        # Read number of slices
        h5_path = f"{rundir}/{folder_path}/{h5_file_name}"
        with h5py.File(h5_path, 'r') as f:
            num_slices = f['/exchange/data'].shape[1]

        print(f"Total slices: {num_slices}")
        slices_per_node = num_slices // num_nodes

        venv_path = "/eagle/SYNAPS-I/reconstruction/env/tomopy"

        # Critical: Set environment variables BEFORE the conda activation
        env_setup = (
            "export TMPDIR=/tmp && "
            "export NUMEXPR_MAX_THREADS=64 && "
            "export NUMEXPR_NUM_THREADS=64 && "
            "export OMP_NUM_THREADS=64 && "
            "export MKL_NUM_THREADS=64 && "
            "module use /soft/modulefiles && "
            "module load conda && "
            "source $(conda info --base)/etc/profile.d/conda.sh && "
            f"conda activate {venv_path} && "
            f"cd {rundir} && "
        )

        procs = []
        temp_hostfiles = []

        for i, node in enumerate(node_list):
            sino_start = i * slices_per_node
            sino_end = num_slices if i == num_nodes - 1 else (i + 1) * slices_per_node

            cmd = f"python {script_path} {h5_file_name} {folder_path} {sino_start} {sino_end}"

            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.hosts') as f:
                f.write(node + '\n')
                temp_hostfile = f.name
            temp_hostfiles.append(temp_hostfile)

            # Use --cpu-bind to ensure proper CPU affinity
            full_cmd = [
                "mpiexec",
                "-n", "1",
                "-ppn", "1",
                "--cpu-bind", "depth",
                "-d", "64",  # depth=64 cores per rank
                "-hostfile", temp_hostfile,
                "bash", "-c", env_setup + cmd
            ]

            print(f"Launching on {node}: slices {sino_start}-{sino_end}")
            proc = subprocess.Popen(full_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            procs.append((proc, node, sino_start, sino_end))

        # Wait and collect results
        failed = []
        for proc, node, sino_start, sino_end in procs:
            stdout, stderr = proc.communicate()
            if proc.returncode != 0:
                print(f"FAILED on {node} (slices {sino_start}-{sino_end})")
                print(f"STDERR: {stderr.decode()[-2000:]}")
                failed.append(node)
            else:
                print(f"SUCCESS on {node} (slices {sino_start}-{sino_end})")

        # Cleanup
        for hf in temp_hostfiles:
            try:
                os.remove(hf)
            except OSError:
                pass

        if failed:
            raise RuntimeError(f"Reconstruction failed on nodes: {failed}")

        return f"Reconstructed {h5_file_name} across {num_nodes} nodes in {time.time() - rec_start:.1f}s"

    def build_multi_resolution(
        self,
        file_path: str = "",
    ) -> bool:
        """
        Tiff to Zarr code that is executed using Globus Compute

        :param file_path: Path to the file to be processed.
        :return: True if the task completed successfully, False otherwise.
        """
        logger = get_run_logger()

        file_name = Path(file_path).stem
        folder_name = Path(file_path).parent.name

        tiff_scratch_path = f"{self.allocation_root}/data/scratch/{folder_name}/rec{file_name}/"
        raw_path = f"{self.allocation_root}/data/raw/{folder_name}/{file_name}.h5"

        iri_als_bl832_rundir = f"{self.allocation_root}/data/raw"
        iri_als_bl832_conversion_script = f"{self.allocation_root}/scripts/tiff_to_zarr.py"

        gcc = Client(code_serialization_strategy=CombinedCode())

        with Executor(endpoint_id=Secret.load("globus-compute-endpoint").get(), client=gcc) as fxe:
            logger.info(f"Running Tiff to Zarr on {raw_path} at ALCF")
            future = fxe.submit(
                self._build_multi_resolution_wrapper,
                iri_als_bl832_rundir,
                iri_als_bl832_conversion_script,
                tiff_scratch_path,
                raw_path
            )
            result = self._wait_for_globus_compute_future(future, "tiff to zarr conversion", check_interval=10)
            return result

    @staticmethod
    def _build_multi_resolution_wrapper(
        rundir: str = "/eagle/IRIProd/ALS/data/raw",
        script_path: str = "/eagle/IRIProd/ALS/scripts/tiff_to_zarr.py",
        recon_path: str = None,
        raw_path: str = None
    ) -> str:
        """
        Python function that wraps around the application call for Tiff to Zarr on ALCF

        :param rundir: the directory on the eagle file system (ALCF) where the input data are located
        :param script_path: the path to the script that will convert the tiff files to zarr
        :param recon_path: the path to the reconstructed data
        :param raw_path: the path to the raw data
        :return: confirmation message
        """
        import os
        import subprocess

        # Move to directory where data are located
        os.chdir(rundir)

        # Convert tiff files to zarr
        command = (f"python {script_path} {recon_path} --raw_directory {raw_path}")
        zarr_res = subprocess.run(command.split(" "), stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        return (
            f"Converted tiff files to zarr;\n {zarr_res}"
        )

    def segmentation_sam3(
        self,
        recon_folder_path: str = "",
    ) -> bool:
        """
        Run tomography segmentation at ALCF through Globus Compute.

        :param recon_folder_path: Path to the reconstructed data folder to be processed.
        :return: True if the task completed successfully, False otherwise.
        """
        logger = get_run_logger()

        # Operate on reconstructed data
        # Input: folder_name/rec20211222_125057_petiole4/
        # Output should go to: folder_name/seg20211222_125057_petiole4/

        rundir = f"{self.allocation_root}/data/bl832/scratch/reconstruction/{recon_folder_path}"
        output_folder = recon_folder_path.replace('/rec', '/seg')
        seg_base = f"{self.allocation_root}/data/bl832/scratch/segmentation/{output_folder}"
        output_dir = f"{seg_base}/sam3"   # SAM3 writes class folders directly here
        gcc = Client(code_serialization_strategy=CombinedCode())

        endpoint_id = Variable.get(
            "alcf-globus-compute-seg-uuid",
            default="168c595b-9493-42db-9c6a-aad960913de2",
            _sync=True
        )

        segmentation_module = "src.inference_v6"
        workdir = f"{self.allocation_root}/segmentation/scripts/inference_latest/forge_feb_seg_model_demo"

        with Executor(endpoint_id=endpoint_id, client=gcc) as fxe:
            logger.info(f"Running segmentation on {recon_folder_path} at ALCF")
            future = fxe.submit(
                self._segmentation_sam3_wrapper,
                input_dir=rundir,
                output_dir=output_dir,
                script_module=segmentation_module,
                workdir=workdir
            )
            result = self._wait_for_globus_compute_future(future, "segmentation", check_interval=10)

        return result

    def segmentation_dino(
        self,
        recon_folder_path: str = "",
    ) -> bool:
        """
        Run tomography segmentation at ALCF through Globus Compute.

        :param recon_folder_path: Path to the reconstructed data folder to be processed.
        :return: True if the task completed successfully, False otherwise.
        """
        logger = get_run_logger()

        # Operate on reconstructed data
        # Input: folder_name/rec20211222_125057_petiole4/
        # Output should go to: folder_name/seg20211222_125057_petiole4/

        rundir = f"{self.allocation_root}/data/bl832/scratch/reconstruction/{recon_folder_path}"
        output_folder = recon_folder_path.replace('/rec', '/seg')
        seg_base = f"{self.allocation_root}/data/bl832/scratch/segmentation/{output_folder}"
        output_dir = f"{seg_base}/dino"   # DINO writes class folders directly here

        gcc = Client(code_serialization_strategy=CombinedCode())

        endpoint_id = Variable.get(
            "alcf-globus-compute-seg-dino-uuid",
            default="07b24393-f649-4f6b-8860-1bfb211d17f4",
            _sync=True
        )

        segmentation_module = "src.inference_dino_v1"
        workdir = f"{self.allocation_root}/segmentation/scripts/inference_latest/forge_feb_seg_model_demo"

        with Executor(endpoint_id=endpoint_id, client=gcc) as fxe:
            logger.info(f"Running segmentation on {recon_folder_path} at ALCF")
            future = fxe.submit(
                self._segmentation_dino_wrapper,
                input_dir=rundir,
                output_dir=output_dir,
                script_module=segmentation_module,
                workdir=workdir
            )
            result = self._wait_for_globus_compute_future(future, "segmentation_dino", check_interval=10)

        return result

    @staticmethod
    def _segmentation_sam3_wrapper(
        input_dir: str = "/eagle/SYNAPS-I/data/bl832/scratch/reconstruction/",
        output_dir: str = "/eagle/SYNAPS-I/data/bl832/scratch/segmentation/",
        script_module: str = "src.inference_v6",
        workdir: str = "/eagle/SYNAPS-I/segmentation/scripts/inference_latest/forge_feb_seg_model_demo",
        nproc_per_node: int = 4,
        patch_size: int = 1000,
        overlap_ratio: float = 0.5,
        batch_size: int = 8,
        confidence: float = 0.5,
        prompts: list[str] = ['Phloem Fibers', 'Hydrated Xylem vessels', 'Air-based Pith cells', 'Dehydrated Xylem vessels'],
        bpe_path: str = "/eagle/SYNAPS-I/segmentation/sam3_finetune/sam3/bpe_simple_vocab_16e6.txt.gz",
        finetuned_checkpoint: str = "/eagle/SYNAPS-I/segmentation/sam3_finetune/sam3/checkpoint_v6.pt",
        original_checkpoint: str = "/eagle/SYNAPS-I/segmentation/sam3_finetune/sam3/sam3.pt",
        use_finetuned: bool = True,
        skip_existing: bool = False,
    ) -> str:
        """
        Wrapper function to run segmentation using torch.distributed.run on ALCF.

        :param input_dir: Directory containing input data for segmentation.
        :param output_dir: Directory to save segmentation outputs.
        :param script_module: Python module containing the segmentation code to run.
        :param workdir: Working directory for the segmentation script.
        :param nproc_per_node: Number of processes per node for distributed training.
        :param patch_size: Patch size for segmentation.
        :param overlap_ratio: Overlap ratio for patch-based segmentation.
        :param batch_size: Batch size for segmentation.
        :param confidence: Confidence threshold for segmentation.
        :param prompts: List of class prompts for segmentation.
        :param bpe_path: Path to the BPE vocab file for SAM.
        :param finetuned_checkpoint: Path to the finetuned SAM checkpoint.
        :param original_checkpoint: Path to the original SAM checkpoint.
        :param use_finetuned: Whether to use the finetuned checkpoint or not.
        :param skip_existing: Whether to skip segmentation for patches that already have outputs.
        :return: Confirmation message upon completion.
        """
        import os
        import subprocess
        import time

        seg_start = time.time()
        os.chdir(workdir)

        # Get PBS info
        pbs_nodefile = os.environ.get("PBS_NODEFILE")
        pbs_jobid = os.environ.get("PBS_JOBID", "12345")

        print("=== PBS DEBUG ===")
        print(f"PBS_NODEFILE: {pbs_nodefile}")
        print(f"PBS_JOBID: {pbs_jobid}")

        if pbs_nodefile and os.path.exists(pbs_nodefile):
            with open(pbs_nodefile, 'r') as f:
                all_lines = [line.strip() for line in f if line.strip()]
            unique_nodes = list(dict.fromkeys(all_lines))
            actual_nnodes = len(unique_nodes)
            master_addr = unique_nodes[0]
            print(f"PBS_NODEFILE contents: {all_lines}")
            print(f"Unique nodes ({actual_nnodes}): {unique_nodes}")
            print(f"Master: {master_addr}")
        else:
            actual_nnodes = 1
            master_addr = "localhost"
            print("No PBS_NODEFILE, single node mode")

        venv_path = "/eagle/SYNAPS-I/segmentation/env"

        # Build command as a list (no shell escaping needed)
        cmd_list = [
            f"{venv_path}/bin/python", "-m", "torch.distributed.run",
            f"--nnodes={actual_nnodes}",
            f"--nproc_per_node={nproc_per_node}",
            f"--rdzv_id={pbs_jobid}",
            "--rdzv_backend=c10d",
            f"--rdzv_endpoint={master_addr}:29500",
            "-m", script_module,
            "--input-dir", input_dir,
            "--output-dir", output_dir,
            "--patch-size", str(patch_size),
            "--overlap-ratio", str(overlap_ratio),
            "--batch-size", str(batch_size),
            "--confidence", str(confidence),
            "--bpe-path", bpe_path,
            "--prompts",
        ]

        # Add prompts directly - no quotes needed with list-based subprocess
        cmd_list.extend(prompts)

        if use_finetuned:
            cmd_list.extend([
                "--finetuned-checkpoint", finetuned_checkpoint,
                "--original-checkpoint", original_checkpoint,
            ])
        else:
            cmd_list.extend(["--original-checkpoint", original_checkpoint])

        if skip_existing:
            cmd_list.append("--skip-existing")

        # Environment variables
        env = os.environ.copy()
        env.update({
            "PATH": f"{venv_path}/bin:{env.get('PATH', '')}",
            "HF_HUB_CACHE": "/eagle/SYNAPS-I/segmentation/.cache/huggingface",
            "HF_HOME": "/eagle/SYNAPS-I/segmentation/.cache/huggingface",
            "CUDA_DEVICE_ORDER": "PCI_BUS_ID",
            "NCCL_NET_GDR_LEVEL": "PHB",
            "NCCL_CROSS_NIC": "1",
            "NCCL_COLLNET_ENABLE": "1",
            "NCCL_NET": "AWS Libfabric",
            "FI_CXI_DISABLE_HOST_REGISTER": "1",
            "FI_MR_CACHE_MONITOR": "userfaultfd",
            "FI_CXI_DEFAULT_CQ_SIZE": "131072",
        })

        # Prepend to LD_LIBRARY_PATH
        ld_path = env.get("LD_LIBRARY_PATH", "")
        env["LD_LIBRARY_PATH"] = f"/soft/libraries/aws-ofi-nccl/v1.9.1-aws/lib:/soft/libraries/hwloc/lib/:{ld_path}"

        if actual_nnodes > 1:
            # Use mpiexec to launch on all nodes
            command = [
                "mpiexec",
                "-n", str(actual_nnodes),
                "-ppn", "1",
                "-hostfile", pbs_nodefile,
                "--cpu-bind", "depth",
                "-d", "16",
            ] + cmd_list
        else:
            command = cmd_list

        print(f"Running: {' '.join(command)}")

        result = subprocess.run(command, env=env, cwd=workdir, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        print(f"STDOUT: {result.stdout[-3000:] if result.stdout else 'None'}")
        print(f"STDERR: {result.stderr[-3000:] if result.stderr else 'None'}")

        if result.returncode != 0:
            raise RuntimeError(
                f"Segmentation failed: {result.returncode}\n"
                f"STDERR: {result.stderr[-2000:] if result.stderr else 'None'}"
            )

        return f"Completed in {time.time() - seg_start:.1f}s"

    @staticmethod
    def _segmentation_dino_wrapper(
        input_dir: str = "/eagle/SYNAPS-I/data/bl832/scratch/reconstruction/",
        output_dir: str = "/eagle/SYNAPS-I/data/bl832/scratch/segmentation/",
        finetuned_checkpoint: str = "/eagle/SYNAPS-I/segmentation/dino/best.ckpt",
        save_overlay: bool = True,
        batch_size: int = 4,
        num_workers: int = 4,
        nproc_per_node: int = 4,
        workdir: str = "/eagle/SYNAPS-I/segmentation/scripts/inference_latest/forge_feb_seg_model_demo",
        script_module: str = "src.inference_dino_v1",
    ) -> str:
        """
        Wrapper function to run segmentation using the DINO model on ALCF.

        :param input_dir: Directory containing input data for segmentation.
        :param output_dir: Directory to save segmentation outputs.
        :param finetuned_checkpoint: Path to the finetuned DINO model checkpoint.
        :param save_overlay: Whether to save overlay visualizations of the segmentation.
        :param batch_size: Batch size for segmentation.
        :param num_workers: Number of worker processes for data loading.
        :param nproc_per_node: Number of processes per node for distributed training.
        :param workdir: Working directory for the segmentation script.
        :return: Confirmation message upon completion.
        """
        import os
        import subprocess
        import time

        seg_start = time.time()
        os.chdir(workdir)

        # Get PBS info
        pbs_nodefile = os.environ.get("PBS_NODEFILE")
        pbs_jobid = os.environ.get("PBS_JOBID", "12345")

        print("=== PBS DEBUG ===")
        print(f"PBS_NODEFILE: {pbs_nodefile}")
        print(f"PBS_JOBID: {pbs_jobid}")

        if pbs_nodefile and os.path.exists(pbs_nodefile):
            with open(pbs_nodefile, 'r') as f:
                all_lines = [line.strip() for line in f if line.strip()]
            unique_nodes = list(dict.fromkeys(all_lines))
            actual_nnodes = len(unique_nodes)
            master_addr = unique_nodes[0]
            print(f"PBS_NODEFILE contents: {all_lines}")
            print(f"Unique nodes ({actual_nnodes}): {unique_nodes}")
            print(f"Master: {master_addr}")
        else:
            actual_nnodes = 1
            master_addr = "localhost"
            print("No PBS_NODEFILE, single node mode")

        venv_path = "/eagle/SYNAPS-I/segmentation/env_dino_cellpose"

        # Build command as a list
        cmd_list = [
            f"{venv_path}/bin/python", "-m", "torch.distributed.run",
            f"--nnodes={actual_nnodes}",
            f"--nproc_per_node={nproc_per_node}",
            f"--rdzv_id={pbs_jobid}",
            "--rdzv_backend=c10d",
            f"--rdzv_endpoint={master_addr}:29500",
            "-m", script_module,
            "--input-dir", input_dir,
            "--output-dir", output_dir,
            "--batch-size", str(batch_size),
            "--finetuned-checkpoint", finetuned_checkpoint,
            "--save-overlay",
        ]

        # Environment variables
        env = os.environ.copy()
        env.update({
            "PATH": f"{venv_path}/bin:{env.get('PATH', '')}",
            "HF_HUB_CACHE": "/eagle/SYNAPS-I/segmentation/.cache/huggingface",
            "HF_HOME": "/eagle/SYNAPS-I/segmentation/.cache/huggingface",
            "CUDA_DEVICE_ORDER": "PCI_BUS_ID",
            "NCCL_NET_GDR_LEVEL": "PHB",
            "NCCL_CROSS_NIC": "1",
            "NCCL_COLLNET_ENABLE": "1",
            "NCCL_NET": "AWS Libfabric",
            "FI_CXI_DISABLE_HOST_REGISTER": "1",
            "FI_MR_CACHE_MONITOR": "userfaultfd",
            "FI_CXI_DEFAULT_CQ_SIZE": "131072",
        })

        # Prepend to LD_LIBRARY_PATH
        ld_path = env.get("LD_LIBRARY_PATH", "")
        env["LD_LIBRARY_PATH"] = f"/soft/libraries/aws-ofi-nccl/v1.9.1-aws/lib:/soft/libraries/hwloc/lib/:{ld_path}"

        if actual_nnodes > 1:
            # Use mpiexec to launch on all nodes
            command = [
                "mpiexec",
                "-n", str(actual_nnodes),
                "-ppn", "1",
                "-hostfile", pbs_nodefile,
                "--cpu-bind", "depth",
                "-d", "16",
            ] + cmd_list
        else:
            command = cmd_list

        print(f"Running: {' '.join(command)}")

        result = subprocess.run(command, env=env, cwd=workdir, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        print(f"STDOUT: {result.stdout[-3000:] if result.stdout else 'None'}")
        print(f"STDERR: {result.stderr[-3000:] if result.stderr else 'None'}")

        if result.returncode != 0:
            raise RuntimeError(
                f"Segmentation failed: {result.returncode}\n"
                f"STDERR: {result.stderr[-2000:] if result.stderr else 'None'}"
            )

        return f"DINO Segmentation completed in {time.time() - seg_start:.1f}s"

    def combine_segmentations(
        self,
        recon_folder_path: str = "",
    ) -> bool:
        """
        Run CPU-based combination of Cellpose+DINO and SAM3+DINO segmentation results at ALCF
        through Globus Compute.

        :param recon_folder_path: Path to the reconstructed data folder (e.g. 'folder/rec20250101_scan/')
        :return: True if the task completed successfully, False otherwise.
        """
        logger = get_run_logger()

        output_folder = recon_folder_path.replace("/rec", "/seg")
        seg_base = f"{self.allocation_root}/data/bl832/scratch/segmentation/{output_folder}"

        input_dir = f"{self.allocation_root}/data/bl832/scratch/reconstruction/{recon_folder_path}"
        sam3_results = f"{seg_base}/sam3"
        dino_results = f"{seg_base}/dino"
        combined_output = f"{seg_base}/combined"

        workdir = f"{self.allocation_root}/segmentation/scripts/inference_latest/forge_feb_seg_model_demo"

        gcc = Client(code_serialization_strategy=CombinedCode())

        endpoint_id = Variable.get(
            "alcf-globus-compute-seg-combine-uuid",
            default="4aae6420-3724-4df7-8884-81ff6c4c4381",
            _sync=True
        )

        with Executor(endpoint_id=endpoint_id, client=gcc) as fxe:
            logger.info(f"Running segmentation combination on {recon_folder_path} at ALCF")
            future = fxe.submit(
                self._combine_segmentations_wrapper,
                input_dir=input_dir,
                dino_results=dino_results,
                sam3_results=sam3_results,
                combined_output=combined_output,
                workdir=workdir,
                dilate_px=5
            )
            result = self._wait_for_globus_compute_future(future, "combine_segmentations", check_interval=10)

        return result

    @staticmethod
    def _combine_segmentations_wrapper(
        input_dir: str = "/eagle/SYNAPS-I/data/bl832/scratch/reconstruction/",
        dino_results: str = "/eagle/SYNAPS-I/data/bl832/scratch/segmentation/dino",
        sam3_results: str = "/eagle/SYNAPS-I/data/bl832/scratch/segmentation/sam3",
        combined_output: str = "/eagle/SYNAPS-I/data/bl832/scratch/segmentation/combined",
        workdir: str = "/eagle/SYNAPS-I/segmentation/scripts/inference_latest/forge_feb_seg_model_demo",
        dilate_px: int = 5,
    ) -> str:
        """
        Wrapper function to combine segmentation results from SAM+DINO.

        :param input_dir: Directory containing input data for segmentation.
        :param dino_results: Directory containing DINO segmentation results.
        :param sam3_results: Directory containing SAM3 segmentation results.
        :param combined_output: Directory to save combined segmentation outputs.
        :param workdir: Working directory for the combination script.
        :param dilate_px: Number of pixels to dilate the SAM masks for better coverage in the combination step.
        :return: Confirmation message upon completion.
        """
        import os
        import subprocess
        import time

        combine_start = time.time()
        os.chdir(workdir)

        venv_path = "/eagle/SYNAPS-I/segmentation/env_dino_cellpose"

        env = os.environ.copy()
        env.update({
            "PATH": f"{venv_path}/bin:{env.get('PATH', '')}",
            "HF_HUB_CACHE": "/eagle/SYNAPS-I/segmentation/.cache/huggingface",
            "HF_HOME": "/eagle/SYNAPS-I/segmentation/.cache/huggingface",
        })

        tasks = [
            {
                "name": "sam_dino",
                "module": "src.combine_sam_dino_v3",
                "args": [
                    "--input-dir", input_dir,
                    "--instance-masks-dir", sam3_results,
                    "--semantic-masks-dir", f"{dino_results}/semantic_masks",
                    "--output-dir", f"{combined_output}/sam_dino",
                    "--dilate-px", str(dilate_px),
                    "--save-extracted",
                    "--dino-trust", "Cortex", "Phloem_Fibers", "Phloem",
                                    "Air-based_Pith_cells", "Water-based_Pith_cells",
                ],
            },
        ]

        failed = []
        for combine_task in tasks:
            cmd = [f"{venv_path}/bin/python", "-m", combine_task["module"]] + combine_task["args"]
            print(f"Running {combine_task['name']}: {' '.join(cmd)}")

            result = subprocess.run(
                cmd,
                env=env,
                cwd=workdir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            print(f"STDOUT [{combine_task['name']}]: {result.stdout[-2000:] if result.stdout else 'None'}")
            print(f"STDERR [{combine_task['name']}]: {result.stderr[-2000:] if result.stderr else 'None'}")

            if result.returncode != 0:
                print(f"FAILED [{combine_task['name']}]: return code {result.returncode}")
                failed.append(combine_task["name"])
            else:
                print(f"SUCCESS [{combine_task['name']}]")

        if failed:
            raise RuntimeError(f"Segmentation combination failed for: {failed}")

        return f"Segmentation combination completed in {time.time() - combine_start:.1f}s"

    @staticmethod
    def _wait_for_globus_compute_future(
        future: Future,
        task_name: str,
        check_interval: int = 20,
        walltime: int = 3600  # seconds = 60 minutes
    ) -> bool:
        """
        Wait for a Globus Compute task to complete, assuming that if future.done() is False, the task is running.

        :param future: The future object returned from the Globus Compute Executor submit method.
        :param task_name: A descriptive name for the task being executed (used for logging).
        :param check_interval: The interval (in seconds) between status checks.
        :param walltime: The maximum time (in seconds) to wait for the task to complete.
        :return: True if the task completed successfully within walltime, False otherwise.
        """
        logger = get_run_logger()

        start_time = time.time()
        success = False

        try:
            previous_state = None
            while not future.done():
                elapsed_time = time.time() - start_time
                if elapsed_time > walltime:
                    logger.error(f"The {task_name} task exceeded the walltime of {walltime} seconds."
                                 "Cancelling the Globus Compute job.")
                    future.cancel()
                    return False

                # Check if the task was cancelled
                if future.cancelled():
                    logger.warning(f"The {task_name} task was cancelled.")
                    return False
                # Assume the task is running if not done and not cancelled
                elif previous_state != 'running':
                    logger.info(f"The {task_name} task is running...")
                    previous_state = 'running'

                time.sleep(check_interval)  # Wait before the next status check

            # Task is done, check if it was cancelled or raised an exception
            if future.cancelled():
                logger.warning(f"The {task_name} task was cancelled after completion.")
                return False

            exception = future.exception()
            if exception:
                logger.error(f"The {task_name} task raised an exception: {exception}")
                return False

            # Task completed successfully
            result = future.result()
            logger.info(f"The {task_name} task completed successfully with result: {result}")
            success = True

        except Exception as e:
            logger.error(f"An error occurred while waiting for the {task_name} task: {str(e)}")
            success = False

        finally:
            # Log the total time taken for the task
            elapsed_time = time.time() - start_time
            logger.info(f"Total duration of the {task_name} task: {elapsed_time:.2f} seconds.")

        return success


@flow(name="alcf_recon_flow", flow_run_name="alcf_recon-{file_path}")
def alcf_recon_flow(
    file_path: str,
    config: Optional[Config832] = None,
) -> bool:
    """
    Process and transfer a file from bl832 to ALCF and run reconstruction and segmentation.

    :param file_path: The path to the file to be processed.
    :param config: Configuration object for the flow.
    :return: True if the flow completed successfully, False otherwise.
    """
    logger = get_run_logger()

    if config is None:
        config = Config832()
    # set up file paths
    path = Path(file_path)
    folder_name = path.parent.name
    file_name = path.stem
    h5_file_name = file_name + '.h5'
    scratch_path_tiff = folder_name + '/rec' + file_name + '/'
    scratch_path_zarr = folder_name + '/rec' + file_name + '.zarr/'

    # initialize transfer_controller with globus
    logger.info("Initializing Globus Transfer Controller.")
    transfer_controller = get_transfer_controller(
        transfer_type=CopyMethod.GLOBUS,
        config=config
    )

    alcf_reconstruction_success = False
    alcf_multi_res_success = False
    data832_tiff_transfer_success = False
    data832_zarr_transfer_success = False

    # STEP 1: Transfer data from data832 to ALCF
    logger.info("Copying raw data to ALCF.")
    data832_raw_path = f"{folder_name}/{h5_file_name}"
    alcf_transfer_success = transfer_controller.copy(
        file_path=data832_raw_path,
        source=config.data832_raw,
        destination=config.alcf832_synaps_raw
    )
    logger.info(f"Transfer status: {alcf_transfer_success}")

    if not alcf_transfer_success:
        logger.error("Transfer failed due to configuration or authorization issues.")
        raise ValueError("Transfer to ALCF Failed")
    else:
        logger.info("Transfer to ALCF Successful.")

        # STEP 2: Run Tomopy Reconstruction on Globus Compute
        logger.info(f"Starting ALCF reconstruction flow for {file_path=}")

        # Initialize the Tomography Controller and run the reconstruction
        logger.info("Initializing ALCF Tomography HPC Controller.")
        tomography_controller = get_controller(
            hpc_type=HPC.ALCF,
            config=config
        )
        logger.info(f"Starting ALCF reconstruction task for {file_path=}")
        alcf_reconstruction_success = tomography_controller.reconstruct(
            file_path=file_path,
        )
        if not alcf_reconstruction_success:
            logger.error("Reconstruction Failed.")
            raise ValueError("Reconstruction at ALCF Failed")
        else:
            logger.info("Reconstruction Successful.")

            # STEP 3: Send reconstructed data (tiff) to data832
            logger.info(f"Transferring {file_name} from {config.alcf832_synaps_recon} "
                        f"at ALCF to {config.data832_scratch} at data832")
            data832_tiff_transfer_success = transfer_controller.copy(
                file_path=scratch_path_tiff,
                source=config.alcf832_synaps_recon,
                destination=config.data832_scratch
            )
            logger.info(f"Transfer reconstructed TIFF data to data832 success: {data832_tiff_transfer_success}")

            # STEP 4: Run the Tiff to Zarr Globus Flow
            logger.info(f"Starting ALCF tiff to zarr flow for {file_path=}")
            alcf_multi_res_success = tomography_controller.build_multi_resolution(
                file_path=file_path,
            )
            if not alcf_multi_res_success:
                logger.error("Tiff to Zarr Failed.")
                raise ValueError("Tiff to Zarr at ALCF Failed")
            else:
                logger.info("Tiff to Zarr Successful.")
                # STEP 5: Send reconstructed data (zarr) to data832
                logger.info(f"Transferring {file_name} from {config.alcf832_synaps_recon} "
                            f"at ALCF to {config.data832_scratch} at data832")
                data832_zarr_transfer_success = transfer_controller.copy(
                    file_path=scratch_path_zarr,
                    source=config.alcf832_synaps_recon,
                    destination=config.data832_scratch
                )

    # Place holder in case we want to transfer to NERSC for long term storage
    # nersc_transfer_success = False

    # STEP 6: Schedule Pruning of files
    logger.info("Scheduling file pruning tasks.")
    prune_controller = get_prune_controller(
        prune_type=PruneMethod.GLOBUS,
        config=config
    )

    # Prune from ALCF raw
    if alcf_transfer_success:
        logger.info("Scheduling pruning of ALCF raw data.")
        prune_controller.prune(
            file_path=data832_raw_path,
            source_endpoint=config.alcf832_synaps_raw,
            check_endpoint=None,
            days_from_now=2.0
        )

    # Prune TIFFs from ALCF scratch/reconstruction
    if alcf_reconstruction_success:
        logger.info("Scheduling pruning of ALCF scratch reconstruction data.")
        prune_controller.prune(
            file_path=scratch_path_tiff,
            source_endpoint=config.alcf832_synaps_recon,
            check_endpoint=config.data832_scratch,
            days_from_now=2.0
        )

    # Prune ZARR from ALCF scratch/reconstruction
    if alcf_multi_res_success:
        logger.info("Scheduling pruning of ALCF scratch zarr reconstruction data.")
        prune_controller.prune(
            file_path=scratch_path_zarr,
            source_endpoint=config.alcf832_synaps_recon,
            check_endpoint=config.data832_scratch,
            days_from_now=2.0
        )

    # Prune reconstructed TIFFs from data832 scratch
    if data832_tiff_transfer_success:
        logger.info("Scheduling pruning of data832 scratch reconstruction TIFF data.")
        prune_controller.prune(
            file_path=scratch_path_tiff,
            source_endpoint=config.data832_scratch,
            check_endpoint=None,
            days_from_now=30.0
        )

    # Prune reconstructed ZARR from data832 scratch
    if data832_zarr_transfer_success:
        logger.info("Scheduling pruning of data832 scratch reconstruction ZARR data.")
        prune_controller.prune(
            file_path=scratch_path_zarr,
            source_endpoint=config.data832_scratch,
            check_endpoint=None,
            days_from_now=30.0
        )

    # TODO: ingest to scicat

    if alcf_reconstruction_success and alcf_multi_res_success:
        return True
    else:
        return False


@flow(name="alcf_forge_recon_segment_flow", flow_run_name="alcf_recon_seg-{file_path}")
def alcf_forge_recon_segment_flow(
    file_path: str,
    config: Optional[Config832] = None,
) -> bool:
    """
    Process and transfer a file from bl832 to ALCF and run reconstruction and segmentation.

    :param file_path: The path to the file to be processed.
    :param config: Configuration object for the flow.
    :return: True if the flow completed successfully, False otherwise.
    """
    logger = get_run_logger()

    if config is None:
        config = Config832()
    # set up file paths
    path = Path(file_path)
    folder_name = path.parent.name
    file_name = path.stem
    h5_file_name = file_name + '.h5'
    scratch_path_tiff = folder_name + '/rec' + file_name + '/'
    scratch_path_zarr = folder_name + '/rec' + file_name + '.zarr/'
    scratch_path_segment = folder_name + '/seg' + file_name + '/'

    # initialize transfer_controller with globus
    logger.info("Initializing Globus Transfer Controller.")
    transfer_controller = get_transfer_controller(
        transfer_type=CopyMethod.GLOBUS,
        config=config
    )

    alcf_reconstruction_success = False
    alcf_segmentation_success = False
    data832_tiff_transfer_success = False
    segment_transfer_success = False

    # STEP 1: Transfer data from data832 to ALCF
    logger.info("Copying raw data to ALCF.")
    data832_raw_path = f"{folder_name}/{h5_file_name}"
    alcf_transfer_success = transfer_controller.copy(
        file_path=data832_raw_path,
        source=config.data832_raw,
        destination=config.alcf832_synaps_raw
    )
    logger.info(f"Transfer status: {alcf_transfer_success}")

    if not alcf_transfer_success:
        logger.error("Transfer failed due to configuration or authorization issues.")
        raise ValueError("Transfer to ALCF Failed")
    else:
        logger.info("Transfer to ALCF Successful.")

        # STEP 2: Run the Tomopy Reconstruction Globus Flow
        logger.info(f"Starting ALCF reconstruction flow for {file_path=}")

        # Initialize the Tomography Controller and run the reconstruction
        logger.info("Initializing ALCF Tomography HPC Controller.")
        tomography_controller = get_controller(
            hpc_type=HPC.ALCF,
            config=config
        )
        logger.info(f"Starting ALCF reconstruction task for {file_path=}")
        alcf_reconstruction_success = tomography_controller.reconstruct(
            file_path=file_path,
        )
        if not alcf_reconstruction_success:
            logger.error("Reconstruction Failed.")
            raise ValueError("Reconstruction at ALCF Failed")
        else:
            logger.info("Reconstruction Successful.")

            # STEP 3: Send reconstructed data (tiff) to data832
            logger.info(f"Transferring {file_name} from {config.alcf832_synaps_recon} "
                        f"at ALCF to {config.data832_scratch} at data832")
            data832_tiff_transfer_success = transfer_controller.copy(
                file_path=scratch_path_tiff,
                source=config.alcf832_synaps_recon,
                destination=config.data832_scratch
            )
            logger.info(f"Transfer reconstructed TIFF data to data832 success: {data832_tiff_transfer_success}")

            # STEP 4: Run the Segmentation Task at ALCF
            logger.info(f"Starting ALCF segmentation task for {scratch_path_tiff=}")
            alcf_segmentation_success = alcf_segmentation_sam3_task(
                recon_folder_path=scratch_path_tiff,
                config=config
            )
            if not alcf_segmentation_success:
                logger.warning("Segmentation at ALCF Failed")
            else:
                logger.info("Segmentation at ALCF Successful")

                # STEP 5: Send segmented data to data832
                logger.info(f"Transferring {file_name} from {config.alcf832_synaps_segment} "
                            f"at ALCF to {config.data832_scratch} at data832")
                segment_transfer_success = transfer_controller.copy(
                    file_path=scratch_path_segment,
                    source=config.alcf832_synaps_segment,
                    destination=config.data832_scratch
                )
                logger.info(f"Transfer segmented data to data832 success: {segment_transfer_success}")

            # Not running TIFF to Zarr conversion at ALCF for now
            # alcf_multi_res_success = False
            # data832_zarr_transfer_success = False
            # STEP 6: Run the Tiff to Zarr Globus Flow
            # logger.info(f"Starting ALCF tiff to zarr flow for {file_path=}")
            # alcf_multi_res_success = tomography_controller.build_multi_resolution(
            #     file_path=file_path,
            # )
            # if not alcf_multi_res_success:
            #     logger.error("Tiff to Zarr Failed.")
            #     raise ValueError("Tiff to Zarr at ALCF Failed")
            # else:
            #     logger.info("Tiff to Zarr Successful.")
            #     # STEP 7: Send reconstructed data (zarr) to data832
            #     logger.info(f"Transferring {file_name} from {config.alcf832_scratch} "
            #                 f"at ALCF to {config.data832_scratch} at data832")
            #     data832_zarr_transfer_success = transfer_controller.copy(
            #         file_path=scratch_path_zarr,
            #         source=config.alcf832_scratch,
            #         destination=config.data832_scratch
            #     )

            beegfs_zarr_transfer_success = transfer_controller.copy(
                file_path=scratch_path_zarr,
                source=config.alcf832_scratch,
                destination=config.beegfs_scratch
            )

            if beegfs_zarr_transfer_success:
                logger.info("Successfully transferred Zarr to beegfs. Now ingesting to Tiled.")
                register_file_to_tiled(
                    path=Path(config.beegfs_scratch.root_path+scratch_path_zarr),
                    prefix="beamlines/bl832/scratch",
                    overwrite=False,
                    tags=["8.3.2", folder_name],
                )
            else:
                logger.error("Failed to transfer Zarr to beegfs, skipping registration to Tiled.")

    # Place holder in case we want to transfer to NERSC for long term storage
    # nersc_transfer_success = False

    # STEP 8: Schedule Pruning of files
    logger.info("Scheduling file pruning tasks.")
    prune_controller = get_prune_controller(
        prune_type=PruneMethod.GLOBUS,
        config=config
    )

    # Prune from ALCF raw
    if alcf_transfer_success:
        logger.info("Scheduling pruning of ALCF raw data.")
        prune_controller.prune(
            file_path=data832_raw_path,
            source_endpoint=config.alcf832_synaps_raw,
            check_endpoint=None,
            days_from_now=2.0
        )

    # Prune TIFFs from ALCF scratch/reconstruction
    if alcf_reconstruction_success:
        logger.info("Scheduling pruning of ALCF scratch reconstruction data.")
        prune_controller.prune(
            file_path=scratch_path_tiff,
            source_endpoint=config.alcf832_synaps_recon,
            check_endpoint=config.data832_scratch,
            days_from_now=2.0
        )

    # Prune TIFFs from ALCF scratch/segmentation
    if alcf_segmentation_success:
        logger.info("Scheduling pruning of ALCF scratch segmentation data.")
        prune_controller.prune(
            file_path=scratch_path_segment,
            source_endpoint=config.alcf832_synaps_segment,
            check_endpoint=config.data832_scratch,
            days_from_now=2.0
        )

    # Prune reconstructed TIFFs from data832 scratch
    if data832_tiff_transfer_success:
        logger.info("Scheduling pruning of data832 scratch reconstruction TIFF data.")
        prune_controller.prune(
            file_path=scratch_path_tiff,
            source_endpoint=config.data832_scratch,
            check_endpoint=None,
            days_from_now=30.0
        )

    # Prune segmented data from data832 scratch
    if alcf_segmentation_success and segment_transfer_success:
        logger.info("Scheduling pruning of data832 scratch segmentation data.")
        prune_controller.prune(
            file_path=scratch_path_segment,
            source_endpoint=config.data832_scratch,
            check_endpoint=None,
            days_from_now=30.0
        )

    # TODO: ingest to scicat

    if alcf_reconstruction_success and alcf_segmentation_success:
        return True
    else:
        return False


@flow(name="alcf_forge_recon_multisegment_flow",
      flow_run_name="alcf_recon_multiseg-{file_path}")
def alcf_forge_recon_multisegment_flow(
    file_path: str,
    config: Optional[Config832] = None,
) -> bool:
    """
    Transfer raw data to ALCF, run multinode reconstruction synchronously,
    then run SAM3 and DINO segmentation concurrently.

    :param file_path: Path to the raw .h5 file (relative), e.g. 'folder/20250101_scan.h5'
    :param config: Optional Config832 instance.
    :return: True if reconstruction and all segmentation tasks succeeded, False otherwise.
    """
    logger = get_run_logger()

    if config is None:
        config = Config832()

    path = Path(file_path)
    folder_name = path.parent.name
    file_name = path.stem
    h5_file_name = file_name + ".h5"
    scratch_path_tiff = folder_name + "/rec" + file_name + "/"
    scratch_path_segment = folder_name + "/seg" + file_name + "/"

    # ── STEP 1: Transfer raw data to ALCF ────────────────────────────────────
    logger.info("Initializing Globus Transfer Controller.")
    transfer_controller = get_transfer_controller(
        transfer_type=CopyMethod.GLOBUS,
        config=config
    )

    data832_raw_path = f"{folder_name}/{h5_file_name}"
    logger.info(f"Transferring raw data to ALCF: {data832_raw_path}")
    alcf_transfer_success = transfer_controller.copy(
        file_path=data832_raw_path,
        source=config.data832_raw,
        destination=config.alcf832_synaps_raw
    )

    if not alcf_transfer_success:
        logger.error("Transfer to ALCF failed. Aborting flow.")
        raise ValueError("Transfer to ALCF Failed")

    logger.info("Transfer to ALCF successful.")

    # ── STEP 2: Multinode reconstruction (sync) ───────────────────────────────
    logger.info("Initializing ALCF Tomography HPC Controller.")
    tomography_controller = get_controller(
        hpc_type=HPC.ALCF,
        config=config
    )

    logger.info(f"Starting multinode reconstruction for {file_path=}")
    alcf_reconstruction_success = tomography_controller.reconstruct(file_path=file_path)

    if not alcf_reconstruction_success:
        logger.error("Reconstruction failed. Aborting segmentation steps.")
        raise ValueError("Reconstruction at ALCF Failed")

    logger.info("Reconstruction successful.")

    # ── STEP 3: Transfer reconstructed TIFFs back to data832 ─────────────────
    logger.info(f"Transferring TIFFs from ALCF to data832: {scratch_path_tiff}")
    data832_tiff_transfer_success = transfer_controller.copy(
        file_path=scratch_path_tiff,
        source=config.alcf832_synaps_recon,
        destination=config.data832_scratch
    )
    logger.info(f"TIFF transfer to data832: {data832_tiff_transfer_success}")

    # ── STEP 4: SAM3 / DINO concurrently ──────────────────────────
    logger.info("Submitting SAM3 and DINO segmentation tasks concurrently.")

    sam3_future = alcf_segmentation_sam3_task.submit(
        recon_folder_path=scratch_path_tiff, config=config
    )
    dino_future = alcf_segmentation_dino_task.submit(
        recon_folder_path=scratch_path_tiff, config=config
    )

    sam3_success = sam3_future.result()
    dino_success = dino_future.result()
    logger.info(f"Segmentation results — SAM3: {sam3_success}, DINO: {dino_success}")

    any_seg_success = any([sam3_success, dino_success])

    # ── STEP 5: Combine segmentation results (sync, CPU) ─────────────────────
    combine_success = False

    if dino_success and sam3_success:
        logger.info("Running segmentation combination (SAM3+DINO).")
        combine_success = tomography_controller.combine_segmentations(
            recon_folder_path=scratch_path_tiff
        )
        logger.info(f"Combination result: {combine_success}")
    else:
        logger.warning("Skipping combination: requires DINO plus SAM3.")

    # ── STEP 6: Transfer segmentation outputs to data832 ─────────────────────
    segment_transfer_success = False
    if any_seg_success:
        logger.info(f"Transferring segmentation outputs from ALCF to data832: {scratch_path_segment}")
        segment_transfer_success = transfer_controller.copy(
            file_path=scratch_path_segment,
            source=config.alcf832_synaps_segment,
            destination=config.data832_scratch
        )
        logger.info(f"Segmentation transfer to data832: {segment_transfer_success}")

    # ── STEP 7: Pruning ───────────────────────────────────────────────────────
    logger.info("Scheduling file pruning tasks.")
    prune_controller = get_prune_controller(
        prune_type=PruneMethod.GLOBUS,
        config=config
    )

    prune_controller.prune(
        file_path=data832_raw_path,
        source_endpoint=config.alcf832_synaps_raw,
        check_endpoint=None,
        days_from_now=2.0
    )

    prune_controller.prune(
        file_path=scratch_path_tiff,
        source_endpoint=config.alcf832_synaps_recon,
        check_endpoint=config.data832_scratch,
        days_from_now=2.0
    )

    if any_seg_success:
        prune_controller.prune(
            file_path=scratch_path_segment,
            source_endpoint=config.alcf832_synaps_segment,
            check_endpoint=config.data832_scratch,
            days_from_now=2.0
        )

    if data832_tiff_transfer_success:
        prune_controller.prune(
            file_path=scratch_path_tiff,
            source_endpoint=config.data832_scratch,
            check_endpoint=None,
            days_from_now=30.0
        )

    if segment_transfer_success:
        prune_controller.prune(
            file_path=scratch_path_segment,
            source_endpoint=config.data832_scratch,
            check_endpoint=None,
            days_from_now=30.0
        )

    # TODO: ingest to scicat

    return alcf_reconstruction_success and any_seg_success


@task(name="alcf_segmentation_sam3_task")
def alcf_segmentation_sam3_task(
    recon_folder_path: str,
    config: Optional[Config832] = None,
) -> bool:
    """
    Run segmentation task at ALCF.

    :param recon_folder_path: Path to the reconstructed data folder to be processed.
    :param config: Configuration object for the flow.
    :return: True if the task completed successfully, False otherwise.
    """
    logger = get_run_logger()
    if config is None:
        logger.info("No config provided, using default Config832.")
        config = Config832()

    # Initialize the Tomography Controller and run the segmentation
    logger.info("Initializing ALCF Tomography HPC Controller.")
    tomography_controller = get_controller(
        hpc_type=HPC.ALCF,
        config=config
    )
    logger.info(f"Starting ALCF segmentation task for {recon_folder_path=}")
    alcf_segmentation_success = tomography_controller.segmentation_sam3(
        recon_folder_path=recon_folder_path,
    )
    if not alcf_segmentation_success:
        logger.error("Segmentation Failed.")
    else:
        logger.info("Segmentation Successful.")
    return alcf_segmentation_success


@task(name="alcf_segmentation_dino_task")
def alcf_segmentation_dino_task(
    recon_folder_path: str,
    config: Optional[Config832] = None,
) -> bool:
    """
    Run DINO segmentation task at ALCF.

    :param recon_folder_path: Path to the reconstructed data folder to be processed.
    :param config: Configuration object for the flow.
    :return: True if the task completed successfully, False otherwise.
    """
    logger = get_run_logger()
    if config is None:
        config = Config832()
    tomography_controller = get_controller(hpc_type=HPC.ALCF, config=config)
    logger.info(f"Starting DINO segmentation task for {recon_folder_path=}")
    success = tomography_controller.segmentation_dino(recon_folder_path=recon_folder_path)
    logger.info(f"DINO segmentation {'successful' if success else 'failed'}.")
    return success


@task(name="alcf_combine_segmentations_task")
def alcf_combine_segmentations_task(
    recon_folder_path: str,
    config: Optional[Config832] = None,
) -> bool:
    """
    Run segmentation combination task at ALCF.

    :param recon_folder_path: Path to the reconstructed data folder to be processed.
    :param config: Configuration object for the flow.
    :return: True if the task completed successfully, False otherwise.
    """
    logger = get_run_logger()
    if config is None:
        config = Config832()
    tomography_controller = get_controller(hpc_type=HPC.ALCF, config=config)
    logger.info(f"Starting combine segmentation task for {recon_folder_path=}")
    success = tomography_controller.combine_segmentations(recon_folder_path=recon_folder_path)
    logger.info(f"Combine segmentation {'successful' if success else 'failed'}.")
    return success


@flow(name="alcf_segmentation_integration_test", flow_run_name="alcf_segmentation_integration_test")
def alcf_segmentation_integration_test() -> bool:
    """
    Integration test for the ALCF segmentation task.

    :return: True if the segmentation task completed successfully, False otherwise.
    """
    logger = get_run_logger()
    logger.info("Starting ALCF segmentation integration test.")
    recon_folder_path = 'DD-00842_hexemer/test_16'  # 'rec20211222_125057_petiole4'  # 'test'  #
    flow_success = alcf_segmentation_sam3_task(
        recon_folder_path=recon_folder_path,
        config=Config832()
    )
    logger.info(f"Flow success: {flow_success}")
    return flow_success


@flow(name="alcf_segmentation_dino_integration_test", flow_run_name="alcf_segmentation_dino_integration_test")
def alcf_segmentation_dino_integration_test() -> bool:
    """
    Integration test for the ALCF DINO segmentation task.

    :return: True if the segmentation task completed successfully, False otherwise.
    """
    logger = get_run_logger()
    logger.info("Starting ALCF segmentation DINO integration test.")
    recon_folder_path = 'DD-00842_hexemer/test_16'  # rec20260212_133951_petiole30'  # 'test'  #
    flow_success = alcf_segmentation_dino_task(
        recon_folder_path=recon_folder_path,
        config=Config832()
    )
    logger.info(f"Flow success: {flow_success}")
    return flow_success


@flow(name="alcf_combine_segmentations_integration_test", flow_run_name="alcf_combine_segmentations_integration_test")
def alcf_combine_segmentations_integration_test() -> bool:
    """
    Integration test for the ALCF combined segmentation task.

    :return: True if the segmentation task completed successfully, False otherwise.
    """
    logger = get_run_logger()
    logger.info("Starting ALCF segmentation combine integration test.")
    recon_folder_path = 'DD-00842_hexemer/test_16'  # rec20260212_133951_petiole30'  # 'test'  #
    flow_success = alcf_combine_segmentations_task(
        recon_folder_path=recon_folder_path,
        config=Config832()
    )
    logger.info(f"Flow success: {flow_success}")
    return flow_success


@flow(name="alcf_reconstruction_integration_test", flow_run_name="alcf_reconstruction_integration_test")
def alcf_reconstruction_integration_test() -> bool:
    """
    Integration test for the ALCF reconstruction task.

    :return: True if the reconstruction task completed successfully, False otherwise.
    """
    logger = get_run_logger()
    logger.info("Starting ALCF reconstruction integration test.")
    raw_file_path = '_ra-00823_bard/20251218_111600_silkraw.h5'  # 'test'  #

    tomography_controller = get_controller(
        hpc_type=HPC.ALCF,
        config=Config832()
    )

    flow_success = tomography_controller.reconstruct(
        file_path=f"{raw_file_path}",
    )

    logger.info(f"Flow success: {flow_success}")
    return flow_success
