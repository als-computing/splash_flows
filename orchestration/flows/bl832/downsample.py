import h5py
import cv2
import numpy as np
import time
import logging
import sys
import os
import torch
import psutil

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

DATASETS_TO_DOWNSAMPLE = [
    "exchange/data",
    "exchange/data_dark",
    "exchange/data_white"
]

FACTOR = 2
CHUNK_DEPTH = 32  # number of slices to process at once if chunking is needed

# Device selection for PyTorch-based GPU acceleration
if torch.backends.mps.is_available():
    device = torch.device("mps")
elif torch.cuda.is_available():
    device = torch.device("cuda")
else:
    device = torch.device("cpu")
logging.info(f"Using device: {device}")


def _validate_inputs(input_file: str, output_file: str) -> None:
    """
    Validate input and output files. Create output directory if needed.
    """
    if not os.path.isfile(input_file):
        raise FileNotFoundError(f"Input file not found: {input_file}")
    out_dir = os.path.dirname(output_file)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)


def _copy_file_structure(input_file: str, output_file: str) -> None:
    """
    Copy structure and attributes. For downsampled datasets, create placeholders.
    For others, copy the data fully.
    """
    with h5py.File(input_file, 'r') as h5in, h5py.File(output_file, 'w') as h5out:
        def copy_structure(src_group, dst_group):
            # Copy attributes
            for attr_name, attr_value in src_group.attrs.items():
                dst_group.attrs[attr_name] = attr_value
            # Copy groups/datasets
            for key, item in src_group.items():
                full_path = (src_group.name + '/' + key).lstrip('/')
                if isinstance(item, h5py.Group):
                    grp = dst_group.create_group(key)
                    copy_structure(item, grp)
                elif isinstance(item, h5py.Dataset):
                    in_list = (full_path in DATASETS_TO_DOWNSAMPLE)
                    if in_list:
                        # Placeholder
                        ds = dst_group.create_dataset(
                            key, shape=item.shape, dtype=item.dtype,
                            compression=item.compression,
                            compression_opts=item.compression_opts,
                            chunks=item.chunks
                        )
                        for a_name, a_value in item.attrs.items():
                            ds.attrs[a_name] = a_value
                    else:
                        # Copy fully
                        ds = dst_group.create_dataset(
                            key, data=item[()], dtype=item.dtype,
                            compression=item.compression,
                            compression_opts=item.compression_opts,
                            chunks=item.chunks
                        )
                        for a_name, a_value in item.attrs.items():
                            ds.attrs[a_name] = a_value
        copy_structure(h5in, h5out)


def _downsample_opencv(data: np.ndarray, factor: int = FACTOR) -> np.ndarray:
    """
    Downsample using OpenCV. Supports 2D and 3D data.
    """
    if data.ndim == 2:
        h, w = data.shape
        new_w = w // factor
        new_h = h // factor
        return cv2.resize(data, (new_w, new_h), interpolation=cv2.INTER_LINEAR)
    elif data.ndim == 3:
        d, h, w = data.shape
        new_w = w // factor
        new_h = h // factor
        out = np.empty((d, new_h, new_w), dtype=data.dtype)
        for i in range(d):
            out[i] = cv2.resize(data[i], (new_w, new_h), interpolation=cv2.INTER_LINEAR)
        return out
    else:
        raise ValueError("Data must be 2D or 3D.")


def _downsample_torch_block(data_t: torch.Tensor, factor: int) -> torch.Tensor:
    """
    Downsample using PyTorch. Supports 2D and 3D data.
    """
    if data_t.ndim == 2:
        h, w = data_t.shape
        return data_t.view(h // factor, factor, w // factor, factor).mean(dim=(1, 3))
    elif data_t.ndim == 3:
        d, h, w = data_t.shape
        return data_t.view(d, h // factor, factor, w // factor, factor).mean(dim=(2, 4))
    else:
        raise ValueError("Data must be 2D or 3D.")


def _downsample_torch_full(data: np.ndarray) -> np.ndarray:
    """
    Downsample using PyTorch, fully in memory.
    """
    data_float32 = data.astype(np.float32, copy=False)
    t = torch.from_numpy(data_float32).to(device)
    downsampled = _downsample_torch_block(t, FACTOR).cpu().numpy()
    return downsampled


def _downsample_torch_chunked(dset_in: h5py.Dataset, ds_out: h5py.Dataset) -> None:
    """
    Downsample using PyTorch, chunked processing.
    """
    d, h, w = dset_in.shape
    for start in range(0, d, CHUNK_DEPTH):
        end = min(start + CHUNK_DEPTH, d)
        chunk_data = dset_in[start:end, :, :]
        data_float32 = chunk_data.astype(np.float32, copy=False)
        t = torch.from_numpy(data_float32).to(device)
        downsampled_chunk = _downsample_torch_block(t, FACTOR).cpu().numpy()
        ds_out[start:end, :, :] = downsampled_chunk


def _should_chunk(dset_in: h5py.Dataset) -> bool:
    """
    Check if chunked processing is needed for the dataset (for torch).
    """
    shape = dset_in.shape
    size_bytes = np.prod(shape) * 4  # float32 = 4 bytes
    avail_mem = psutil.virtual_memory().available
    return size_bytes > avail_mem / 4


def opencv_downsample_h5(input_file: str, output_file: str) -> None:
    """
    Downsample using OpenCV, always fully in memory.
    """
    _validate_inputs(input_file, output_file)
    _copy_file_structure(input_file, output_file)
    # Perform downsampling
    with h5py.File(input_file, 'r') as h5in, h5py.File(output_file, 'a') as h5out:
        for dname in DATASETS_TO_DOWNSAMPLE:
            if dname in h5out and dname in h5in:
                data = h5in[dname][()]
                try:
                    downsampled = _downsample_opencv(data, FACTOR)
                    del h5out[dname]
                    ds = h5out.create_dataset(dname, data=downsampled, compression="gzip")
                    for a_name, a_value in h5in[dname].attrs.items():
                        ds.attrs[a_name] = a_value
                except Exception as e:
                    logging.error(f"Failed to downsample {dname}: {e}")
                    if dname not in h5out:
                        ds = h5out.create_dataset(dname, data=data)
                        for a_name, a_value in h5in[dname].attrs.items():
                            ds.attrs[a_name] = a_value


def torch_downsample_h5(input_file: str, output_file: str) -> None:
    """
    Downsample using Torch. If the dataset is too large (>2x available memory), use chunking.
    """
    _validate_inputs(input_file, output_file)
    _copy_file_structure(input_file, output_file)

    with h5py.File(input_file, 'r') as h5in, h5py.File(output_file, 'a') as h5out:
        for dname in DATASETS_TO_DOWNSAMPLE:
            if dname in h5out and dname in h5in:
                dset_in = h5in[dname]
                shape = dset_in.shape
                ndims = len(shape)
                del h5out[dname]

                if ndims == 2:
                    h, w = shape
                    if h % FACTOR != 0 or w % FACTOR != 0:
                        raise ValueError(f"Dimensions of {dname} not divisible by factor {FACTOR}.")
                    data = dset_in[()]
                    try:
                        downsampled = _downsample_torch_full(data)
                        ds_out = h5out.create_dataset(dname, data=downsampled, compression="gzip")
                        for a_name, a_value in dset_in.attrs.items():
                            ds_out.attrs[a_name] = a_value
                    except Exception as e:
                        logging.error(f"Failed to downsample {dname}: {e}")
                        ds_out = h5out.create_dataset(dname, data=data)
                        for a_name, a_value in dset_in.attrs.items():
                            ds_out.attrs[a_name] = a_value

                elif ndims == 3:
                    d, h, w = shape
                    if h % FACTOR != 0 or w % FACTOR != 0:
                        raise ValueError(f"Dimensions of {dname} not divisible by factor {FACTOR}.")
                    new_shape = (d, h // FACTOR, w // FACTOR)
                    ds_out = h5out.create_dataset(dname, shape=new_shape, dtype=np.float32, compression="gzip")
                    for a_name, a_value in dset_in.attrs.items():
                        ds_out.attrs[a_name] = a_value

                    if _should_chunk(dset_in):
                        # Chunked downsampling
                        logging.info(f"Using chunked downsampling for {dname}")
                        _downsample_torch_chunked(dset_in, ds_out)
                    else:
                        # Try full read
                        try:
                            data = dset_in[()]
                            downsampled = _downsample_torch_full(data)
                            ds_out[...] = downsampled
                        except RuntimeError as e:
                            logging.warning(f"Full downsampling failed for {dname}, using chunking: {e}")
                            del h5out[dname]
                            ds_out = h5out.create_dataset(dname, shape=new_shape, dtype=np.float32, compression="gzip")
                            for a_name, a_value in dset_in.attrs.items():
                                ds_out.attrs[a_name] = a_value
                            _downsample_torch_chunked(dset_in, ds_out)
                else:
                    raise ValueError("Data must be 2D or 3D.")


def main():
    # If no arguments: run example usage
    if len(sys.argv) < 2:
        input_file = 'examples/tomo_scan.h5'
        # input_file = 'examples/20230606_151124_jong-seto_fungal-mycelia_roll-AQ_fungi1_fast.h5'
        output_opencv = 'examples/downsample_cv_tomo_scan.h5'
        output_torch = 'examples/downsample_torch_tomo_scan.h5'

        start_time = time.time()
        opencv_downsample_h5(input_file, output_opencv)
        elapsed_opencv = time.time() - start_time
        logging.info(f"OpenCV-based downsampling took {elapsed_opencv:.2f} seconds.")

        start_time = time.time()
        torch_downsample_h5(input_file, output_torch)
        elapsed_torch = time.time() - start_time
        logging.info(f"PyTorch-based (GPU) downsampling took {elapsed_torch:.2f} seconds.")
        sys.exit(0)

    if len(sys.argv) < 4:
        print("Usage: python script.py <mode> <input.h5> <output.h5>")
        print("mode: opencv or torch")
        sys.exit(1)

    mode = sys.argv[1]
    input_file = sys.argv[2]
    output_file = sys.argv[3]

    start_time = time.time()
    if mode == "opencv":
        opencv_downsample_h5(input_file, output_file)
    elif mode == "torch":
        torch_downsample_h5(input_file, output_file)
    else:
        print("Unknown mode. Use 'opencv' or 'torch'.")
        sys.exit(1)

    elapsed = time.time() - start_time
    logging.info(f"{mode.capitalize()} downsampling took {elapsed:.2f} seconds.")


if __name__ == '__main__':
    main()
