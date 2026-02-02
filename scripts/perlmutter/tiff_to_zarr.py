#!/usr/bin/env python

import argparse
import os
import sys

import dxchange
from ngff_zarr import (
    detect_cli_io_backend,
    cli_input_to_ngff_image,
    to_multiscales,
    to_ngff_zarr,
    Methods
)


def parse_arguments():
    parser = argparse.ArgumentParser(description='Convert TIFF files to NGFF Zarr format.')
    parser.add_argument('tiff_directory', type=str, help='Directory containing the TIFF files.')
    parser.add_argument('--zarr_directory',
                        type=str,
                        default=None,
                        help='Directory to store Zarr output. Default is new folder in input directory.')
    parser.add_argument('--raw_file',
                        type=str,
                        default=None,
                        help='Path to the raw hdf5 file (for reading pixelsize metadata).')

    return parser.parse_args()


def debug_log(message):
    """Writes a debug message to stdout, ensuring it appears in SLURM logs."""
    sys.stdout.write(f"{message}\n")
    sys.stdout.flush()


def set_permissions_recursive(path, permissions=0o2775):
    for root, dirs, files in os.walk(path):
        for dir in dirs:
            os.chmod(os.path.join(root, dir), permissions)
        for file in files:
            os.chmod(os.path.join(root, file), permissions)
    os.chmod(path, permissions)  # Also set permissions for the top-level directory


def read_pixelsize_from_hdf5(raw_file: str) -> dict:
    pxsize = dxchange.read_hdf5(raw_file,
                                "/measurement/instrument/detector/pixel_size")[0]  # Expect mm
    pxsize = pxsize * 1000  # Convert to micrometer
    return {'x': pxsize, 'y': pxsize, 'z': pxsize}


def main():
    args = parse_arguments()

    tiff_dir = args.tiff_directory
    zarr_dir = args.zarr_directory
    
    # Debugging: Print the TIFF directory and current working directory
    debug_log(f"Specified TIFF directory: {tiff_dir}")
    debug_log(f"Current working directory: {os.getcwd()}")

    # Debugging: List contents of the current working directory
    debug_log("Contents of the current working directory:")
    debug_log(", ".join(os.listdir(os.getcwd())))

    if not os.path.isdir(tiff_dir):
        raise TypeError("The specified TIFF directory is not a valid directory")

    file_names = os.listdir(tiff_dir)
    file_paths = [os.path.join(tiff_dir, file_name) for file_name in file_names if not file_name.startswith('.')]
    file_paths.sort()

    if not file_paths:
        raise ValueError("No TIFF files found in the specified directory")
    if not zarr_dir:
        last_part = os.path.basename(os.path.normpath(tiff_dir))
        zarr_dir = os.path.abspath(os.path.join(tiff_dir, '..', last_part + '.zarr'))
    if not os.path.exists(zarr_dir):
        os.makedirs(zarr_dir, mode=0o2775, exist_ok=True)

    print('Output directory: ' + zarr_dir)

    # Build NGFF Zarr directory
    backend = detect_cli_io_backend(file_paths)
    image = cli_input_to_ngff_image(backend, file_paths)
    # The scale and axis units are the same as the one printed in the reconstruction script
    image.scale = read_pixelsize_from_hdf5(args.raw_file)
    image.axes_units = {'x': 'micrometer', 'y': 'micrometer', 'z': 'micrometer'}
    multiscales = to_multiscales(image, method=Methods.DASK_IMAGE_GAUSSIAN, cache=False)
    to_ngff_zarr(zarr_dir, multiscales)
    print('NGFF Zarr created')

    # Set permissions for the output directory and its contents
    set_permissions_recursive(zarr_dir)

    # Extract and set permissions for the parent directory (folder_name)
    parent_dir = os.path.abspath(os.path.join(tiff_dir, '../'))  # Extract parent directory
    set_permissions_recursive(parent_dir)  # Set permissions for parent directory


if __name__ == "__main__":
    main()
