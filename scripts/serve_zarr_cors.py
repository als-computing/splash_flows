"""
Use this script to launch a local HTTP server to serve a directory containing Zarr files.
The script will automatically open the URL to the itkwidgets in the default web browser.
To use this script, run the following command in the terminal:

    python serve_zarr_cors.py {relative_directory_path}

Note: this works if the Zarr folder is stored in the same directory as this script.

itk-vtk-viewer documentation: https://kitware.github.io/itk-vtk-viewer/docs/viewer.html
"""

import http.server
import socketserver
import webbrowser
import sys
import os


class CORSHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        # Allow CORS for all domains
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        super().end_headers()

    def translate_path(self, path):
        # Ensure hidden files like .zattrs are served correctly
        path = super().translate_path(path)
        print(f"Serving path: {path}")  # Debugging line to see the path being served
        return path


def start_server(directory, port=8000):
    handler = CORSHTTPRequestHandler
    handler.directory = directory
    with socketserver.TCPServer(("", port), handler) as httpd:
        print(f"Serving {directory} at http://localhost:{port}")

        # Remove leading slash if present
        directory_path = directory.lstrip('/')

        # Construct the URL for itk-vtk-viewer
        url = f"https://kitware.github.io/itk-vtk-viewer/app/?rotate=false&image=http://localhost:{port}/{directory_path}"

        # Automatically open the URL in the default web browser
        webbrowser.open(url)

        # Start the server to serve files
        httpd.serve_forever()


if __name__ == "__main__":
    # Check if a directory path is provided as a command-line argument
    if len(sys.argv) < 2:
        print("Usage: python serve_zarr_cors.py {file_path}")
        sys.exit(1)

    # Get the directory path from the command-line argument
    directory = sys.argv[1]

    # Check if the directory exists
    if not os.path.isdir(directory):
        print(f"Error: The directory '{directory}' does not exist.")
        sys.exit(1)

    # Start the server
    start_server(directory)
