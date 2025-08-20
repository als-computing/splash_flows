from dotenv import load_dotenv
import os
import time
import typer

import globus_sdk
from prefect import flow, task, get_run_logger
from typing import Tuple, Optional

load_dotenv()

# Set the client ID and fetch client secret from environment
CLIENT_ID: Optional[str] = os.getenv('GLOBUS_CLIENT_ID')
CLIENT_SECRET: Optional[str] = os.getenv('GLOBUS_CLIENT_SECRET')
SCOPES: str = "urn:globus:auth:scope:transfer.api.globus.org:all"

app = typer.Typer()


@task
def initialize_transfer_client() -> Tuple[Optional[globus_sdk.TransferClient], bool]:
    """
    Initialize and return a Globus TransferClient using confidential client credentials.

    Returns:
        Tuple[Optional[globus_sdk.TransferClient], bool]: The TransferClient object and a success flag.
    """
    logger = get_run_logger()
    try:
        logger.info("Initializing TransferClient...")
        confidential_client = globus_sdk.ConfidentialAppAuthClient(client_id=CLIENT_ID,
                                                                   client_secret=CLIENT_SECRET)
        cc_authorizer = globus_sdk.ClientCredentialsAuthorizer(confidential_client, SCOPES)
        transfer_client = globus_sdk.TransferClient(authorizer=cc_authorizer)
        logger.info("TransferClient initialized successfully.")
        return transfer_client, True
    except Exception as e:
        logger.error(f"Failed to initialize TransferClient: {str(e)}")
        return None, False


@task
def check_permissions(transfer_client: globus_sdk.TransferClient, endpoint_id: str) -> bool:
    """
    Check and log file permissions and other relevant details of a Globus transfer endpoint.

    Args:
        transfer_client (globus_sdk.TransferClient): An authenticated TransferClient object.
        endpoint_id (str): The UUID of the endpoint to check.

    Returns:
        bool: True if successful, False otherwise.
    """
    logger = get_run_logger()
    try:
        endpoint = transfer_client.get_endpoint(endpoint_id)
        logger.info(f"Endpoint ID: {endpoint['id']}")
        logger.info(f"Endpoint display name: {endpoint['display_name']}")
        logger.info(f"Endpoint owner: {endpoint['owner_string']}")
        return True
    except globus_sdk.GlobusAPIError as err:
        logger.error(f"Error fetching endpoint information: {err.message}")
        return False


@task
def list_directory(transfer_client: globus_sdk.TransferClient, endpoint_id: str, path: str = "") -> bool:
    """
    List the contents of a specified directory on a Globus endpoint.

    Args:
        transfer_client (globus_sdk.TransferClient): An authenticated TransferClient object.
        endpoint_id (str): The UUID of the endpoint to list the directory on.
        path (str): The path of the directory to list.

    Returns:
        bool: True if successful, False otherwise.
    """
    logger = get_run_logger()
    start_time = time.time()
    success = False
    try:
        response = transfer_client.operation_ls(endpoint_id, path=path)
        logger.info(f"Contents of {path} in endpoint {endpoint_id}:")
        if not response:
            logger.info(f"No contents found in {path}.")
        for item in response:
            logger.info(f"{item['type']} - {item['name']}")
        success = True
    except globus_sdk.GlobusAPIError as err:
        logger.error(f"Error accessing {path} in endpoint {endpoint_id}: {err.message}")
    finally:
        elapsed_time = time.time() - start_time
        logger.info(f"list_directory task took {elapsed_time:.2f} seconds")
        return success


@task
def create_directory(transfer_client: globus_sdk.TransferClient,
                     endpoint_id: str,
                     base_path: str = "",
                     directory_name: str = "test_directory/") -> bool:
    """
    Create a directory on a specified Globus endpoint.

    Args:
        transfer_client (globus_sdk.TransferClient): An authenticated TransferClient object.
        endpoint_id (str): The UUID of the endpoint to create the directory on.
        base_path (str): The base path where the directory will be created.
        directory_name (str): The name of the directory to create.

    Returns:
        bool: True if successful, False otherwise.
    """
    logger = get_run_logger()
    start_time = time.time()
    success = False
    try:
        if not base_path.endswith('/'):
            base_path += '/'
        if base_path.startswith('/'):
            base_path = base_path.lstrip('/')

        full_path = base_path + directory_name

        if full_path.startswith('/'):
            raise ValueError(f"Invalid directory path: {full_path}")

        transfer_client.operation_mkdir(endpoint_id, full_path)
        logger.info(f"Successfully created directory {full_path} in endpoint {endpoint_id}.")
        success = True
    except (ValueError, globus_sdk.GlobusAPIError) as err:
        logger.error(f"Error creating directory {full_path} in endpoint {endpoint_id}: {str(err)}")
    finally:
        elapsed_time = time.time() - start_time
        logger.info(f"create_directory task took {elapsed_time:.2f} seconds")
        return success


@task
def create_test_file(transfer_client: globus_sdk.TransferClient,
                     endpoint_id: str,
                     directory_path: str = "test_directory/",
                     file_name: str = "test.txt",
                     content: str = "This is a test file.") -> bool:
    """
    Create a file on a Globus endpoint by uploading provided content.

    Args:
        transfer_client (globus_sdk.TransferClient): Authenticated TransferClient.
        endpoint_id (str): UUID of the endpoint.
        directory_path (str): Path to the directory where file will be created.
        file_name (str): Name of the file to create.
        content (str): Text content to write to the file.

    Returns:
        bool: True if successful, False otherwise.
    """
    logger = get_run_logger()
    start_time = time.time()
    success = False
    try:
        if not directory_path.endswith('/'):
            directory_path += '/'
        full_path = directory_path + file_name

        # Create a temporary local file
        import tempfile
        with tempfile.NamedTemporaryFile("w+", delete=False) as tmp_file:
            tmp_file.write(content)
            tmp_file_path = tmp_file.name

        # Upload the file to the endpoint
        transfer_client.operation_upload(tmp_file_path, endpoint_id, full_path)
        logger.info(f"Successfully created file {full_path} in endpoint {endpoint_id}.")
        success = True
    except globus_sdk.GlobusAPIError as err:
        logger.error(f"Error creating file {full_path} in endpoint {endpoint_id}: {err.message}")
    finally:
        elapsed_time = time.time() - start_time
        logger.info(f"create_test_file task took {elapsed_time:.2f} seconds")
        return success

# @task
# def remove_directory(transfer_client: globus_sdk.TransferClient, endpoint_id: str, path: str) -> bool:
#     """
#     Remove a directory on a specified Globus endpoint.

#     Args:
#         transfer_client (globus_sdk.TransferClient): An authenticated TransferClient object.
#         endpoint_id (str): The UUID of the endpoint where the directory is located.
#         path (str): The path of the directory to remove.

#     Returns:
#         bool: True if successful, False otherwise.
#     """
#     logger = get_run_logger()
#     start_time = time.time()
#     success = False
#     try:
#         # delete_data = globus_sdk.DeleteData(transfer_client, endpoint_id, recursive=True)
#         # delete_data.add_item(path)
#         # transfer_result = transfer_client.submit_delete(delete_data)
#         logger.info(f"Successfully submitted request to remove directory {path} in endpoint {endpoint_id}.")
#         logger.info(f"Task ID: {transfer_result['task_id']}")
#         logger.info(f"transfer result:  {transfer_result}")
#         success = True
#     except globus_sdk.GlobusAPIError as err:
#         logger.error(f"Error removing directory {path} in endpoint {endpoint_id}: {err.message}")
#     finally:
#         elapsed_time = time.time() - start_time
#         logger.info(f"remove_directory task took {elapsed_time:.2f} seconds")
#         return success


@flow(name="check-globus-transfer")
def check_globus_transfer_permissions(endpoint_id: str,
                                      transfer_client: Optional[globus_sdk.TransferClient],
                                      list_contents: bool = True,
                                      create_test_directory: bool = True,
                                      delete_test_directory: bool = False,
                                      directory_name: str = "test_directory/") -> None:
    """
    Check permissions, list directory contents, create a directory,
    and remove a directory on a Globus endpoint.

    Args:
        endpoint_id (str): The UUID of the endpoint to check.
        transfer_client (Optional[globus_sdk.TransferClient]): An authenticated TransferClient object or None.
        list_contents (bool, optional): Whether to list directory contents. Default is True.
        create_test_directory (bool, optional): Whether to create a directory. Default is True.
        delete_test_directory (bool, optional): Whether to delete the directory. Default is True.
        directory_name (str, optional): The name of the directory to create/delete. Default is "test_directory/".
    """
    logger = get_run_logger()
    if transfer_client is None:
        transfer_client, success = initialize_transfer_client()
        if not success or transfer_client is None:
            logger.error("Failed to initialize TransferClient. Exiting flow.")
            return

    success_check_permissions = check_permissions(transfer_client, endpoint_id)
    logger.info(f"check_permissions successful: {success_check_permissions}")

    if list_contents:
        logger.info("Listing / directory:")
        success_list_directory = list_directory(transfer_client, endpoint_id, "")
        logger.info(f"list_directory successful: {success_list_directory}")

    if create_test_directory:
        success_create_directory = create_directory(transfer_client, endpoint_id, "", directory_name)
        logger.info(f"create_directory successful: {success_create_directory}")

    # if delete_test_directory:
    #     success_remove_directory = remove_directory(transfer_client, endpoint_id, directory_name)
    #     logger.info(f"remove_directory successful: {success_remove_directory}")

    if list_contents and create_test_directory:
        logger.info(f"Listing / directory after creating {directory_name}:")
        success_list_directory_after = list_directory(transfer_client, endpoint_id, directory_name)
        logger.info(f"list_directory (after creating {directory_name}) successful: {success_list_directory_after}")


@app.command()
def main(endpoint_id: str,
         list_contents: bool = True,
         create_test_directory: bool = True,
         delete_test_directory: bool = False,
         directory_name: str = "test_directory/") -> None:
    """
    Main function to parse command-line arguments and run the check_globus_transfer_permissions flow.

    Run from the command line:
    python check_globus_transfer.py "your-endpoint-id" --directory-name "your-directory-name"

    Args:
        endpoint_id (str): The UUID of the Globus endpoint.
        list_contents (bool): Whether to list directory contents. Default is True.
        create_test_directory (bool): Whether to create a directory. Default is True.
        delete_test_directory (bool): Whether to delete the directory. Default is True.
        directory_name (str): The name of the directory to create or delete. Default is "test_directory/".
    """

    check_globus_transfer_permissions(
        endpoint_id=endpoint_id,
        transfer_client=None,
        list_contents=list_contents,
        create_test_directory=create_test_directory,
        delete_test_directory=delete_test_directory,
        directory_name=directory_name
    )


if __name__ == "__main__":
    app()
