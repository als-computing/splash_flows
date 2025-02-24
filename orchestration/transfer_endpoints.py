from abc import ABC


class TransferEndpoint(ABC):
    """
    Abstract base class for endpoints.
    """
    def __init__(
        self,
        name: str,
        root_path: str,
        uri: str
    ) -> None:
        self.name = name
        self.root_path = root_path
        self.uri = uri

    def name(self) -> str:
        """
        A human-readable or reference name for the endpoint.
        """
        return self.name

    def root_path(self) -> str:
        """
        Root path or base directory for this endpoint.
        """
        return self.root_path

    def uri(self) -> str:
        """
        Root path or base directory for this endpoint.
        """
        return self.uri


class FileSystemEndpoint(TransferEndpoint):
    """
    A file system endpoint.

    Args:
        TransferEndpoint: Abstract class for endpoints.
    """
    def __init__(
        self,
        name: str,
        root_path: str,
        uri: str
    ) -> None:
        super().__init__(name, root_path, uri)

    def full_path(
        self,
        path_suffix: str
    ) -> str:
        """
        Constructs the full path by appending the path_suffix to the root_path.

        Args:
            path_suffix (str): The relative path to append.

        Returns:
            str: The full absolute path.
        """
        if path_suffix.startswith("/"):
            path_suffix = path_suffix[1:]
        return f"{self.root_path.rstrip('/')}/{path_suffix}"


class HPSSEndpoint(TransferEndpoint):
    """
    An HPSS endpoint.

    Args:
        TransferEndpoint: Abstract class for endpoints.
    """
    def __init__(
        self,
        name: str,
        root_path: str,
        uri: str
    ) -> None:
        super().__init__(name, root_path, uri)

    def full_path(self, path_suffix: str) -> str:
        """
        Constructs the full path by appending the path_suffix to the HPSS endpoint's root_path.
        This is used by the HPSS transfer controllers to compute the absolute path on HPSS.

        Args:
            path_suffix (str): The relative path to append.

        Returns:
            str: The full absolute path.
        """
        if path_suffix.startswith("/"):
            path_suffix = path_suffix[1:]
        return f"{self.root_path.rstrip('/')}/{path_suffix}"
