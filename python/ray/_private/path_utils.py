import pathlib
import urllib

"""Cross-platform utilities for manipulating paths and URIs.

NOTE: All functions in this file must support POSIX and Windows.
"""


def is_path(path_or_uri: str) -> bool:
    """Returns True if uri_or_path is a path and False otherwise.

    Windows paths start with a drive name which can be interpreted as
    a URI scheme by urlparse and thus needs to be treated differently
    form POSIX paths.

    E.g. Creating a directory returns the path 'C:\\Users\\mp5n6ul72w\\working_dir'
    will have the scheme 'C:'.
    """
    if not isinstance(path_or_uri, str):
        raise TypeError(f" path_or_uri must be a string, got {type(path_or_uri)}.")

    parsed_path = pathlib.Path(path_or_uri)
    parsed_uri = urllib.parse.urlparse(path_or_uri)

    if isinstance(parsed_path, pathlib.PurePosixPath):
        return not parsed_uri.scheme
    elif isinstance(parsed_path, pathlib.PureWindowsPath):
        return parsed_uri.scheme == parsed_path.drive.strip(":").lower()
    else:
        # this should never happen.
        raise TypeError(f"Unsupported path type: {type(parsed_path).__name__}")
