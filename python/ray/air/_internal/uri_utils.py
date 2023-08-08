from pathlib import Path
import urllib.parse
import os
from typing import Union


class URI:
    """Represents a URI, supporting path appending and retrieving parent URIs.

    Example Usage:

        >>> s3_uri = URI("s3://bucket/a?scheme=http&param=1")
        >>> s3_uri
        URI<s3://bucket/a?scheme=http&param=1>
        >>> str(s3_uri / "b" / "c")
        's3://bucket/a/b/c?scheme=http&param=1'
        >>> str(s3_uri.parent)
        's3://bucket?scheme=http&param=1'
        >>> str(s3_uri)
        's3://bucket/a?scheme=http&param=1'
        >>> s3_uri.parent.name, s3_uri.name
        ('bucket', 'a')
        >>> local_path = URI("/tmp/local")
        >>> str(local_path)
        '/tmp/local'
        >>> str(local_path.parent)
        '/tmp'
        >>> str(local_path / "b" / "c")
        '/tmp/local/b/c'

    Args:
        uri: The URI to represent.
            Ex: s3://bucket?scheme=http&endpoint_override=localhost%3A900
            Ex: file:///a/b/c/d
    """

    def __init__(self, uri: str):
        self._parsed = urllib.parse.urlparse(uri)
        if not self._parsed.scheme:
            # Just treat this as a regular path
            self._path = Path(uri)
        else:
            self._path = Path(os.path.normpath(self._parsed.netloc + self._parsed.path))

    def rstrip_subpath(self, subpath: Path) -> "URI":
        """Returns a new URI that strips the given subpath from the end of this URI.

        Example:
            >>> uri = URI("s3://bucket/a/b/c/?param=1")
            >>> str(uri.rstrip_subpath(Path("b/c")))
            's3://bucket/a?param=1'

            >>> uri = URI("/tmp/a/b/c/")
            >>> str(uri.rstrip_subpath(Path("/b/c/.//")))
            '/tmp/a'

        """
        assert str(self._path).endswith(str(subpath)), (self._path, subpath)
        stripped_path = str(self._path).replace(str(subpath), "")
        return URI(self._get_str_representation(self._parsed, stripped_path))

    @property
    def name(self) -> str:
        return self._path.name

    @property
    def parent(self) -> "URI":
        assert self._path.parent != ".", f"{str(self)} has no valid parent URI"
        return URI(self._get_str_representation(self._parsed, self._path.parent))

    @property
    def scheme(self) -> str:
        return self._parsed.scheme

    @property
    def path(self) -> str:
        return str(self._path)

    def __truediv__(self, path_to_append):
        assert isinstance(path_to_append, str)
        return URI(
            self._get_str_representation(self._parsed, self._path / path_to_append)
        )

    @classmethod
    def _get_str_representation(
        cls, parsed_uri: urllib.parse.ParseResult, path: Union[str, Path]
    ) -> str:
        if not parsed_uri.scheme:
            return str(path)
        return parsed_uri._replace(netloc=str(path), path="").geturl()

    def __repr__(self):
        return f"URI<{str(self)}>"

    def __str__(self):
        return self._get_str_representation(self._parsed, self._path)


def is_uri(path: str) -> bool:
    return bool(urllib.parse.urlparse(path).scheme)


def _join_path_or_uri(base_path: str, path_to_join: str) -> str:
    """Joins paths to form either a URI (w/ possible URL params) or a local path.

    Example:

        >>> local_path = "/a/b"
        >>> uri = "s3://bucket/a?scheme=http"
        >>> path_to_join = "c/d"
        >>> _join_path_or_uri(local_path, path_to_join)
        '/a/b/c/d'
        >>> _join_path_or_uri(uri, path_to_join)
        's3://bucket/a/c/d?scheme=http'

    """
    from ray.air._internal.remote_storage import is_local_path

    base_path_or_uri = Path(base_path) if is_local_path(base_path) else URI(base_path)
    return str(base_path_or_uri / path_to_join)
