from pathlib import Path
import urllib.parse
import os
from typing import Union


class URI:
    """Represents a URI, supporting path appending and retrieving parent URIs.

    Example Usage:

        >>> s3_uri = URI("s3://bucket/a?scheme=http&endpoint_override=localhost%3A900")
        >>> s3_uri
        URI<s3://bucket/a?scheme=http&endpoint_override=localhost%3A900>
        >>> str(s3_uri / "b" / "c")
        's3://bucket/a/b/c?scheme=http&endpoint_override=localhost%3A900'
        >>> str(s3_uri.parent)
        's3://bucket?scheme=http&endpoint_override=localhost%3A900'
        >>> str(s3_uri)
        's3://bucket/a?scheme=http&endpoint_override=localhost%3A900'
        >>> s3_uri.parent.name, s3_uri.name
        ('bucket', 'a')

    Args:
        uri: The URI to represent.
            Ex: s3://bucket?scheme=http&endpoint_override=localhost%3A900
            Ex: file:///a/b/c/d
    """

    def __init__(self, uri: str):
        self._parsed = urllib.parse.urlparse(uri)
        if not self._parsed.scheme:
            raise ValueError(f"Invalid URI: {uri}")
        self._path = Path(os.path.normpath(self._parsed.netloc + self._parsed.path))

    @property
    def name(self) -> str:
        return self._path.name

    @property
    def parent(self) -> "URI":
        assert self._path.parent != ".", f"{str(self)} has no valid parent URI"
        return URI(self._get_str_representation(self._parsed, self._path.parent))

    def __truediv__(self, path_to_append):
        assert isinstance(path_to_append, str)
        return URI(
            self._get_str_representation(self._parsed, self._path / path_to_append)
        )

    @classmethod
    def _get_str_representation(
        cls, parsed_uri: urllib.parse.ParseResult, path: Union[str, Path]
    ) -> str:
        return parsed_uri._replace(netloc=str(path), path="").geturl()

    def __repr__(self):
        return f"URI<{str(self)}>"

    def __str__(self):
        return self._get_str_representation(self._parsed, self._path)
