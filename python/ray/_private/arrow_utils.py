import json
from typing import Dict, Optional
from urllib.parse import urlencode, unquote, urlparse, parse_qsl, urlunparse

from packaging.version import Version
from packaging.version import parse as parse_version

_PYARROW_INSTALLED: Optional[bool] = None
_PYARROW_VERSION: Optional[Version] = None


def get_pyarrow_version() -> Optional[Version]:
    """Get the version of the pyarrow package or None if not installed."""
    global _PYARROW_INSTALLED, _PYARROW_VERSION
    if _PYARROW_INSTALLED is False:
        return None

    if _PYARROW_INSTALLED is None:
        try:
            import pyarrow

            _PYARROW_INSTALLED = True
            if hasattr(pyarrow, "__version__"):
                _PYARROW_VERSION = parse_version(pyarrow.__version__)
        except ModuleNotFoundError:
            _PYARROW_INSTALLED = False

    return _PYARROW_VERSION


def _add_url_query_params(url: str, params: Dict[str, str]) -> str:
    """Add params to the provided url as query parameters.

    If url already contains query parameters, they will be merged with params, with the
    existing query parameters overriding any in params with the same parameter name.

    Args:
        url: The URL to add query parameters to.
        params: The query parameters to add.

    Returns:
        URL with params added as query parameters.
    """
    # Unquote URL first so we don't lose existing args.
    url = unquote(url)
    # Parse URL.
    parsed_url = urlparse(url)
    # Merge URL query string arguments dict with new params.
    base_params = params
    params = dict(parse_qsl(parsed_url.query))
    base_params.update(params)
    # bool and dict values should be converted to json-friendly values.
    base_params.update(
        {
            k: json.dumps(v)
            for k, v in base_params.items()
            if isinstance(v, (bool, dict))
        }
    )

    # Convert URL arguments to proper query string.
    encoded_params = urlencode(base_params, doseq=True)
    # Replace query string in parsed URL with updated query string.
    parsed_url = parsed_url._replace(query=encoded_params)
    # Convert back to URL.
    return urlunparse(parsed_url)


def add_creatable_buckets_param_if_s3_uri(uri: str) -> str:
    """If the provided URI is an S3 URL, add allow_bucket_creation=true as a query
    parameter. For pyarrow >= 9.0.0, this is required in order to allow
    ``S3FileSystem.create_dir()`` to create S3 buckets.

    If the provided URI is not an S3 URL or if pyarrow < 9.0.0 is installed, we return
    the URI unchanged.

    Args:
        uri: The URI that we'll add the query parameter to, if it's an S3 URL.

    Returns:
        A URI with the added allow_bucket_creation=true query parameter, if the provided
        URI is an S3 URL; uri will be returned unchanged otherwise.
    """

    pyarrow_version = get_pyarrow_version()
    if pyarrow_version is not None and pyarrow_version < parse_version("9.0.0"):
        # This bucket creation query parameter is not required for pyarrow < 9.0.0.
        return uri
    parsed_uri = urlparse(uri)
    if parsed_uri.scheme == "s3":
        uri = _add_url_query_params(uri, {"allow_bucket_creation": True})
    return uri
