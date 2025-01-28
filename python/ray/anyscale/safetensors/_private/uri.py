import os
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Callable, Dict, Optional, Tuple
from urllib.parse import ParseResult, quote_plus, urlparse

if TYPE_CHECKING:
    import botocore
    import google.oauth2


@dataclass(frozen=True)
class URIInfo:
    uri: str
    download_url: str
    download_headers: Dict
    cache_prefix: str


_URIParser = Callable[[ParseResult, Optional[str]], URIInfo]


def _urijoin(base: str, *subpaths: str) -> str:
    """Join the base URI with the subpaths.

    The base may be empty.

    The resulting URI will not contain any leading or trailing slashes.
    """
    result = base.strip("/")
    for subpath in subpaths:
        subpath = subpath.strip("/")
        if len(result) > 0:
            result += "/" + subpath
        else:
            result = subpath

    return result.strip("/")


def _get_cache_prefix(parsed_uri: ParseResult) -> str:
    return _urijoin(
        parsed_uri.scheme,
        parsed_uri.netloc,
        os.path.dirname(parsed_uri.path),
    )


def _transform_anyscale_uri(
    uri_info: ParseResult, region: Optional[str]
) -> Tuple[ParseResult, str]:
    """Transforms an 'anyscale://' URI to its corresponding cloud provider URI.

    Also returns the region of the storage bucket if one was not specified.

    Raises:
        ValueError if the ANYSCALE_ARTIFACT_STORAGE env var is not populated or
        the region is not provided and the ANYSCALE_CLOUD_STORAGE_BUCKET_REGION
        env var is not populated.
    """
    assert uri_info.scheme == "anyscale", "Only anyscale:// URIs should be passed."

    # Populate the URI prefix from the ANYSCALE_ARTIFACT_STORAGE env var.
    artifact_storage = os.getenv("ANYSCALE_ARTIFACT_STORAGE")
    if artifact_storage is None:
        raise ValueError(
            "ANYSCALE_ARTIFACT_STORAGE env var not detected; "
            "anyscale:// URIs can only be used inside an Anyscale cluster."
        )

    artifact_storage_uri = _urijoin(artifact_storage, uri_info.netloc, uri_info.path)

    # Populate the region from the ANYSCALE_CLOUD_STORAGE_BUCKET_REGION env var.
    artifact_storage_region = os.getenv("ANYSCALE_CLOUD_STORAGE_BUCKET_REGION")
    if region is None:
        if artifact_storage_region is None:
            raise ValueError(
                "ANYSCALE_CLOUD_STORAGE_BUCKET_REGION env var not "
                "detected, so region must be manually specified."
            )

        region = artifact_storage_region
    elif artifact_storage_region is not None and region != artifact_storage_region:
        raise ValueError(
            "Region was manually specified but does not match "
            "ANYSCALE_CLOUD_STORAGE_BUCKET_REGION. When using an 'anyscale://' URI, "
            "region does not need to be specified."
        )

    return urlparse(artifact_storage_uri), region


def parse_uri_info(uri: str, *, region: Optional[str] = None) -> URIInfo:
    if uri.endswith("/"):
        raise ValueError("URI must not end with '/'")

    parsed_uri: ParseResult = urlparse(uri)
    if not parsed_uri.scheme:
        raise ValueError(
            "URI missing scheme. Must be of the form: 'scheme://path/to/dir'"
        )

    if parsed_uri.scheme == "anyscale":
        parsed_uri, region = _transform_anyscale_uri(parsed_uri, region)

    # Scheme must always be populated above.
    assert parsed_uri.scheme
    _supported_scheme_to_parser: Dict[str, _URIParser] = {
        "http": parse_http_uri,
        "https": parse_http_uri,
        "s3": parse_aws_s3_uri,
        "gs": parse_gcp_gs_uri,
    }
    parser = _supported_scheme_to_parser.get(parsed_uri.scheme)
    if parser is None:
        raise ValueError(
            f"Unrecognized URI scheme '{parsed_uri.scheme}'. "
            "Supported values are: {list(_supported_scheme_to_parser.keys())}."
        )

    return parser(parsed_uri, region)


def parse_http_uri(parsed_uri: ParseResult, region: Optional[str]) -> URIInfo:
    return URIInfo(
        uri=parsed_uri.geturl(),
        download_url=parsed_uri.geturl(),
        download_headers={},
        cache_prefix=_get_cache_prefix(parsed_uri),
    )


def _parse_bucket_name_and_object_path(parsed_uri: ParseResult) -> Tuple[str, str]:
    """Parses & validates the bucket name and path for a URI.

    Raises:
        ValueError if either are not populated.
    """
    bucket_name = parsed_uri.netloc
    object_path = parsed_uri.path.strip("/")

    if not bucket_name or not object_path:
        raise ValueError(
            "Malformed URI, must be of the form: "
            f"'{parsed_uri.scheme}://<bucket_name>/<object_path>'"
        )

    return bucket_name, object_path


def _get_aws_credentials() -> "botocore.credentials.Credentials":
    """Get credentials for use with AWS REST APIs.

    May be overridden for unit testing.

    Raises:
        RuntimeError if boto3 is not installed or if valid credentials
        can't be generated.
    """
    try:
        from boto3 import Session
    except ImportError as e:
        raise RuntimeError(
            "An s3:// URI was provided but boto3 is not installed. "
            "Install it by using: 'pip install boto3'"
        ) from e

    session = Session()
    credentials = session.get_credentials()
    if credentials is None:
        raise RuntimeError("Failed to fetch AWS credentials.")
    return credentials.get_frozen_credentials()


def parse_aws_s3_uri(parsed_uri: ParseResult, region: Optional[str]) -> URIInfo:
    try:
        from botocore.auth import SigV4Auth
        from botocore.awsrequest import AWSRequest
    except ImportError as e:
        raise RuntimeError(
            "An s3:// URI was provided but boto3 is not installed. "
            "Install it using: 'pip install boto3'"
        ) from e

    if region is None:
        raise ValueError("Region must be provided for s3:// URIs.")

    credentials = _get_aws_credentials()

    # Parse the URI and construct the corresponding HTTP request.
    bucket_name, object_path = _parse_bucket_name_and_object_path(parsed_uri)
    download_url = f"https://{bucket_name}.s3.{region}.amazonaws.com/{object_path}"

    # Create and sign the request metadata.
    aws_request = AWSRequest(
        method="GET",
        url=download_url,
        headers={
            "Host": f"{bucket_name}.s3.{region}.amazonaws.com",
            "x-amz-content-sha256": "UNSIGNED-PAYLOAD",
            "x-amz-date": datetime.utcnow().strftime("%Y%m%dT%H%M%SZ"),
        },
        data=None,
    )
    SigV4Auth(credentials, "s3", region).add_auth(aws_request)

    return URIInfo(
        uri=f"s3://{bucket_name}/{object_path}",
        download_url=download_url,
        # Extract the headers containing the signature.
        download_headers=dict(aws_request.headers.items()),
        cache_prefix=_get_cache_prefix(parsed_uri),
    )


def _get_gcp_credentials() -> "google.oauth2.credentials.Credentials":
    """Get credentials for use with GCP REST APIs.

    May be overridden for unit testing.

    Raises:
        RuntimeError if google.auth is not installed or if valid credentials
        can't be generated.
    """
    try:
        import google.auth
        import google.auth.transport.requests
    except ImportError as e:
        raise RuntimeError(
            "A gs:// URI was provided but the Google Cloud SDK is not installed. "
            "Install it by following: 'https://cloud.google.com/sdk/docs/install'."
        ) from e

    # Generate credentials to be used in authorization header.
    credentials, _ = google.auth.default()
    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)
    if not credentials.valid:
        raise RuntimeError(
            "Failed to refresh GCP credentials. "
            "Ensure you have logged in using 'gcloud auth login'."
        )

    return credentials


def parse_gcp_gs_uri(parsed_uri: ParseResult, region: Optional[str]) -> URIInfo:
    credentials = _get_gcp_credentials()

    # Parse the URI and construct the corresponding HTTP request.
    bucket_name, object_path = _parse_bucket_name_and_object_path(parsed_uri)
    url_safe_object_path = quote_plus(object_path)
    return URIInfo(
        uri=f"gs://{bucket_name}/{object_path}",
        download_url=f"https://storage.googleapis.com/storage/v1/b/{bucket_name}/o/{url_safe_object_path}?alt=media",  # noqa: E501
        download_headers={"Authorization": f"Bearer {credentials.token}"},
        cache_prefix=_get_cache_prefix(parsed_uri),
    )
