"""Mounting a public cloud bucket into a Sandbox.

Mirrors ``modal.CloudBucketMount``: a plain value object describing a bucket to
expose at a path inside a Sandbox::

    sandbox = modal.Sandbox.create(
        image="python:3.13-slim",
        volumes={"/data": modal.CloudBucketMount("noaa-ghcn-pds", read_only=True)},
    )
    print(sandbox.exec("ls", "/data").stdout.read())

As on Modal, the bucket is mounted with `AWS Mountpoint
<https://github.com/awslabs/mountpoint-s3>`_ outside the sandbox, so file
semantics are Mountpoint's: reads work as anywhere, but writes must be
sequential, an existing file can only be replaced (not appended to), and
renames are not supported. Files show as owned by root with modes 0755 and
0644, as on Modal. The bucket stays reachable with ``block_network=True``,
because its traffic never passes through the sandbox's network.

Only **public** buckets can be mounted here: ``secret=`` and
``oidc_auth_role_arn=`` raise ``NotSupportedError``. Cloudflare R2 always
requires credentials, so R2 buckets cannot be mounted yet.

Like :class:`~ray.experimental.sandbox.modal.probe.Probe`, and like Modal's own
class, this is a plain dataclass rather than a ``synchronize_api`` wrapper: it
has no async methods. And like Modal's, it checks nothing when constructed;
everything is checked when it is passed to ``Sandbox.create``.
"""

import dataclasses
import logging
from typing import Any, Optional
from urllib.parse import urlparse

from ray.experimental.sandbox.config import BucketMount
from ray.experimental.sandbox.modal.exception import InvalidError, NotSupportedError
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)

# The endpoint hostname suffixes Modal tells providers apart by. Everything
# else is assumed to be S3, which keeps other S3-compatible servers working.
_R2_HOST_SUFFIX = "r2.cloudflarestorage.com"
_GCS_HOST_SUFFIX = "storage.googleapis.com"


@PublicAPI(stability="alpha")
@dataclasses.dataclass
class CloudBucketMount:
    """A public cloud bucket to mount inside a Sandbox.

    Pass one as a value of :meth:`Sandbox.create`'s ``volumes`` mapping. The
    same object may be used at more than one path.

    Attributes:
        bucket_name: Name of the bucket to mount.
        bucket_endpoint_url: Endpoint for a non-S3 service, such as
            ``"https://storage.googleapis.com"`` for Google Cloud Storage.
        key_prefix: Only expose keys under this prefix, which must end in "/".
        secret: Not supported: only public buckets can be mounted here.
        oidc_auth_role_arn: Not supported: only public buckets can be mounted
            here.
        read_only: Mount the bucket read-only.
        requester_pays: Requires credentials, as on Modal, so it cannot be
            used here.
        force_path_style: Address the bucket as ``<endpoint>/<bucket>`` rather
            than ``<bucket>.<endpoint>``, as some S3-compatible servers need.
    """

    bucket_name: str
    bucket_endpoint_url: Optional[str] = None
    key_prefix: Optional[str] = None
    secret: Optional[Any] = None
    oidc_auth_role_arn: Optional[str] = None
    read_only: bool = False
    requester_pays: bool = False
    force_path_style: bool = False

    def _bucket_type(self, log: bool = False) -> str:
        """The provider Modal infers from the endpoint: "S3", "R2" or "GCP".

        Modal's rule (``cloud_bucket_mounts_to_proto``): the endpoint's host
        names R2 or Google Cloud Storage by suffix, and anything else,
        including no endpoint at all, is S3. The mount itself does not depend
        on this here; mount-s3 is configured from the endpoint directly.

        Args:
            log: Log Modal's line for an unrecognized endpoint, as Modal does
                once per mount when a Sandbox is created.

        Returns:
            "S3", "R2" or "GCP".
        """
        if not self.bucket_endpoint_url:
            return "S3"
        hostname = urlparse(self.bucket_endpoint_url).hostname or ""
        if hostname.endswith(_R2_HOST_SUFFIX):
            return "R2"
        if hostname.endswith(_GCS_HOST_SUFFIX):
            return "GCP"
        if log:
            logger.info(
                "CloudBucketMount received unrecognized bucket endpoint URL. "
                "Assuming AWS S3 configuration as fallback."
            )
        return "S3"

    def _check(self) -> None:
        """Modal's own checks of a mount, in its order: the endpoint, then its
        two ``ValueError``\\ s, with its messages, so ported code catching them
        still catches them.

        Raises:
            ValueError: A combination Modal rejects too.
        """
        self._bucket_type(log=True)
        if self.requester_pays and not self.secret:
            raise ValueError("Credentials required in order to use Requester Pays.")
        if self.key_prefix and not self.key_prefix.endswith("/"):
            raise ValueError(
                "key_prefix will be prefixed to all object paths, so it must end in a '/'"
            )

    def _to_bucket_mount(self, mount_path: str) -> BucketMount:
        """Describe this mount for the sandbox runtime, refusing what Modal
        supports and this backend does not. Follows :meth:`_check`.

        Args:
            mount_path: Where the bucket is mounted in the sandbox, already
                checked by ``Sandbox.create``.

        Returns:
            The bucket mount the sandbox runtime takes.

        Raises:
            NotSupportedError: Credentials, which this backend does not take.
            InvalidError: A bucket name or endpoint Mountpoint cannot use.
        """
        for name, value in (
            ("secret", self.secret),
            ("oidc_auth_role_arn", self.oidc_auth_role_arn),
        ):
            if value is not None:
                raise NotSupportedError(
                    f"The '{name}' parameter of CloudBucketMount is not supported "
                    "by the Ray sandbox backend. Only public buckets can be mounted."
                )
        try:
            return BucketMount(
                bucket_name=self.bucket_name,
                mount_path=mount_path,
                endpoint_url=self.bucket_endpoint_url or None,
                key_prefix=self.key_prefix or None,
                read_only=bool(self.read_only),
                force_path_style=bool(self.force_path_style),
            )
        except ValueError as err:
            # A bucket name or endpoint Mountpoint cannot use. Modal accepts
            # these and fails later, on its side.
            raise InvalidError(str(err)) from None
