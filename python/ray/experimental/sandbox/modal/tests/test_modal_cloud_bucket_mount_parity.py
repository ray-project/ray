"""Modal's own CloudBucketMount tests, ported to this backend.

Ported from modal-client 02613d5d (2026-08-19); modal 1.6.1 carries the same
class. Each test names the test it ports. Only public mounts and their use in
Sandboxes are ported:

* ``py/test/sandbox_test.py``: the sandbox CloudBucketMount and
  ``reload_volumes`` tests.
* ``py/test/mount_utils_test.py``: mount-path and value validation.
* ``py/test/cloud_bucket_mount_test.py``: the one test, moved from a Function
  to a Sandbox and without its secret.
* ``go/cloud_bucket_mount_test.go`` and ``js/test/cloud_bucket_mount.test.ts``:
  construction, defaults, provider detection, validation, and what a mount
  turns into.
* ``go/sandbox_test.go`` and ``js/test/sandbox.test.ts``: the CloudBucketMount
  parts of the sandbox create request.

Not ported, because they need what this backend does not offer: every test
that passes a ``secret`` (``..._credentials``, ``..._credentials_multiple``,
``..._credentials_from_name``, ``..._credentials_from_dict_and_from_name``,
``TestSandboxCreateHydratesCloudBucketMountSecret``), the OIDC tests
(``..._oidc_auth_role_arn``, ``..._OidcIdentityToken``), and the
``modal.Volume``, ``Cls.with_options`` and ``Image.run_commands`` tests.

Modal's tests intercept the create request its client sends to Modal's
servers. The equivalent here is what ``Sandbox.create`` hands the sandbox
actor: the :class:`BucketMount` list in its create arguments, and the mount-s3
command each one turns into. The ``intercept`` fixture captures them without a
Ray cluster.
"""

import logging
import sys

import pytest

from ray.experimental.sandbox._internal import bucket_mount
from ray.experimental.sandbox.config import BucketMount
from ray.experimental.sandbox.modal import sandbox as sandbox_mod
from ray.experimental.sandbox.modal.cloud_bucket_mount import CloudBucketMount
from ray.experimental.sandbox.modal.exception import InvalidError
from ray.experimental.sandbox.modal.sandbox import Sandbox, _validate_volumes


class _Intercepted:
    """What ``Sandbox.create`` asked the sandbox actor for."""

    create_kwargs = None

    @property
    def bucket_mounts(self):
        return self.create_kwargs.get("bucket_mounts", [])


@pytest.fixture
def intercept(monkeypatch):
    """Stand in for the sandbox actor and capture its create arguments.

    The counterpart of Modal's ``servicer.intercept()``: ``Sandbox.create``
    runs in full, and the request it would have sent is recorded.
    """
    record = _Intercepted()

    def _reply(value):
        async def _awaitable():
            return value

        return _awaitable()

    class _Method:
        def __init__(self, value):
            self._value = value

        def remote(self, *args, **kwargs):
            return _reply(self._value)

    class _Actor:
        start = _Method({"instance_id": "ray-sandbox-intercepted", "image_config": {}})
        launch_main = _Method("main-exec")
        # Where each bucket mount really is: as configured, here.
        exec_collect = _Method({"returncode": 1, "stdout": b"", "stderr": b""})

    class _ActorClass:
        def options(self, **options):
            return self

        def remote(self, create_kwargs, timeout, force_pull, readiness_probe):
            record.create_kwargs = create_kwargs
            return _Actor()

    monkeypatch.setattr(sandbox_mod, "_SandboxActor", _ActorClass())
    monkeypatch.setattr(sandbox_mod, "require_ray_connection", lambda: None)
    return record


def _argv(mount: BucketMount):
    """The mount-s3 command line a BucketMount turns into."""
    return bucket_mount.build_mount_command(mount, "/mnt/0", "/mnt/0-home")[0]


# -- py/test/sandbox_test.py ------------------------------------------------------


def test_experimental_sandbox_create_cloud_bucket_mount(intercept):
    """Ports ``test_experimental_sandbox_create_cloud_bucket_mount``."""
    cbm = CloudBucketMount(bucket_name="my-bucket")
    Sandbox.create("echo", "hi", volumes={"/mnt": cbm})
    assert len(intercept.bucket_mounts) == 1
    assert intercept.bucket_mounts[0].bucket_name == "my-bucket"
    assert intercept.bucket_mounts[0].mount_path == "/mnt"
    assert cbm._bucket_type() == "S3"
    # An S3 bucket: no endpoint, so mount-s3 reaches AWS S3 itself.
    assert not any(
        a.startswith("--endpoint-url") for a in _argv(intercept.bucket_mounts[0])
    )


def test_experimental_sandbox_create_cloud_bucket_mount_no_credentials(
    intercept, monkeypatch
):
    """Ports ``test_experimental_sandbox_create_cloud_bucket_mount_no_credentials``.

    Modal sends no credentials for a mount without a secret. Here no
    credentials exist to send: the mount is anonymous, and mount-s3 gets none
    of the node's own, even when the node has some.
    """
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "node-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "node-secret")
    cbm = CloudBucketMount(bucket_name="my-bucket")
    Sandbox.create("echo", "hi", volumes={"/mnt": cbm})
    (mount,) = intercept.bucket_mounts
    argv, env = bucket_mount.build_mount_command(mount, "/mnt/0", "/mnt/0-home")
    assert "--no-sign-request" in argv
    assert not {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"} & set(env)
    assert "secret" not in intercept.create_kwargs
    assert not any(
        "node-" in value for value in intercept.create_kwargs["env"].values()
    )


@pytest.mark.parametrize("timeout", [0, -5])
def test_sandbox_reload_volumes_timeout_validation(intercept, timeout):
    """Ports ``test_sandbox_reload_volumes_timeout_validation`` (v1 and v2).

    Modal has two create paths; this backend has one.
    """
    sb = Sandbox.create(
        "sleep", "infinity", volumes={"/mnt": CloudBucketMount("my-bucket")}
    )
    with pytest.raises(InvalidError, match="must be positive"):
        sb.reload_volumes(timeout=timeout)


def test_sandbox_reload_volumes(intercept):
    """Ports ``test_sandbox_reload_volumes`` and ``..._v2``.

    Modal's tests check that the call reaches its command router. Here there
    is nothing to reload, so the observable part is that both forms succeed.
    """
    sb = Sandbox.create(
        "sleep", "infinity", volumes={"/mnt": CloudBucketMount("my-bucket")}
    )
    assert sb.reload_volumes() is None
    assert sb.reload_volumes(timeout=17) is None


# -- py/test/mount_utils_test.py ------------------------------------------------------


class _NetworkFileSystem:
    """Stands in for ``modal.NetworkFileSystem``, which this backend lacks."""


def test_validate_mount_points():
    """Ports ``test_validate_mount_points`` (a dict is required)."""
    _validate_volumes({"/foo/bar": CloudBucketMount("bucket")})
    with pytest.raises(InvalidError, match="volume_likes"):
        _validate_volumes([CloudBucketMount("bucket")])


@pytest.mark.parametrize("path", ["/", "/root", "/tmp", "foo/bar"])
def test_validate_mount_points_invalid_paths(path):
    """Ports ``test_validate_mount_points_invalid_paths``.

    Modal's helper takes the display name as an argument; for volumes it is
    "Volume", which is what its messages here start with.
    """
    with pytest.raises(InvalidError, match="Volume"):
        _validate_volumes({path: CloudBucketMount("bucket")})


def test_validate_volumes():
    """Ports ``test_validate_volumes``: a value that is not a volume is refused,
    naming its type. (Its duplicate ``modal.Volume`` half does not apply.)"""
    _validate_volumes({"/my/path": CloudBucketMount("bucket")})
    with pytest.raises(InvalidError, match="_NetworkFileSystem"):
        _validate_volumes({"/my/path": _NetworkFileSystem()})


# -- py/test/cloud_bucket_mount_test.py ------------------------------------------------------


def test_volume_mount(intercept):
    """Ports ``test_volume_mount``: an R2 bucket with a key prefix, writable,
    mounted at /root/foo. Moved from a Function to a Sandbox, and without its
    secret: R2 always requires credentials, so this mount could not actually
    be served here, but it is accepted and described exactly as Modal would.
    """
    cld_bckt_mnt = CloudBucketMount(
        bucket_name="foo",
        key_prefix="dir/",
        bucket_endpoint_url="https://1234.r2.cloudflarestorage.com",
        read_only=False,
    )
    Sandbox.create("echo", "hi", volumes={"/root/foo": cld_bckt_mnt})
    assert intercept.bucket_mounts == [
        BucketMount(
            "foo",
            "/root/foo",
            endpoint_url="https://1234.r2.cloudflarestorage.com",
            key_prefix="dir/",
            read_only=False,
        )
    ]
    assert cld_bckt_mnt._bucket_type() == "R2"
    argv = _argv(intercept.bucket_mounts[0])
    assert "--region=auto" in argv and "--prefix=dir/" in argv


# -- go/cloud_bucket_mount_test.go, js/test/cloud_bucket_mount.test.ts --------------


def test_new_cloud_bucket_mount_minimal_options():
    """Ports ``TestNewCloudBucketMount_MinimalOptions`` and the JS
    ``create() with minimal options``."""
    mount = CloudBucketMount("my-bucket")
    assert mount.bucket_name == "my-bucket"
    assert mount.read_only is False
    assert mount.requester_pays is False
    assert mount.secret is None
    assert mount.bucket_endpoint_url is None
    assert mount.key_prefix is None
    assert mount.oidc_auth_role_arn is None
    assert mount._bucket_type() == "S3"


def test_new_cloud_bucket_mount_all_public_options():
    """Ports ``TestNewCloudBucketMount_AllOptions`` and the JS ``create() with
    all options``, without the secret, requester-pays and OIDC options."""
    mount = CloudBucketMount(
        "my-bucket",
        read_only=True,
        bucket_endpoint_url="https://my-bucket.r2.cloudflarestorage.com",
        key_prefix="prefix/",
    )
    assert mount.bucket_name == "my-bucket"
    assert mount.read_only is True
    assert mount.bucket_endpoint_url == "https://my-bucket.r2.cloudflarestorage.com"
    assert mount.key_prefix == "prefix/"
    assert mount._bucket_type() == "R2"


@pytest.mark.parametrize(
    "endpoint_url,expected",
    [
        ("", "S3"),  # Empty defaults to S3
        ("https://my-bucket.r2.cloudflarestorage.com", "R2"),
        ("https://storage.googleapis.com/my-bucket", "GCP"),
        ("https://unknown-endpoint.com/my-bucket", "S3"),  # Unknown defaults to S3
    ],
)
def test_bucket_type_detection(endpoint_url, expected, caplog):
    """Ports ``TestBucketTypeDetection`` and the JS ``bucket type detection
    from endpoint URLs``, plus what each type means for the mount."""
    mount = CloudBucketMount("my-bucket", bucket_endpoint_url=endpoint_url)
    assert mount._bucket_type() == expected
    with caplog.at_level(logging.INFO):
        (bucket,) = _validate_volumes({"/mnt": mount})
    assert ("unrecognized bucket endpoint URL" in caplog.text) == (
        endpoint_url == "https://unknown-endpoint.com/my-bucket"
    )
    argv = _argv(bucket)
    assert ("--region=auto" in argv) == (expected == "R2")
    # Every non-AWS service gets mount-s3's extra upload checksums turned off.
    assert ("--upload-checksums=off" in argv) == bool(endpoint_url)


def test_new_cloud_bucket_mount_invalid_url():
    """Ports ``TestNewCloudBucketMount_InvalidURL`` and the JS ``://invalid-url``
    case. Go and JS reject it when the mount is built; Modal's Python client
    accepts it (as S3) and its server fails later. This backend rejects it when
    the Sandbox is created, before anything starts."""
    with pytest.raises(InvalidError, match="endpoint_url"):
        _validate_volumes(
            {
                "/mnt": CloudBucketMount(
                    "my-bucket", bucket_endpoint_url="://invalid-url"
                )
            }
        )


def test_new_cloud_bucket_mount_validation_requester_pays_without_secret():
    """Ports ``TestNewCloudBucketMount_ValidationRequesterPaysWithoutSecret``
    and its JS twin, with the Python client's exception and wording."""
    with pytest.raises(ValueError) as err:
        _validate_volumes({"/mnt": CloudBucketMount("my-bucket", requester_pays=True)})
    assert str(err.value) == "Credentials required in order to use Requester Pays."


def test_new_cloud_bucket_mount_validation_key_prefix_without_trailing_slash():
    """Ports ``TestNewCloudBucketMount_ValidationKeyPrefixWithoutTrailingSlash``
    and its JS twin, with the Python client's exception and wording."""
    with pytest.raises(ValueError) as err:
        _validate_volumes({"/mnt": CloudBucketMount("my-bucket", key_prefix="prefix")})
    assert str(err.value) == (
        "key_prefix will be prefixed to all object paths, so it must end in a '/'"
    )


def test_cloud_bucket_mount_to_proto_minimal_options():
    """Ports ``TestCloudBucketMount_ToProtoMinimalOptions`` and the JS
    ``toProto() with minimal options``: what the mount turns into."""
    (bucket,) = _validate_volumes({"/mnt/bucket": CloudBucketMount("my-bucket")})
    assert bucket.bucket_name == "my-bucket"
    assert bucket.mount_path == "/mnt/bucket"
    assert bucket.read_only is False
    assert bucket.endpoint_url is None
    assert bucket.key_prefix is None
    assert "--no-sign-request" in _argv(bucket)  # no credentials


def test_cloud_bucket_mount_to_proto_all_public_options():
    """Ports ``TestCloudBucketMount_ToProtoAllOptions`` and the JS ``toProto()
    with all options``, without the secret, requester-pays and OIDC options."""
    mount = CloudBucketMount(
        "my-bucket",
        read_only=True,
        bucket_endpoint_url="https://storage.googleapis.com/my-bucket",
        key_prefix="prefix/",
    )
    (bucket,) = _validate_volumes({"/mnt/bucket": mount})
    assert bucket.bucket_name == "my-bucket"
    assert bucket.mount_path == "/mnt/bucket"
    assert bucket.read_only is True
    assert bucket.endpoint_url == "https://storage.googleapis.com/my-bucket"
    assert bucket.key_prefix == "prefix/"
    assert mount._bucket_type() == "GCP"
    argv = _argv(bucket)
    assert "--read-only" in argv and "--prefix=prefix/" in argv


# -- go/sandbox_test.go, js/test/sandbox.test.ts ------------------------------------


def test_sandbox_create_request_cloud_bucket_mounts(intercept):
    """Ports the CloudBucketMount half of
    ``TestSandboxCreateV2RequestProto_VolumesAndCloudBucketMounts`` and the JS
    ``supports volumes and cloud bucket mounts``."""
    Sandbox.create("echo", "hi", volumes={"/mnt/s3": CloudBucketMount("my-bucket")})
    assert len(intercept.bucket_mounts) == 1
    assert intercept.bucket_mounts[0].mount_path == "/mnt/s3"
    assert intercept.bucket_mounts[0].bucket_name == "my-bucket"


def test_sandbox_create_request_has_no_cloud_bucket_mounts_by_default(intercept):
    """Ports the ``CloudBucketMounts`` line of the Go and JS default-request
    tests: a Sandbox created without volumes mounts no buckets."""
    Sandbox.create("echo", "hi")
    assert intercept.bucket_mounts == []


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
