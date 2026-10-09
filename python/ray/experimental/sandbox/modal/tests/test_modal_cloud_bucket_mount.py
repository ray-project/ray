"""``modal.CloudBucketMount`` and ``Sandbox.create(volumes=...)``, checked
against Modal's client (modal-client 02613d5d; modal 1.6.1 has the same class).

Everything here is refused or translated before any Ray connection, so no
cluster is needed.
"""

import asyncio
import dataclasses
import logging
import os
import sys
from pathlib import PurePosixPath

import pytest

from ray.experimental.sandbox import modal
from ray.experimental.sandbox.config import BucketMount
from ray.experimental.sandbox.modal.cloud_bucket_mount import CloudBucketMount
from ray.experimental.sandbox.modal.exception import InvalidError, NotSupportedError
from ray.experimental.sandbox.modal.sandbox import (
    _resolve_bucket_mounts,
    _Sandbox,
    _validate_volumes,
)

# Modal's fields, in order, with their defaults (py/modal/cloud_bucket_mount.py).
MODAL_FIELDS = [
    ("bucket_name", dataclasses.MISSING),
    ("bucket_endpoint_url", None),
    ("key_prefix", None),
    ("secret", None),
    ("oidc_auth_role_arn", None),
    ("read_only", False),
    ("requester_pays", False),
    ("force_path_style", False),
]


def test_class_matches_modals():
    assert [
        (f.name, f.default) for f in dataclasses.fields(CloudBucketMount)
    ] == MODAL_FIELDS
    assert modal.CloudBucketMount is CloudBucketMount
    assert "CloudBucketMount" in modal.__all__


def test_positional_construction():
    mount = CloudBucketMount("bucket", "https://storage.googleapis.com", "dir/")
    assert mount.bucket_endpoint_url == "https://storage.googleapis.com"
    assert mount.key_prefix == "dir/"


def test_unhashable_and_mutable_like_modals():
    mount = CloudBucketMount("bucket")
    with pytest.raises(TypeError):
        hash(mount)
    mount.read_only = True
    assert mount == CloudBucketMount("bucket", read_only=True)


def test_nothing_is_checked_at_construction():
    # All of these are refused only when passed to Sandbox.create.
    CloudBucketMount("bucket", key_prefix="no-slash")
    CloudBucketMount("bucket", requester_pays=True)
    CloudBucketMount("bucket", secret=object(), oidc_auth_role_arn="arn")


# -- volumes= validation ---------------------------------------------------------


def test_translation_to_bucket_mounts():
    mounts = _validate_volumes(
        {
            "/data": CloudBucketMount("a", read_only=True),
            "/gcs": CloudBucketMount(
                "b",
                bucket_endpoint_url="https://storage.googleapis.com",
                key_prefix="dir/",
                force_path_style=True,
            ),
        }
    )
    assert mounts == [
        BucketMount("a", "/data", read_only=True),
        BucketMount(
            "b",
            "/gcs",
            endpoint_url="https://storage.googleapis.com",
            key_prefix="dir/",
            read_only=False,  # Modal's default, passed explicitly.
            force_path_style=True,
        ),
    ]


def test_no_volumes():
    assert _validate_volumes(None) == []
    assert _validate_volumes({}) == []


@pytest.mark.parametrize(
    "key,expected",
    [
        ("/mnt/data/", "/mnt/data"),
        ("/mnt/./data", "/mnt/data"),
        ("//data", "/data"),
        ("/tmp/x", "/tmp/x"),
        ("/root/x", "/root/x"),
        (PurePosixPath("/p"), "/p"),
    ],
)
def test_paths_modal_accepts(key, expected):
    (mount,) = _validate_volumes({key: CloudBucketMount("b")})
    assert mount.mount_path == expected


def test_path_like_keys():
    class Path(os.PathLike):
        def __fspath__(self):
            return "/fs"

    (mount,) = _validate_volumes({Path(): CloudBucketMount("b")})
    assert mount.mount_path == "/fs"


@pytest.mark.parametrize(
    "key,message",
    [
        ("rel", "Volume rel must be a canonical, absolute path."),
        ("/a/../b", "Volume /a/../b must be a canonical, absolute path."),
        ("/", "Volume / cannot be mounted into root directory."),
        ("/root", "Volume /root cannot be mounted at '/root'."),
        ("/tmp", "Volume /tmp cannot be mounted at '/tmp'."),
    ],
)
def test_paths_modal_refuses_with_its_messages(key, message):
    with pytest.raises(InvalidError) as err:
        _validate_volumes({key: CloudBucketMount("b")})
    assert str(err.value) == message


def test_container_type_message():
    with pytest.raises(InvalidError) as err:
        _validate_volumes([CloudBucketMount("b")])
    assert str(err.value) == (
        "`volume_likes` should be a dict[str | PurePosixPath, Volume], got "
        "<class 'list'> instead"
    )


def test_value_type_message():
    with pytest.raises(InvalidError) as err:
        _validate_volumes({"/v": object()})
    assert str(err.value) == (
        "Object of type <class 'object'> mounted at '/v' is not usable as a volume."
    )


def test_every_path_is_checked_before_any_value():
    with pytest.raises(InvalidError, match="canonical"):
        _validate_volumes({"/ok": object(), "bad": CloudBucketMount("b")})


def test_values_are_checked_before_mount_options():
    with pytest.raises(InvalidError, match="not usable"):
        _validate_volumes({"/a": CloudBucketMount("b", key_prefix="x"), "/v": object()})


def test_modals_value_errors_in_modals_order():
    with pytest.raises(ValueError) as err:
        _validate_volumes(
            {"/a": CloudBucketMount("b", requester_pays=True, key_prefix="x")}
        )
    assert str(err.value) == "Credentials required in order to use Requester Pays."
    with pytest.raises(ValueError) as err:
        _validate_volumes(
            {"/a": CloudBucketMount("b", key_prefix="x", secret=object())}
        )
    assert str(err.value) == (
        "key_prefix will be prefixed to all object paths, so it must end in a '/'"
    )


def test_modals_checks_come_before_this_runtimes_refusals():
    """Every mount gets Modal's own checks first: code ported from Modal sees
    Modal's error, whatever else this runtime would refuse."""
    with pytest.raises(ValueError, match="key_prefix"):
        _validate_volumes(
            {"/etc": CloudBucketMount("a"), "/b": CloudBucketMount("b", key_prefix="x")}
        )
    with pytest.raises(ValueError, match="Requester Pays"):
        _validate_volumes(
            {
                "/a": CloudBucketMount("a", secret="s"),
                "/b": CloudBucketMount("b", requester_pays=True),
            }
        )


@pytest.mark.parametrize("option", ["secret", "oidc_auth_role_arn"])
def test_credentials_are_not_supported(option):
    with pytest.raises(
        NotSupportedError, match=f"'{option}' parameter of CloudBucketMount"
    ):
        _validate_volumes({"/a": CloudBucketMount("b", **{option: "x"})})


def test_unrecognized_endpoint_logs_modals_line(caplog):
    with caplog.at_level(logging.INFO):
        _validate_volumes(
            {"/a": CloudBucketMount("b", bucket_endpoint_url="http://minio:9000")}
        )
    assert (
        "CloudBucketMount received unrecognized bucket endpoint URL. Assuming AWS S3 "
        "configuration as fallback." in caplog.text
    )
    caplog.clear()
    with caplog.at_level(logging.INFO):
        _validate_volumes(
            {
                "/a": CloudBucketMount(
                    "b", bucket_endpoint_url="https://storage.googleapis.com"
                )
            }
        )
    assert "unrecognized" not in caplog.text


@pytest.mark.parametrize(
    "volumes",
    [
        {"/data": CloudBucketMount("a"), "/data/sub": CloudBucketMount("b")},
        {"/etc": CloudBucketMount("a")},
        {"/proc/x": CloudBucketMount("a")},
        {"/etc/hosts": CloudBucketMount("a")},
    ],
)
def test_paths_this_runtime_cannot_mount(volumes):
    """Modal accepts these; mounting them here is not supported."""
    with pytest.raises(NotSupportedError):
        _validate_volumes(volumes)


def test_one_object_at_several_paths():
    mount = CloudBucketMount("a")
    assert [m.mount_path for m in _validate_volumes({"/x": mount, "/y": mount})] == [
        "/x",
        "/y",
    ]


def test_the_same_path_twice_after_normalizing():
    with pytest.raises(InvalidError, match="mounted twice"):
        _validate_volumes(
            {"/data": CloudBucketMount("a"), "/data/": CloudBucketMount("b")}
        )


@pytest.mark.parametrize(
    "mount",
    [CloudBucketMount("-bad"), CloudBucketMount("b", bucket_endpoint_url="host-only")],
)
def test_what_mountpoint_cannot_use_is_invalid(mount):
    with pytest.raises(InvalidError):
        _validate_volumes({"/a": mount})


# -- Sandbox.create and the handle -------------------------------------------------


@pytest.mark.parametrize(
    "volumes,error",
    [
        ({"/v": object()}, InvalidError),
        ({"/a": CloudBucketMount("b", secret="s")}, NotSupportedError),
        ({"/a": CloudBucketMount("b", key_prefix="x")}, ValueError),
        (
            {"/a": CloudBucketMount("b"), "/a/b": CloudBucketMount("c")},
            NotSupportedError,
        ),
    ],
)
def test_create_refuses_before_any_actor_exists(volumes, error):
    with pytest.raises(error):
        modal.Sandbox.create(image="busybox:latest", volumes=volumes)


def test_create_refuses_copy_false_files_bound_for_a_bucket(tmp_path):
    local = tmp_path / "f.txt"
    local.write_text("x")
    image = modal.Image.from_registry("busybox:latest").add_local_file(
        str(local), "/data/f.txt", copy=False
    )
    with pytest.raises(NotSupportedError, match="inside the volume mounted at '/data'"):
        modal.Sandbox.create(image=image, volumes={"/data": CloudBucketMount("b")})


@pytest.mark.parametrize(
    "reply,expected",
    [
        # Resolved inside the sandbox: /lib64 links to /lib there.
        (
            {"returncode": 0, "stdout": b"/lib/x\0/ro\0"},
            (("/lib/x", False), ("/ro", True)),
        ),
        # Not resolvable: as configured.
        ({"returncode": 1, "stdout": b""}, (("/lib64/x", False), ("/ro", True))),
    ],
)
def test_bucket_mounts_are_used_where_they_really_are(reply, expected):
    class _ExecCollect:
        def remote(self, argv):
            self.argv = argv

            async def result():
                return reply

            return result()

    actor = type("_Actor", (), {"exec_collect": _ExecCollect()})()
    mounts = [
        BucketMount("a", "/lib64/x", read_only=False),
        BucketMount("b", "/ro", read_only=True),
    ]
    assert asyncio.run(_resolve_bucket_mounts(actor, mounts)) == expected
    assert actor.exec_collect.argv[-2:] == ["/lib64/x", "/ro"]


def test_reload_volumes_is_a_no_op():
    """Bucket mounts need no reload, and Modal returns None for a Sandbox
    without Volumes; the timeout check is Modal's."""
    sandbox = modal.Sandbox._from_impl(_Sandbox(None, "ray-sandbox-test", None))
    assert sandbox.reload_volumes() is None
    with pytest.raises(InvalidError, match="must be positive"):
        sandbox.reload_volumes(timeout=0)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
