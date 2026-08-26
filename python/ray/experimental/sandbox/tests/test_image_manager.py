import io
import json
import os
import sys
import tarfile
from unittest.mock import MagicMock

import pytest

from ray.experimental.sandbox._internal.image_utils import DEFAULT_IMAGES_DIR
from ray.experimental.sandbox.backend.gvisor import GVisorSandboxBackend
from ray.experimental.sandbox.config import SandboxConfig
from ray.experimental.sandbox.image_manager import (
    BaseImageManager,
    ImageManager,
    get_default_oci_spec,
)
from ray.experimental.sandbox.runtime import SandboxRuntime


def test_image_manager_init(tmp_path):
    mgr_default = ImageManager()
    assert mgr_default.images_dir == DEFAULT_IMAGES_DIR

    custom_dir = str(tmp_path / "custom_images")
    mgr_custom = ImageManager(images_dir=custom_dir)
    assert mgr_custom.images_dir == custom_dir


def test_get_default_oci_spec():
    spec = get_default_oci_spec()
    assert isinstance(spec, dict)
    assert "ociVersion" in spec
    assert "process" in spec
    assert "root" in spec
    assert "mounts" in spec


def test_image_manager_pull_and_paths(tmp_path):
    images_dir = str(tmp_path / "images")
    mgr = ImageManager(images_dir=images_dir)

    # Create dummy local tar image
    local_tar = tmp_path / "test_app.tar"
    with tarfile.open(str(local_tar), "w") as tar:
        data = b"hello from image manager"
        ti = tarfile.TarInfo("test.txt")
        ti.size = len(data)
        tar.addfile(ti, io.BytesIO(data))

    assert not mgr.is_image_extracted(str(local_tar))

    extracted_dir = mgr.pull_image(str(local_tar))
    assert os.path.exists(extracted_dir)
    assert mgr.is_image_extracted(str(local_tar))

    rootfs = mgr.get_rootfs_path(str(local_tar))
    assert rootfs == os.path.join(extracted_dir, "rootfs")
    assert (
        open(os.path.join(rootfs, "test.txt"), "rb").read()
        == b"hello from image manager"
    )


def test_image_manager_config_parsing(tmp_path):
    images_dir = str(tmp_path / "images")
    mgr = ImageManager(images_dir=images_dir)

    # Create dummy local tar image
    local_tar = tmp_path / "cfg_app.tar"
    with tarfile.open(str(local_tar), "w") as tar:
        data = b"app data"
        ti = tarfile.TarInfo("app.txt")
        ti.size = len(data)
        tar.addfile(ti, io.BytesIO(data))

    extracted_dir = mgr.pull_image(str(local_tar))

    # Without config json
    assert mgr.get_image_config(str(local_tar)) == {}
    assert mgr.get_workdir(str(local_tar)) is None
    assert mgr.get_envs(str(local_tar)) == []

    # Write .image_config.json
    cfg_data = {
        "config": {
            "WorkingDir": "/app",
            "Env": ["PATH=/usr/bin", "FOO=bar", "BAZ=123"],
        }
    }
    with open(
        os.path.join(extracted_dir, ".image_config.json"), "w", encoding="utf-8"
    ) as f:
        json.dump(cfg_data, f)

    assert mgr.get_image_config(str(local_tar)) == cfg_data
    assert mgr.get_workdir(str(local_tar)) == "/app"
    assert mgr.get_envs(str(local_tar)) == ["PATH=/usr/bin", "FOO=bar", "BAZ=123"]


def test_image_manager_create_oci_spec(tmp_path):
    images_dir = str(tmp_path / "images")
    mgr = ImageManager(images_dir=images_dir)

    local_tar = tmp_path / "spec_test.tar"
    with tarfile.open(str(local_tar), "w") as tar:
        ti = tarfile.TarInfo("spec.txt")
        ti.size = 4
        tar.addfile(ti, io.BytesIO(b"spec"))

    extracted_dir = mgr.pull_image(str(local_tar))
    # Write image config with default envs
    with open(
        os.path.join(extracted_dir, ".image_config.json"), "w", encoding="utf-8"
    ) as f:
        json.dump(
            {
                "config": {
                    "Env": ["VAR1=default1", "VAR2=default2"],
                }
            },
            f,
        )

    host_workdir = str(tmp_path / "workspace")
    os.makedirs(host_workdir, exist_ok=True)

    spec = mgr.create_oci_spec(
        image=str(local_tar),
        container_cwd="/workspace",
        workdir_path=host_workdir,
        env_dict={"VAR2": "override2", "VAR3": "new3"},
        cpu=2.5,
        memory="1Gi",
        readonly=True,
        _oci_spec_transform_fn=lambda s: {**s, "customField": "customValue"},
    )

    assert spec["root"]["path"] == os.path.join(extracted_dir, "rootfs")
    assert spec["root"]["readonly"] is True
    assert spec["process"]["cwd"] == "/workspace"
    assert spec["process"]["args"] == ["sleep", "infinity"]

    # Verify envs override
    env_list = spec["process"]["env"]
    assert "VAR1=default1" in env_list
    assert "VAR2=override2" in env_list
    assert "VAR2=default2" not in env_list
    assert "VAR3=new3" in env_list

    # Verify mounts
    mount_dests = [m["destination"] for m in spec["mounts"]]
    assert "/workspace" in mount_dests

    # Verify cpu and memory limits
    assert spec["linux"]["resources"]["cpu"]["quota"] == 250000
    assert spec["linux"]["resources"]["cpu"]["period"] == 100000
    assert spec["linux"]["resources"]["memory"]["limit"] == 1024**3

    # Verify custom transform
    assert spec.get("customField") == "customValue"


def test_image_manager_prepare_oci_bundle(tmp_path):
    images_dir = str(tmp_path / "images")
    mgr = ImageManager(images_dir=images_dir)

    local_tar = tmp_path / "bundle_test.tar"
    with tarfile.open(str(local_tar), "w") as tar:
        ti = tarfile.TarInfo("bundle.txt")
        ti.size = 6
        tar.addfile(ti, io.BytesIO(b"bundle"))

    bundle_dir = str(tmp_path / "bundle")
    workdir_path = str(tmp_path / "workdir")
    os.makedirs(workdir_path, exist_ok=True)

    config_json_path = mgr.prepare_oci_bundle(
        root_dir=bundle_dir,
        workdir_path=workdir_path,
        container_cwd="/workdir",
        image=str(local_tar),
        env_dict={"TEST_ENV": "123"},
        cpu=1.0,
        memory="256Mi",
        readonly=False,
    )

    assert os.path.exists(config_json_path)
    assert os.path.exists(os.path.join(bundle_dir, "rootfs"))

    with open(config_json_path, "r", encoding="utf-8") as f:
        spec = json.load(f)

    assert spec["root"]["readonly"] is False
    assert "TEST_ENV=123" in spec["process"]["env"]
    assert spec["linux"]["resources"]["memory"]["limit"] == 256 * 1024 * 1024


def test_sandbox_runtime_image_manager_integration(tmp_path):
    rt = SandboxRuntime()
    assert isinstance(rt.image_manager, ImageManager)
    assert isinstance(rt.backend, GVisorSandboxBackend)

    mock_backend = MagicMock()
    mock_backend.create_sandbox.return_value = "ray-sandbox-12345"
    rt._backend = mock_backend

    local_tar = tmp_path / "rt_test.tar"
    with tarfile.open(str(local_tar), "w") as tar:
        ti = tarfile.TarInfo("test.txt")
        ti.size = 2
        tar.addfile(ti, io.BytesIO(b"ok"))

    # Test runtime.pull_image
    extracted_path = rt.pull_image(str(local_tar))
    assert os.path.exists(extracted_path)
    assert rt.image_manager.is_image_extracted(str(local_tar))

    # Test runtime.create triggers image manager and backend
    sid = rt.create(
        image=str(local_tar),
        workdir="/workspace",
        cpu=1.0,
        memory="512Mi",
    )
    assert sid == "ray-sandbox-12345"
    assert mock_backend.create_sandbox.called
    created_cfg: SandboxConfig = mock_backend.create_sandbox.call_args[0][0]
    assert created_cfg.image == str(local_tar)
    assert created_cfg.cpu == 1.0


def test_base_image_manager_abstract():
    with pytest.raises(TypeError):
        BaseImageManager()  # Cannot instantiate abstract class


def test_custom_image_manager_subclass(tmp_path):
    class CustomImageManager(BaseImageManager):
        def __init__(self, rootfs_dir: str):
            self.rootfs_dir = rootfs_dir
            self.pull_calls = []

        def pull_image(self, image: str, timeout_seconds: float = 120.0) -> str:
            self.pull_calls.append((image, timeout_seconds))
            return self.rootfs_dir

        def get_image_dir(self, image: str) -> str:
            return self.rootfs_dir

        def get_rootfs_path(self, image: str) -> str:
            return os.path.join(self.rootfs_dir, "rootfs")

        def get_image_config(self, image: str):
            return {"config": {"WorkingDir": "/custom_workdir"}}

        def get_workdir(self, image: str):
            return "/custom_workdir"

        def get_envs(self, image: str):
            return ["CUSTOM_ENV=1"]

        def create_oci_spec(self, image: str, **kwargs):
            return {"ociVersion": "1.0.2", "custom": True}

        def prepare_oci_bundle(self, root_dir: str, **kwargs) -> str:
            config_path = os.path.join(root_dir, "config.json")
            with open(config_path, "w", encoding="utf-8") as f:
                json.dump({"custom": True}, f)
            return config_path

    custom_root = str(tmp_path / "custom_root")
    os.makedirs(custom_root, exist_ok=True)
    custom_mgr = CustomImageManager(rootfs_dir=custom_root)

    backend = GVisorSandboxBackend(image_manager=custom_mgr)
    assert backend.image_manager is custom_mgr
    assert custom_mgr.pull_image("another-image") == custom_root
    assert ("another-image", 120.0) in custom_mgr.pull_calls


class _StubImageManager(ImageManager):
    """ImageManager that skips pulling so spec construction is testable offline."""

    def __init__(self, tmp_path):
        super().__init__(images_dir=str(tmp_path))
        self._fake_image_dir = str(tmp_path)

    def pull_image(self, image, timeout_seconds=120.0):
        return self._fake_image_dir

    def get_image_config(self, image):
        return {}


def _sample_base_spec():
    return {
        "process": {"capabilities": {"bounding": ["CAP_KILL"]}},
        "root": {},
        "mounts": [],
        "linux": {
            "namespaces": [
                {"type": "pid"},
                {"type": "network"},
                {"type": "mount"},
            ]
        },
    }


def test_create_oci_spec_sets_capabilities_exactly(tmp_path):
    mgr = _StubImageManager(tmp_path)
    spec = mgr.create_oci_spec(
        image="fake:latest",
        base_spec=_sample_base_spec(),
        capabilities=["CAP_CHOWN", "CAP_SETUID"],
    )
    for cap_set in ("bounding", "effective", "permitted"):
        assert spec["process"]["capabilities"][cap_set] == [
            "CAP_CHOWN",
            "CAP_SETUID",
        ]
    # Inheritable and ambient are left alone (modern Docker behavior).
    assert "inheritable" not in spec["process"]["capabilities"]
    assert "ambient" not in spec["process"]["capabilities"]


def test_create_oci_spec_empty_capabilities_remove_all(tmp_path):
    mgr = _StubImageManager(tmp_path)
    spec = mgr.create_oci_spec(
        image="fake:latest", base_spec=_sample_base_spec(), capabilities=[]
    )
    for cap_set in ("bounding", "effective", "permitted"):
        assert spec["process"]["capabilities"][cap_set] == []


def test_create_oci_spec_default_capabilities_untouched(tmp_path):
    mgr = _StubImageManager(tmp_path)
    spec = mgr.create_oci_spec(image="fake:latest", base_spec=_sample_base_spec())
    assert spec["process"]["capabilities"] == {"bounding": ["CAP_KILL"]}


def test_create_oci_spec_host_side_networking_drops_empty_netns(tmp_path):
    mgr = _StubImageManager(tmp_path)
    for mode in ("host", "public"):
        spec = mgr.create_oci_spec(
            image="fake:latest", base_spec=_sample_base_spec(), network=mode
        )
        assert {"type": "network"} not in spec["linux"]["namespaces"]
        assert {"type": "pid"} in spec["linux"]["namespaces"]


def test_create_oci_spec_mounts_resolv_conf_source(tmp_path):
    mgr = _StubImageManager(tmp_path)
    source = tmp_path / "resolv.conf"
    source.write_text("nameserver 8.8.8.8\n")
    spec = mgr.create_oci_spec(
        image="fake:latest",
        base_spec=_sample_base_spec(),
        network="public",
        resolv_conf_source=str(source),
    )
    (mount,) = [m for m in spec["mounts"] if m["destination"] == "/etc/resolv.conf"]
    assert mount["source"] == str(source)
    assert "ro" in mount["options"]

    # No source, no mount.
    spec = mgr.create_oci_spec(
        image="fake:latest", base_spec=_sample_base_spec(), network="public"
    )
    assert not any(m["destination"] == "/etc/resolv.conf" for m in spec["mounts"])


def test_create_oci_spec_host_network_keeps_pathed_netns(tmp_path):
    mgr = _StubImageManager(tmp_path)
    base = _sample_base_spec()
    base["linux"]["namespaces"] = [{"type": "network", "path": "/proc/1/ns/net"}]
    spec = mgr.create_oci_spec(image="fake:latest", base_spec=base, network="host")
    # A *pathed* network namespace is somebody's explicit choice; keep it.
    assert spec["linux"]["namespaces"] == [
        {"type": "network", "path": "/proc/1/ns/net"}
    ]


def test_create_oci_spec_tolerates_null_capability_set(tmp_path):
    """A base_spec capability *set* (not just the dict) may be null."""
    mgr = _StubImageManager(tmp_path)
    base = _sample_base_spec()
    base["process"]["capabilities"] = {"effective": None, "bounding": ["CAP_KILL"]}
    spec = mgr.create_oci_spec(
        image="fake:latest", base_spec=base, capabilities=["CAP_CHOWN"]
    )
    assert spec["process"]["capabilities"]["effective"] == ["CAP_CHOWN"]
    assert spec["process"]["capabilities"]["bounding"] == ["CAP_CHOWN"]


def test_create_oci_spec_tolerates_non_dict_namespace_entries(tmp_path):
    """Malformed namespace entries are kept for runsc to reject, not dropped."""
    mgr = _StubImageManager(tmp_path)
    base = _sample_base_spec()
    base["linux"]["namespaces"] = [{"type": "network"}, "pid", None]
    spec = mgr.create_oci_spec(image="fake:latest", base_spec=base, network="host")
    assert spec["linux"]["namespaces"] == ["pid", None]


def test_create_oci_spec_workdir_path_drives_the_scratch_mount(tmp_path):
    """The scratch bind exists iff a workdir_path is given; the process cwd
    is independent of it."""
    mgr = _StubImageManager(tmp_path)
    workdir_path = str(tmp_path / "scratch")
    os.makedirs(workdir_path, exist_ok=True)

    mounted = mgr.create_oci_spec(
        image="fake:latest",
        base_spec=_sample_base_spec(),
        container_cwd="/app",
        workdir_path=workdir_path,
    )
    assert any(m["destination"] == "/app" for m in mounted["mounts"])

    unmounted = mgr.create_oci_spec(
        image="fake:latest",
        base_spec=_sample_base_spec(),
        container_cwd="/app",
        workdir_path=None,
    )
    assert not any(m["destination"] == "/app" for m in unmounted["mounts"])
    assert unmounted["process"]["cwd"] == "/app"


def test_create_oci_spec_tolerates_null_sections_in_base_spec(tmp_path):
    """A caller-supplied base_spec may carry null capabilities/linux keys."""
    mgr = _StubImageManager(tmp_path)
    base = _sample_base_spec()
    base["process"]["capabilities"] = None
    base["linux"] = None
    spec = mgr.create_oci_spec(
        image="fake:latest",
        base_spec=base,
        capabilities=["CAP_CHOWN"],
        network="host",
    )
    assert "CAP_CHOWN" in spec["process"]["capabilities"]["bounding"]
    assert isinstance(spec["linux"], dict)


def test_create_oci_spec_non_host_network_untouched(tmp_path):
    mgr = _StubImageManager(tmp_path)
    for mode in ("none", "sandbox"):
        spec = mgr.create_oci_spec(
            image="fake:latest", base_spec=_sample_base_spec(), network=mode
        )
        assert {"type": "network"} in spec["linux"]["namespaces"]
        assert all(m["destination"] != "/etc/resolv.conf" for m in spec["mounts"])


class _SpecCapturingManager(_StubImageManager):
    """Captures create_oci_spec kwargs so prepare_oci_bundle's resolv policy
    is testable without runsc."""

    def __init__(self, tmp_path):
        super().__init__(tmp_path)
        self.spec_kwargs = None

    def create_oci_spec(self, image, **kwargs):
        self.spec_kwargs = kwargs
        return {}


def _prepare(mgr, root_dir, **kwargs):
    mgr.prepare_oci_bundle(
        root_dir=str(root_dir),
        workdir_path=str(root_dir),
        container_cwd="/",
        image="fake:latest",
        **kwargs,
    )
    return mgr.spec_kwargs["resolv_conf_source"]


def test_prepare_oci_bundle_public_generates_default_resolv(tmp_path):
    mgr = _SpecCapturingManager(tmp_path)
    source = _prepare(mgr, tmp_path, network="public")
    assert source == os.path.join(str(tmp_path), "resolv.conf")
    content = open(source, encoding="utf-8").read()
    assert content == "nameserver 8.8.8.8\nnameserver 1.1.1.1\n"


def test_prepare_oci_bundle_dns_overrides_for_public_and_host(tmp_path):
    for network in ("public", "host"):
        mgr = _SpecCapturingManager(tmp_path)
        source = _prepare(mgr, tmp_path, network=network, dns=["10.0.0.2"])
        content = open(source, encoding="utf-8").read()
        assert content == "nameserver 10.0.0.2\n"


def test_prepare_oci_bundle_host_uses_host_resolv(tmp_path):
    mgr = _SpecCapturingManager(tmp_path)
    source = _prepare(mgr, tmp_path, network="host")
    if os.path.exists("/etc/resolv.conf"):
        assert source == "/etc/resolv.conf"
    else:
        assert source is None


def test_prepare_oci_bundle_no_resolv_without_host_side_networking(tmp_path):
    for network in ("none", "sandbox"):
        mgr = _SpecCapturingManager(tmp_path)
        assert _prepare(mgr, tmp_path, network=network) is None


def test_registry_mirror_rewrites_docker_hub_only(monkeypatch):
    """RAY_SANDBOX_REGISTRY_MIRROR reroutes Docker Hub pulls (with an
    optional repository prefix, as ECR pull-through caches require) and
    leaves every other registry untouched."""
    from ray.experimental.sandbox._internal.image_utils import (
        apply_registry_mirror,
    )

    monkeypatch.delenv("RAY_SANDBOX_REGISTRY_MIRROR", raising=False)
    assert apply_registry_mirror("registry-1.docker.io", "library/python") == (
        "registry-1.docker.io",
        "library/python",
    )

    monkeypatch.setenv("RAY_SANDBOX_REGISTRY_MIRROR", "mirror.local:5000")
    assert apply_registry_mirror("registry-1.docker.io", "library/python") == (
        "mirror.local:5000",
        "library/python",
    )

    monkeypatch.setenv(
        "RAY_SANDBOX_REGISTRY_MIRROR",
        "123.dkr.ecr.us-east-2.amazonaws.com/dockerhub/",
    )
    assert apply_registry_mirror("registry-1.docker.io", "library/python") == (
        "123.dkr.ecr.us-east-2.amazonaws.com",
        "dockerhub/library/python",
    )
    # Non-Docker-Hub registries are never rewritten.
    assert apply_registry_mirror("ghcr.io", "org/repo") == ("ghcr.io", "org/repo")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
