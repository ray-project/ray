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


def test_create_oci_spec_unions_capabilities(tmp_path):
    mgr = _StubImageManager(tmp_path)
    spec = mgr.create_oci_spec(
        image="fake:latest",
        base_spec=_sample_base_spec(),
        capabilities=["CAP_CHOWN", "CAP_SETUID"],
    )
    for cap_set in ("bounding", "effective", "inheritable", "permitted"):
        caps = spec["process"]["capabilities"][cap_set]
        assert "CAP_CHOWN" in caps
        assert "CAP_SETUID" in caps
    # Anything the runtime default grants survives the union.
    assert "CAP_KILL" in spec["process"]["capabilities"]["bounding"]
    # The ambient set is deliberately left alone.
    assert "ambient" not in spec["process"]["capabilities"]


def test_create_oci_spec_default_capabilities_untouched(tmp_path):
    mgr = _StubImageManager(tmp_path)
    spec = mgr.create_oci_spec(image="fake:latest", base_spec=_sample_base_spec())
    assert spec["process"]["capabilities"] == {"bounding": ["CAP_KILL"]}


def test_create_oci_spec_host_network_drops_empty_netns(tmp_path):
    mgr = _StubImageManager(tmp_path)
    spec = mgr.create_oci_spec(
        image="fake:latest", base_spec=_sample_base_spec(), network="host"
    )
    assert {"type": "network"} not in spec["linux"]["namespaces"]
    assert {"type": "pid"} in spec["linux"]["namespaces"]

    # Host networking gets the host's resolver, like Docker.
    if os.path.exists("/etc/resolv.conf"):
        resolv_mounts = [
            m for m in spec["mounts"] if m["destination"] == "/etc/resolv.conf"
        ]
        assert len(resolv_mounts) == 1
        assert resolv_mounts[0]["source"] == "/etc/resolv.conf"
        assert "ro" in resolv_mounts[0]["options"]


def test_create_oci_spec_host_network_keeps_pathed_netns(tmp_path):
    mgr = _StubImageManager(tmp_path)
    base = _sample_base_spec()
    base["linux"]["namespaces"] = [{"type": "network", "path": "/proc/1/ns/net"}]
    spec = mgr.create_oci_spec(image="fake:latest", base_spec=base, network="host")
    # A *pathed* network namespace is somebody's explicit choice; keep it.
    assert spec["linux"]["namespaces"] == [
        {"type": "network", "path": "/proc/1/ns/net"}
    ]


def test_create_oci_spec_non_host_network_untouched(tmp_path):
    mgr = _StubImageManager(tmp_path)
    for mode in ("none", "sandbox"):
        spec = mgr.create_oci_spec(
            image="fake:latest", base_spec=_sample_base_spec(), network=mode
        )
        assert {"type": "network"} in spec["linux"]["namespaces"]
        assert all(m["destination"] != "/etc/resolv.conf" for m in spec["mounts"])


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
