import io
import json
import os
import sys
import tarfile
from unittest.mock import MagicMock

import pytest

from ray.experimental.sandbox._internal.image_utils import DEFAULT_IMAGES_DIR
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
        work_dir_path=host_workdir,
        env_dict={"VAR2": "override2", "VAR3": "new3"},
        cpu=2.5,
        memory="1Gi",
        readonly=True,
        _oci_spec_transforms=lambda s: {**s, "customField": "customValue"},
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
        work_dir_path=workdir_path,
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
    images_dir = str(tmp_path / "images")
    custom_mgr = ImageManager(images_dir=images_dir)

    mock_backend = MagicMock()
    mock_backend.create_sandbox.return_value = "ray-sandbox-12345"

    rt = SandboxRuntime(backend=mock_backend, image_manager=custom_mgr)
    assert rt.image_manager is custom_mgr
    assert rt.backend is mock_backend

    local_tar = tmp_path / "rt_test.tar"
    with tarfile.open(str(local_tar), "w") as tar:
        ti = tarfile.TarInfo("test.txt")
        ti.size = 2
        tar.addfile(ti, io.BytesIO(b"ok"))

    # Test runtime.pull_image
    extracted_path = rt.pull_image(str(local_tar))
    assert os.path.exists(extracted_path)
    assert custom_mgr.is_image_extracted(str(local_tar))

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

    mock_backend = MagicMock()
    mock_backend.create_sandbox.return_value = "custom-sb-id"

    rt = SandboxRuntime(backend=mock_backend, image_manager=custom_mgr)
    assert rt.image_manager is custom_mgr

    sid = rt.create(image="my-custom-image:v1")
    assert sid == "custom-sb-id"
    assert ("my-custom-image:v1", 30.0) in custom_mgr.pull_calls
    assert rt.pull_image("another-image") == custom_root


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
