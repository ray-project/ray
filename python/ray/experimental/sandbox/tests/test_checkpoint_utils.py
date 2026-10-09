import json
import os
import shutil
import tempfile
import unittest

import pytest

from ray.experimental.sandbox.backend.checkpoint_utils import (
    StagedCheckpoint,
    build_restored_config,
    copy_fs_tree,
    create_manifest,
    load_checkpoint_manifest,
)
from ray.experimental.sandbox.config import SandboxConfig
from ray.experimental.sandbox.exceptions import SandboxError


class TestCheckpointUtils(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_create_and_load_manifest(self):
        config = SandboxConfig(
            image="busybox:latest",
            cpu=2.0,
            memory="2Gi",
            network="none",
            workdir="/workspace",
        )
        manifest = create_manifest(
            sandbox_id="test-sb-123",
            config=config,
            cwd="/workspace",
            workdir="/workspace",
            leave_running=True,
        )

        ckpt_dir = os.path.join(self.temp_dir, "checkpoint")
        os.makedirs(os.path.join(ckpt_dir, "state"), exist_ok=True)
        manifest_path = os.path.join(ckpt_dir, "manifest.json")
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f)

        loaded_manifest, state_dir = load_checkpoint_manifest(ckpt_dir)
        assert loaded_manifest["sandbox_id"] == "test-sb-123"
        assert loaded_manifest["config"]["cpu"] == 2.0
        assert state_dir == os.path.join(ckpt_dir, "state")

    def test_load_manifest_validation_errors(self):
        empty_dir = os.path.join(self.temp_dir, "empty")
        os.makedirs(empty_dir, exist_ok=True)

        # Missing manifest.json
        with pytest.raises(SandboxError, match="Missing manifest.json"):
            load_checkpoint_manifest(empty_dir)

        # Missing state directory
        manifest_path = os.path.join(empty_dir, "manifest.json")
        with open(manifest_path, "w", encoding="utf-8") as f:
            f.write("{}")

        with pytest.raises(SandboxError, match="Missing state directory"):
            load_checkpoint_manifest(empty_dir)

    def test_build_restored_config_overrides(self):
        manifest = {
            "config": {
                "image": "busybox:latest",
                "cpu": 1.0,
                "memory": "1Gi",
                "network": "public",
                "dns": ["8.8.8.8"],
            }
        }
        # Override CPU and memory; network and dns are preserved from manifest
        restored = build_restored_config(
            manifest,
            cpu=4.0,
            memory="8Gi",
        )
        assert restored.cpu == 4.0
        assert restored.memory == "8Gi"
        assert restored.network == "public"
        assert restored.dns == ["8.8.8.8"]

    def test_staged_checkpoint_atomic_swap_and_rollback(self):
        target_dir = os.path.join(self.temp_dir, "bundle")
        os.makedirs(target_dir, exist_ok=True)
        with open(os.path.join(target_dir, "old_file.txt"), "w") as f:
            f.write("original")

        # 1. Successful staging and swap
        with StagedCheckpoint(target_dir) as stage:
            with open(os.path.join(stage.state_dir, "state.bin"), "w") as f:
                f.write("state_data")
            stage.write_manifest({"version": "1.0"})

        assert os.path.exists(os.path.join(target_dir, "state", "state.bin"))
        assert os.path.exists(os.path.join(target_dir, "manifest.json"))
        # Old file was cleanly replaced
        assert not os.path.exists(os.path.join(target_dir, "old_file.txt"))

        # 2. Failure inside StagedCheckpoint preserves existing bundle
        with pytest.raises(RuntimeError, match="simulated failure"):
            with StagedCheckpoint(target_dir) as stage:
                with open(os.path.join(stage.state_dir, "corrupt.bin"), "w") as f:
                    f.write("corrupt")
                raise RuntimeError("simulated failure")

        # Previous valid checkpoint must remain completely intact
        assert os.path.exists(os.path.join(target_dir, "state", "state.bin"))
        assert not os.path.exists(os.path.join(target_dir, "state", "corrupt.bin"))

    def test_copy_fs_tree(self):
        src = os.path.join(self.temp_dir, "src")
        dst = os.path.join(self.temp_dir, "dst")
        os.makedirs(os.path.join(src, "subdir"), exist_ok=True)
        with open(os.path.join(src, "file.txt"), "w") as f:
            f.write("hello")
        with open(os.path.join(src, "ignore.me"), "w") as f:
            f.write("skip")

        copy_fs_tree(src, dst, ignore_patterns=["*.me"])
        assert os.path.exists(os.path.join(dst, "file.txt"))
        assert not os.path.exists(os.path.join(dst, "ignore.me"))

    def test_harden_permissions_preserves_guest_file_modes(self):
        target_dir = os.path.join(self.temp_dir, "perm_bundle")
        workdir = os.path.join(self.temp_dir, "workdir")
        os.makedirs(workdir, exist_ok=True)

        guest_file = os.path.join(workdir, "readable.txt")
        with open(guest_file, "w") as f:
            f.write("public content")
        os.chmod(guest_file, 0o644)

        with StagedCheckpoint(target_dir) as stage:
            stage.copy_bundle_filesystems(
                root_dir=self.temp_dir,
                workdir=workdir,
                sandbox_id="test-sb-perm",
            )
            stage.harden_permissions(rootless=True)

        staged_guest_file = os.path.join(target_dir, "fs", "workdir", "readable.txt")
        assert os.path.exists(staged_guest_file)
        # Verify that guest file mode 0644 was preserved inside fs/workdir
        mode = os.stat(staged_guest_file).st_mode & 0o777
        assert mode == 0o644

        # Verify outer target_dir is strictly restricted to owner (0700)
        target_mode = os.stat(target_dir).st_mode & 0o777
        assert target_mode == 0o700


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
