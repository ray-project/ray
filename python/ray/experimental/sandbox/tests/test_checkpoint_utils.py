import json
import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

import pytest

from ray.experimental.sandbox.backend.checkpoint_utils import (
    CheckpointPath,
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

    def test_manifest_records_what_restore_needs(self):
        config = SandboxConfig(
            image="busybox:latest",
            network="public",
            readonly=False,
            disk="2Gi",
            cidr_allowlist=["203.0.113.0/24"],
        )
        manifest = create_manifest(
            sandbox_id="test-sb",
            config=config,
            cwd="/",
            workdir=None,
            leave_running=True,
            compression="flate-best-speed",
        )
        assert manifest["compression"] == "flate-best-speed"
        assert manifest["config"]["disk"] == "2Gi"
        assert manifest["config"]["cidr_allowlist"] == ["203.0.113.0/24"]
        # The recorded disk and allowlist come back on restore.
        restored = build_restored_config(json.loads(json.dumps(manifest)))
        assert restored.disk == "2Gi"
        assert restored.cidr_allowlist == ["203.0.113.0/24"]

    def test_build_restored_config_rejects_changes_to_saved_state(self):
        manifest = {
            "config": {
                "image": "busybox:latest",
                "network": "none",
                "readonly": True,
                "workdir": "/workspace",
                "rootless": False,
                "capabilities": None,
                "disk": "1Gi",
            }
        }
        for field, value in (
            ("capabilities", ["CAP_NET_ADMIN"]),
            ("disk", "2Gi"),
            ("image", "alpine:latest"),
            ("network", "public"),
            ("readonly", False),
            ("workdir", "/other"),
            ("rootless", True),
        ):
            with pytest.raises(SandboxError, match=f"Cannot restore with {field}="):
                build_restored_config(manifest, **{field: value})
        # Passing the recorded value, or None, is not a change.
        restored = build_restored_config(
            manifest, network="none", readonly=True, workdir="/workspace", image=None
        )
        assert restored.network == "none"

    def test_build_restored_config_workdir_is_free_on_a_writable_rootfs(self):
        # Without a readonly rootfs, the workdir is only the working
        # directory, not a mount.
        manifest = {
            "config": {
                "image": "busybox:latest",
                "readonly": False,
                "workdir": "/workspace",
                "rootless": False,
            }
        }
        assert build_restored_config(manifest, workdir="/other").workdir == "/other"

    def test_build_restored_config_requires_the_source_transform(self):
        manifest = {
            "oci_spec_transform": True,
            "config": {"image": "busybox:latest", "rootless": False},
        }
        with pytest.raises(SandboxError, match="_oci_spec_transform_fn"):
            build_restored_config(manifest)
        assert build_restored_config(manifest, _oci_spec_transform_fn=lambda s: s)

    def test_copy_fs_tree_keeps_owners_as_root(self):
        src = os.path.join(self.temp_dir, "owned_src")
        os.makedirs(os.path.join(src, "sub"))
        with open(os.path.join(src, "sub", "f.txt"), "w") as f:
            f.write("x")
        dst = os.path.join(self.temp_dir, "owned_dst")

        real_lstat = os.lstat

        def lstat(path):
            st = real_lstat(path)
            # Pretend a guest user (uid 1234) owns everything in src.
            if path.startswith(src):
                return os.stat_result((st.st_mode, 0, 0, 0, 1234, 1234, 0, 0, 0, 0))
            return st

        chowned = {}
        with (
            patch("os.geteuid", return_value=0),
            patch("os.lstat", side_effect=lstat),
            patch("os.lchown", side_effect=lambda p, u, g: chowned.update({p: (u, g)})),
        ):
            copy_fs_tree(src, dst)
        for rel in ("", "sub", os.path.join("sub", "f.txt")):
            assert chowned[os.path.normpath(os.path.join(dst, rel))] == (1234, 1234)

    def test_staged_checkpoint_atomic_swap_and_rollback(self):
        target_dir = os.path.join(self.temp_dir, "bundle")
        os.makedirs(target_dir, exist_ok=True)
        # An earlier checkpoint, which a new one may replace.
        with open(os.path.join(target_dir, "manifest.json"), "w") as f:
            f.write("{}")
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

    def test_checkpoint_path_str_subclass_and_pickle(self):
        import pickle

        raw_path = "/tmp/ray/sandbox/checkpoints/test-sb-1"
        cp = CheckpointPath(
            raw_path,
            node_id="node_abc_123",
            remote=True,
            cpu=2.5,
            memory="4Gi",
        )
        assert isinstance(cp, str)
        assert cp == raw_path
        assert cp.node_id == "node_abc_123"
        assert cp.remote is True
        assert cp.cpu == 2.5
        assert cp.memory == "4Gi"
        assert os.path.join(cp, "manifest.json") == f"{raw_path}/manifest.json"

        # Verify pickle roundtrip (simulating Ray Actor serialization)
        serialized = pickle.dumps(cp)
        restored = pickle.loads(serialized)
        assert isinstance(restored, CheckpointPath)
        assert restored == raw_path
        assert restored.node_id == "node_abc_123"
        assert restored.remote is True
        assert restored.cpu == 2.5
        assert restored.memory == "4Gi"

    def test_create_manifest_with_remote_and_node_id(self):
        config = SandboxConfig(image="busybox:latest", cpu=1.0, memory="512Mi")
        manifest_local = create_manifest(
            sandbox_id="sb-local",
            config=config,
            cwd="/",
            workdir="/workspace",
            leave_running=True,
            remote=False,
            node_id="node_1",
        )
        assert manifest_local["remote"] is False
        assert manifest_local["node_id"] == "node_1"

        manifest_remote = create_manifest(
            sandbox_id="sb-remote",
            config=config,
            cwd="/",
            workdir="/workspace",
            leave_running=True,
            remote=True,
            node_id="node_2",
        )
        assert manifest_remote["remote"] is True
        assert manifest_remote["node_id"] == "node_2"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
