import json
import os
import shutil
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import pytest

import ray.experimental.sandbox as sandbox_api
from ray.experimental.sandbox.backend.base import SandboxStatus
from ray.experimental.sandbox.backend.gvisor import GVisorSandboxBackend
from ray.experimental.sandbox.config import SandboxConfig
from ray.experimental.sandbox.exceptions import SandboxError
from ray.experimental.sandbox.runtime import SandboxRuntime


class TestCheckpointRestore(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.image_manager = MagicMock()
        self.image_manager.images_dir = os.path.join(self.temp_dir, "images")
        self.image_manager.pull_image.return_value = os.path.join(
            self.temp_dir, "images", "busybox_latest"
        )
        self.backend = GVisorSandboxBackend(
            image_manager=self.image_manager,
        )

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_mock_sandbox(self, rootless: bool = False, network: str = "none"):
        config = SandboxConfig(
            image="busybox:latest",
            rootless=rootless,
            network=network,
            readonly=True,
            workdir="/workspace",
        )
        sandbox_id = "ray-sandbox-test-123"
        root_dir = os.path.join(self.temp_dir, "instances", sandbox_id)
        os.makedirs(os.path.join(root_dir, "bundle"), exist_ok=True)
        os.makedirs(os.path.join(root_dir, "rootfs"), exist_ok=True)
        os.makedirs(os.path.join(root_dir, "workdir"), exist_ok=True)
        with open(os.path.join(root_dir, "config.json"), "w") as f:
            f.write("{}")

        self.backend._sandbox_metadata[sandbox_id] = {
            "config": config,
            "root_dir": root_dir,
            "workdir": os.path.join(root_dir, "workdir"),
            "cwd": "/workspace",
            "process": MagicMock(),
            "status": SandboxStatus.RUNNING,
            "paused": False,
        }
        return sandbox_id, root_dir

    def test_pause_and_resume(self):
        sandbox_id, _ = self._create_mock_sandbox()

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            self.backend.pause_sandbox(sandbox_id)
            assert self.backend.get_status(sandbox_id) == SandboxStatus.PAUSED
            # Verify runsc pause was called
            args, _ = mock_run.call_args
            assert "pause" in args[0]
            assert sandbox_id in args[0]

            self.backend.resume_sandbox(sandbox_id)
            assert self.backend.get_status(sandbox_id) == SandboxStatus.RUNNING
            # Verify runsc resume was called
            args, _ = mock_run.call_args
            assert "resume" in args[0]
            assert sandbox_id in args[0]

    def test_checkpoint_rootless_disallowed(self):
        sandbox_id, _ = self._create_mock_sandbox(rootless=True)
        with pytest.raises(
            SandboxError, match="does not support checkpoint/restore in rootless mode"
        ):
            self.backend.checkpoint_sandbox(sandbox_id)

    def test_checkpoint_bundle_creation(self):
        sandbox_id, root_dir = self._create_mock_sandbox(rootless=False)

        # Write dummy files to upper rootfs and workdir
        with open(os.path.join(root_dir, "rootfs", "file_in_overlay.txt"), "w") as f:
            f.write("test overlay content")
        with open(os.path.join(root_dir, "workdir", "file_in_workdir.txt"), "w") as f:
            f.write("test workdir content")

        dest_checkpoint_dir = os.path.join(self.temp_dir, "my_checkpoint")

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            result = self.backend.checkpoint_sandbox(
                sandbox_id,
                checkpoint_path=dest_checkpoint_dir,
                leave_running=True,
            )

            checkpoint_path = result["checkpoint_path"]
            assert checkpoint_path == dest_checkpoint_dir
            assert os.path.isdir(checkpoint_path)
            assert os.path.isfile(os.path.join(checkpoint_path, "manifest.json"))
            assert os.path.isfile(os.path.join(checkpoint_path, "config.json"))
            assert os.path.isdir(os.path.join(checkpoint_path, "state"))
            assert os.path.isdir(os.path.join(checkpoint_path, "fs"))

            # Check manifest contents
            with open(os.path.join(checkpoint_path, "manifest.json")) as f:
                manifest = json.load(f)
            assert manifest["version"] == "1.0"
            assert manifest["image"] == "busybox:latest"
            assert manifest["sandbox_id"] == sandbox_id

            # Check that upper layer and workdir were copied
            assert os.path.isfile(
                os.path.join(checkpoint_path, "fs", "rootfs", "file_in_overlay.txt")
            )
            assert os.path.isfile(
                os.path.join(checkpoint_path, "fs", "workdir", "file_in_workdir.txt")
            )

            # Check runsc checkpoint command flags
            checkpoint_calls = [
                call[0][0]
                for call in mock_run.call_args_list
                if len(call[0]) > 0
                and isinstance(call[0][0], list)
                and "checkpoint" in call[0][0]
            ]
            assert len(checkpoint_calls) == 1
            cmd = checkpoint_calls[0]
            assert "checkpoint" in cmd
            assert "--leave-running" in cmd
            assert "--compression=none" in cmd
            assert "--exclude-committed-zero-pages" in cmd
            assert any(arg.startswith("--image-path=") for arg in cmd)

    def test_restore_validation_errors(self):
        # Non-existent checkpoint path
        with pytest.raises(SandboxError, match="Checkpoint path does not exist"):
            self.backend.restore_sandbox("/nonexistent/checkpoint/path")

        # Missing manifest.json
        invalid_ckpt = os.path.join(self.temp_dir, "invalid_ckpt")
        os.makedirs(invalid_ckpt, exist_ok=True)
        with pytest.raises(SandboxError, match="Missing manifest.json"):
            self.backend.restore_sandbox(invalid_ckpt)

        # Missing state dir
        with open(os.path.join(invalid_ckpt, "manifest.json"), "w") as f:
            json.dump({"version": "1.0", "image": "busybox:latest"}, f)
        with pytest.raises(SandboxError, match="Missing state directory"):
            self.backend.restore_sandbox(invalid_ckpt)

    def test_build_restore_command(self):
        config_none = SandboxConfig(
            image="busybox:latest", network="none", rootless=False
        )
        cmd = self.backend._build_restore_command(
            config=config_none,
            root_dir="/path/to/bundle",
            state_dir="/path/to/state",
            sandbox_id="sb-test",
            background=True,
            direct=False,
        )
        assert cmd[0] in ("runsc", "sudo")
        assert "runsc" in cmd
        assert "restore" in cmd
        assert "--background" in cmd
        assert "--bundle" in cmd
        assert "/path/to/bundle" in cmd
        assert "--image-path" in cmd
        assert "/path/to/state" in cmd
        assert cmd[-1] == "sb-test"

        # Public network with slirp4netns
        config_public = SandboxConfig(
            image="busybox:latest", network="public", rootless=False
        )
        cmd_public = self.backend._build_restore_command(
            config=config_public,
            root_dir="/path/to/bundle",
            state_dir="/path/to/state",
            sandbox_id="sb-test-net",
            background=True,
            direct=True,
        )
        assert cmd_public[0] == "bash"
        assert cmd_public[1] == "-c"
        assert "unshare --user" in cmd_public[2]
        assert "slirp4netns" in cmd_public[2]
        assert "restore" in cmd_public[2]
        assert "--direct" in cmd_public[2]

    def test_restore_success(self):
        # Setup mock checkpoint directory
        ckpt_dir = os.path.join(self.temp_dir, "valid_checkpoint")
        state_dir = os.path.join(ckpt_dir, "state")
        fs_upper = os.path.join(ckpt_dir, "fs", "rootfs")
        fs_workdir = os.path.join(ckpt_dir, "fs", "workdir")
        os.makedirs(state_dir, exist_ok=True)
        os.makedirs(fs_upper, exist_ok=True)
        os.makedirs(fs_workdir, exist_ok=True)

        with open(os.path.join(fs_upper, "saved.txt"), "w") as f:
            f.write("restored upper data")

        manifest = {
            "version": "1.0",
            "image": "busybox:latest",
            "sandbox_id": "ray-sandbox-src",
            "config": {
                "image": "busybox:latest",
                "cpu": 2.0,
                "memory": "1Gi",
                "env": {"FOO": "BAR"},
                "workdir": "/workspace",
                "rootless": False,
                "network": "none",
                "readonly": True,
            },
        }
        with open(os.path.join(ckpt_dir, "manifest.json"), "w") as f:
            json.dump(manifest, f)

        # Mock image cache directory with rootfs.erofs
        images_dir = os.path.join(self.temp_dir, "images", "busybox_latest")
        os.makedirs(images_dir, exist_ok=True)
        with open(os.path.join(images_dir, "rootfs.erofs"), "w") as f:
            f.write("mock erofs")

        mock_proc = MagicMock()
        mock_proc.poll.return_value = 0
        mock_proc.returncode = 0

        with patch("subprocess.Popen", return_value=mock_proc), patch(
            "subprocess.run"
        ) as mock_run, patch("time.sleep"):
            # runsc state query returns running
            mock_run.return_value = MagicMock(
                returncode=0, stdout=json.dumps({"status": "running"})
            )
            restored_id = self.backend.restore_sandbox(ckpt_dir)

            assert restored_id.startswith("ray-sandbox-")
            assert restored_id in self.backend._sandbox_metadata
            meta = self.backend._sandbox_metadata[restored_id]
            assert meta["config"].image == "busybox:latest"
            assert meta["status"] == SandboxStatus.RUNNING

            # Check that restored rootfs received the copied file
            restored_root = meta["root_dir"]
            assert os.path.isfile(os.path.join(restored_root, "rootfs", "saved.txt"))
            with open(os.path.join(restored_root, "rootfs", "saved.txt")) as f:
                assert f.read() == "restored upper data"

    def test_restore_config_overrides(self):
        ckpt_dir = os.path.join(self.temp_dir, "override_checkpoint")
        state_dir = os.path.join(ckpt_dir, "state")
        fs_upper = os.path.join(ckpt_dir, "fs", "rootfs")
        os.makedirs(state_dir, exist_ok=True)
        os.makedirs(fs_upper, exist_ok=True)

        manifest = {
            "version": "1.0",
            "image": "busybox:latest",
            "sandbox_id": "ray-sandbox-orig",
            "config": {
                "image": "busybox:latest",
                "cpu": 1.0,
                "memory": "512Mi",
                "env": {"ORIG": "VAL"},
                "workdir": "/orig",
                "rootless": False,
                "network": "none",
                "readonly": True,
            },
        }
        with open(os.path.join(ckpt_dir, "manifest.json"), "w") as f:
            json.dump(manifest, f)

        mock_proc = MagicMock()
        mock_proc.poll.return_value = 0
        mock_proc.returncode = 0

        with patch("subprocess.Popen", return_value=mock_proc), patch(
            "subprocess.run"
        ) as mock_run, patch("time.sleep"):
            mock_run.return_value = MagicMock(
                returncode=0, stdout=json.dumps({"status": "running"})
            )
            restored_id = self.backend.restore_sandbox(
                ckpt_dir,
                cpu=4.0,
                memory="2Gi",
                env={"NEW": "VAL2"},
                workdir="/new_workdir",
                network="public",
                readonly=False,
            )
            meta = self.backend._sandbox_metadata[restored_id]
            assert meta["config"].cpu == 4.0
            assert meta["config"].memory == "2Gi"
            assert meta["config"].env == {"NEW": "VAL2"}
            assert meta["config"].workdir == "/new_workdir"
            assert meta["config"].network == "public"
            assert meta["config"].readonly is False

    def test_default_checkpoint_path_format(self):
        sandbox_id, _ = self._create_mock_sandbox(rootless=False)
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            result = self.backend.checkpoint_sandbox(sandbox_id)
            ckpt_path = result["checkpoint_path"]
            assert sandbox_id in ckpt_path
            assert "/checkpoints/" in ckpt_path

    def test_runtime_delegation(self):
        runtime = SandboxRuntime()
        runtime._backend = MagicMock()
        runtime._backend.checkpoint_sandbox.return_value = {
            "checkpoint_path": "/ckpt/dir"
        }
        runtime._backend.restore_sandbox.return_value = "restored-id"

        ckpt = runtime.checkpoint("id-123", checkpoint_path="/custom")
        assert ckpt == "/ckpt/dir"
        runtime._backend.checkpoint_sandbox.assert_called_once_with(
            sandbox_id="id-123",
            checkpoint_path="/custom",
            leave_running=True,
            timeout_seconds=30.0,
        )

        res = runtime.restore("/ckpt/dir", cpu=4.0)
        assert res == "restored-id"
        runtime._backend.restore_sandbox.assert_called_once()

        runtime.pause("id-123")
        runtime._backend.pause_sandbox.assert_called_once_with(
            "id-123", timeout_seconds=10.0
        )

        runtime.resume("id-123")
        runtime._backend.resume_sandbox.assert_called_once_with(
            "id-123", timeout_seconds=10.0
        )

    def test_api_exports(self):
        assert hasattr(sandbox_api, "restore")
        assert callable(sandbox_api.restore)
        assert "restore" in sandbox_api.__all__


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
