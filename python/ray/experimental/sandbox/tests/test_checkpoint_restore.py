import contextlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import pytest

import ray.experimental.sandbox as sandbox_api
from ray.experimental.sandbox.backend.base import SandboxStatus
from ray.experimental.sandbox.backend.checkpoint_utils import copy_fs_tree
from ray.experimental.sandbox.backend.gvisor import (
    GVisorSandboxBackend,
    _RestoreImage,
)
from ray.experimental.sandbox.config import SandboxConfig
from ray.experimental.sandbox.exceptions import (
    SandboxCreationError,
    SandboxError,
    SandboxTimeoutError,
)
from ray.experimental.sandbox.runtime import SandboxRuntime
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


class TestCheckpointRestore(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.image_manager = MagicMock()
        self.image_manager.images_dir = os.path.join(self.temp_dir, "images")
        self.image_manager.pull_image.return_value = os.path.join(
            self.temp_dir, "images", "busybox_latest"
        )
        self.image_manager.prepare_oci_bundle.side_effect = self._write_bundle
        self.backend = GVisorSandboxBackend(
            image_manager=self.image_manager,
        )
        # No reclaim passes over the node's real bundles, and no host cgroups.
        self.backend._maybe_reclaim_unowned_bundles = lambda: None
        cgroups = patch.object(
            GVisorSandboxBackend, "_cgroup_manager_for", return_value=None
        )
        cgroups.start()
        self.addCleanup(cgroups.stop)

    def tearDown(self):
        for sandbox_id in list(self.backend._sandbox_metadata):
            owner_file = self.backend._sandbox_metadata[sandbox_id].get("owner_file")
            if owner_file is not None:
                owner_file.close()
                shutil.rmtree(
                    self.backend._sandbox_metadata[sandbox_id]["root_dir"],
                    ignore_errors=True,
                )
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @staticmethod
    def _write_bundle(root_dir, **kwargs):
        """Write a bundle's config.json, as prepare_oci_bundle does, with a
        spec that boots straight from the image."""
        path = os.path.join(root_dir, "config.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"root": {"path": "rootfs", "readonly": True}}, f)
        return path

    @staticmethod
    def _running_proc():
        proc = MagicMock()
        proc.poll.return_value = None
        proc.pid = 12345
        return proc

    def _fake_runsc(self, state: str = "running", checkpoint_returncode: int = 0):
        """Patch runsc and its launch processes for one test.

        Every runsc command succeeds except ``checkpoint`` when
        ``checkpoint_returncode`` is set, ``runsc state`` reports ``state``,
        and launch processes keep running.

        Args:
            state: The status that ``runsc state`` reports.
            checkpoint_returncode: The exit code of ``runsc checkpoint``.

        Returns:
            The ExitStack holding the patches, and the subprocess.run and
            subprocess.Popen mocks.
        """

        def fake_run(args, *_, **__):
            if isinstance(args, list) and "state" in args:
                return MagicMock(
                    returncode=0, stdout=json.dumps({"status": state}), stderr=""
                )
            if isinstance(args, list) and "checkpoint" in args:
                return MagicMock(
                    returncode=checkpoint_returncode,
                    stdout="",
                    stderr="runsc crashed" if checkpoint_returncode else "",
                )
            return MagicMock(returncode=0, stdout="", stderr="")

        stack = contextlib.ExitStack()
        mock_run = stack.enter_context(patch("subprocess.run", side_effect=fake_run))
        mock_popen = stack.enter_context(
            patch("subprocess.Popen", side_effect=lambda *a, **k: self._running_proc())
        )
        stack.enter_context(patch("time.sleep"))
        stack.enter_context(patch("shutil.which", return_value="/usr/bin/runsc"))
        return stack, mock_run, mock_popen

    @staticmethod
    def _runsc_calls(mock_run, subcommand):
        return [
            call[0][0]
            for call in mock_run.call_args_list
            if call[0] and isinstance(call[0][0], list) and subcommand in call[0][0]
        ]

    @staticmethod
    def _launch_commands(mock_popen):
        return [" ".join(call[0][0]) for call in mock_popen.call_args_list]

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
            "proc": self._running_proc(),
            "status": SandboxStatus.RUNNING,
            "paused": False,
        }
        return sandbox_id, root_dir

    def test_checkpoint_rootless_disallowed(self):
        sandbox_id, _ = self._create_mock_sandbox(rootless=True)
        with pytest.raises(
            SandboxError, match="does not support checkpoint/restore in rootless mode"
        ):
            self.backend.checkpoint_sandbox(sandbox_id)

    def test_checkpoint_bundle_creation(self):
        sandbox_id, root_dir = self._create_mock_sandbox(rootless=False)
        old_proc = self.backend._sandbox_metadata[sandbox_id]["proc"]

        # Write dummy files to upper rootfs and workdir
        with open(os.path.join(root_dir, "rootfs", "file_in_overlay.txt"), "w") as f:
            f.write("test overlay content")
        with open(os.path.join(root_dir, "workdir", "file_in_workdir.txt"), "w") as f:
            f.write("test workdir content")

        dest_checkpoint_dir = os.path.join(self.temp_dir, "my_checkpoint")

        stack, mock_run, mock_popen = self._fake_runsc()
        with stack:
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
        assert manifest["compression"] == "none"

        # Check that upper layer and workdir were copied
        assert os.path.isfile(
            os.path.join(checkpoint_path, "fs", "rootfs", "file_in_overlay.txt")
        )
        assert os.path.isfile(
            os.path.join(checkpoint_path, "fs", "workdir", "file_in_workdir.txt")
        )

        # runsc checkpoint stops the sandbox: --leave-running would restore it
        # under a container that the exiting launch process deletes.
        checkpoint_calls = self._runsc_calls(mock_run, "checkpoint")
        assert len(checkpoint_calls) == 1
        cmd = checkpoint_calls[0]
        assert "--leave-running" not in cmd
        assert "--compression=none" in cmd
        assert "--exclude-committed-zero-pages" in cmd
        assert any(arg.startswith("--image-path=") for arg in cmd)
        assert not self._runsc_calls(mock_run, "pause")
        assert not self._runsc_calls(mock_run, "resume")

        # leave_running=True restores the sandbox in place from the new image,
        # under a new launch process, keeping its ID and bundle.
        old_proc.wait.assert_called()
        launches = self._launch_commands(mock_popen)
        assert len(launches) == 1
        assert " restore --bundle " + root_dir in launches[0]
        assert os.path.join(checkpoint_path, "state") in launches[0]
        assert "--background" in launches[0]
        assert launches[0].endswith(sandbox_id)
        meta = self.backend._sandbox_metadata[sandbox_id]
        assert meta["proc"] is not old_proc
        assert meta["status"] == SandboxStatus.RUNNING
        assert self.backend.get_status(sandbox_id) == SandboxStatus.RUNNING

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
        cmd = self.backend._build_run_command(
            config_none,
            "/path/to/bundle",
            "sb-test",
            _RestoreImage("/path/to/state", background=True, direct=False),
        )
        assert cmd[:2] == ["sh", "-c"]
        assert "restore" in cmd
        assert "run" not in cmd
        assert "--background" in cmd
        assert "--direct" not in cmd
        assert "--bundle" in cmd
        assert "/path/to/bundle" in cmd
        assert "--image-path" in cmd
        assert "/path/to/state" in cmd
        assert cmd[-1] == "sb-test"

        # Public network: the same namespace, egress and interrupt wrapping as
        # a created sandbox.
        config_public = SandboxConfig(
            image="busybox:latest", network="public", rootless=False
        )
        restore = _RestoreImage("/path/to/state", background=False, direct=True)
        cmd_public = self.backend._build_run_command(
            config_public, "/path/to/bundle", "sb-test-net", restore
        )
        run_public = self.backend._build_run_command(
            config_public, "/path/to/bundle", "sb-test-net"
        )
        assert cmd_public[:2] == ["bash", "-c"]
        script = cmd_public[2]
        assert "unshare --user" in script
        assert "slirp4netns" in script
        assert "restore" in script
        assert "--direct" in script
        assert "--background" not in script
        assert "--gofer-network-namespace=new" in script
        assert "nft -f" in script
        assert script.startswith("trap '' INT QUIT; ")
        assert (
            script.replace(
                "restore --bundle /path/to/bundle --image-path /path/to/state --direct",
                "run --bundle /path/to/bundle",
            )
            == run_public[2]
        )

    def test_checkpoint_already_paused_sandbox_remains_paused(self):
        sandbox_id, root_dir = self._create_mock_sandbox(rootless=False)
        self.backend._sandbox_metadata[sandbox_id]["status"] = SandboxStatus.PAUSED
        dest_checkpoint_dir = os.path.join(self.temp_dir, "paused_checkpoint")

        stack, mock_run, mock_popen = self._fake_runsc()
        with stack:
            result = self.backend.checkpoint_sandbox(
                sandbox_id,
                checkpoint_path=dest_checkpoint_dir,
                leave_running=True,
            )

        assert os.path.isdir(result["checkpoint_path"])
        # Restored in place, then paused again, as it was before.
        assert len(self._launch_commands(mock_popen)) == 1
        assert len(self._runsc_calls(mock_run, "pause")) == 1
        assert not self._runsc_calls(mock_run, "resume")
        assert (
            self.backend._sandbox_metadata[sandbox_id]["status"] == SandboxStatus.PAUSED
        )

    def test_checkpoint_restores_an_overlayfs_sandbox_in_its_overlay(self):
        """leave_running=True restores an overlayfs sandbox inside its kernel
        overlay."""
        sandbox_id, _ = self._create_mock_sandbox()
        rootfs_overlay = MagicMock()
        self.backend._sandbox_metadata[sandbox_id]["rootfs_overlay"] = rootfs_overlay

        stack, _, _ = self._fake_runsc()
        with (
            stack,
            patch.object(
                self.backend, "_build_run_command", return_value=["true"]
            ) as build,
        ):
            self.backend.checkpoint_sandbox(
                sandbox_id,
                checkpoint_path=os.path.join(self.temp_dir, "overlay_checkpoint"),
                leave_running=True,
            )

        assert build.call_args.kwargs["restore"] is not None
        assert build.call_args.kwargs["rootfs_overlay"] is rootfs_overlay

    def test_restore_boots_an_overlayfs_sandbox_in_its_overlay(self):
        """A restored sandbox whose spec needs a kernel overlay is restored
        inside one."""
        ckpt_dir = os.path.join(self.temp_dir, "overlay_checkpoint")
        os.makedirs(os.path.join(ckpt_dir, "state"))
        manifest = {
            "config": {
                "image": "busybox:latest",
                "rootless": False,
                "network": "none",
                "readonly": True,
            },
        }
        with open(os.path.join(ckpt_dir, "manifest.json"), "w") as f:
            json.dump(manifest, f)
        rootfs_overlay = MagicMock()

        stack, _, _ = self._fake_runsc()
        with (
            stack,
            patch(
                "ray.experimental.sandbox._internal.overlayfs.needs_rootfs_overlay",
                return_value=True,
            ),
            patch.object(
                self.backend, "_prepare_rootfs_overlay", return_value=rootfs_overlay
            ),
            patch.object(
                self.backend, "_build_run_command", return_value=["true"]
            ) as build,
        ):
            restored_id = self.backend.restore_sandbox(ckpt_dir)

        assert build.call_args.kwargs["restore"] is not None
        assert build.call_args.kwargs["rootfs_overlay"] is rootfs_overlay
        meta = self.backend._sandbox_metadata[restored_id]
        assert meta["rootfs_overlay"] is rootfs_overlay

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
        mock_proc.poll.return_value = None
        mock_proc.returncode = 0

        with (
            patch("subprocess.Popen", return_value=mock_proc),
            patch("subprocess.run") as mock_run,
            patch("time.sleep"),
        ):
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
        mock_proc.poll.return_value = None
        mock_proc.returncode = 0

        with (
            patch("shutil.which", return_value="/usr/bin/mock"),
            patch("subprocess.Popen", return_value=mock_proc),
            patch("subprocess.run") as mock_run,
            patch("time.sleep"),
        ):
            mock_run.return_value = MagicMock(
                returncode=0, stdout=json.dumps({"status": "running"})
            )
            restored_id = self.backend.restore_sandbox(
                ckpt_dir,
                cpu=4.0,
                memory="2Gi",
            )
            meta = self.backend._sandbox_metadata[restored_id]
            assert meta["config"].cpu == 4.0
            assert meta["config"].memory == "2Gi"
            assert meta["config"].env == {"ORIG": "VAL"}
            assert meta["config"].workdir == "/orig"
            assert meta["config"].network == "none"
            assert meta["config"].readonly is True

    def test_default_checkpoint_path_format(self):
        sandbox_id, _ = self._create_mock_sandbox(rootless=False)
        stack, _, _ = self._fake_runsc()
        with stack:
            result = self.backend.checkpoint_sandbox(sandbox_id)
        ckpt_path = result["checkpoint_path"]
        assert sandbox_id in ckpt_path
        assert "/checkpoints/" in ckpt_path
        shutil.rmtree(ckpt_path, ignore_errors=True)

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

    def test_checkpoint_copy_failure_preserves_sandbox(self):
        sandbox_id, root_dir = self._create_mock_sandbox(rootless=False)
        stack, _, mock_popen = self._fake_runsc()
        with (
            stack,
            patch(
                "ray.experimental.sandbox.backend.checkpoint_utils.copy_fs_tree",
                side_effect=PermissionError("Permission denied"),
            ),
            patch.object(self.backend, "delete_sandbox") as mock_delete,
        ):
            with pytest.raises(SandboxError, match="Failed to copy rootfs overlay"):
                self.backend.checkpoint_sandbox(sandbox_id, leave_running=False)

        # The checkpoint had stopped the sandbox; it is restored from the staged
        # image, synchronously since the staging directory is then deleted.
        launches = self._launch_commands(mock_popen)
        assert len(launches) == 1
        assert " restore --bundle " + root_dir in launches[0]
        assert "--background" not in launches[0]
        meta = self.backend._sandbox_metadata[sandbox_id]
        assert meta["status"] == SandboxStatus.RUNNING
        assert self.backend.get_status(sandbox_id) == SandboxStatus.RUNNING
        mock_delete.assert_not_called()

    def test_checkpoint_leave_running_false_terminates_on_success(self):
        sandbox_id, _ = self._create_mock_sandbox(rootless=False)
        stack, _, mock_popen = self._fake_runsc()
        with (
            stack,
            patch.object(self.backend, "delete_sandbox") as mock_delete,
        ):
            self.backend.checkpoint_sandbox(sandbox_id, leave_running=False)

            mock_delete.assert_called_once_with(sandbox_id)
        assert not mock_popen.called

    def test_checkpoint_clears_existing_destination(self):
        sandbox_id, root_dir = self._create_mock_sandbox(rootless=False)
        rootfs_dir = os.path.join(root_dir, "rootfs")
        workdir_dir = self.backend._sandbox_metadata[sandbox_id]["workdir"]
        with open(os.path.join(rootfs_dir, "active.txt"), "w") as f:
            f.write("active rootfs")
        with open(os.path.join(workdir_dir, "active.py"), "w") as f:
            f.write("active workdir")

        ckpt_dir = os.path.join(self.temp_dir, "fixed_path_checkpoint")
        state_dir = os.path.join(ckpt_dir, "state")
        old_rootfs = os.path.join(ckpt_dir, "fs", "rootfs")
        old_workdir = os.path.join(ckpt_dir, "fs", "workdir")
        os.makedirs(state_dir, exist_ok=True)
        os.makedirs(old_rootfs, exist_ok=True)
        os.makedirs(old_workdir, exist_ok=True)

        # An earlier checkpoint at the same path.
        with open(os.path.join(ckpt_dir, "manifest.json"), "w") as f:
            f.write("{}")
        stale_dump = os.path.join(state_dir, "stale_dump.img")
        stale_rootfs = os.path.join(old_rootfs, "deleted_guest_file.txt")
        stale_workdir = os.path.join(old_workdir, "deleted_script.py")
        with open(stale_dump, "w") as f:
            f.write("stale dump")
        with open(stale_rootfs, "w") as f:
            f.write("deleted in guest")
        with open(stale_workdir, "w") as f:
            f.write("deleted script in guest")

        stack, _, _ = self._fake_runsc()
        with stack:
            self.backend.checkpoint_sandbox(
                sandbox_id, checkpoint_path=ckpt_dir, leave_running=True
            )

        assert not os.path.exists(stale_dump)
        assert not os.path.exists(stale_rootfs)
        assert not os.path.exists(stale_workdir)
        assert os.path.isfile(os.path.join(ckpt_dir, "fs", "rootfs", "active.txt"))
        assert os.path.isfile(os.path.join(ckpt_dir, "fs", "workdir", "active.py"))
        assert os.path.isfile(os.path.join(ckpt_dir, "manifest.json"))

    def test_checkpoint_to_existing_file_raises(self):
        sandbox_id, _ = self._create_mock_sandbox(rootless=False)
        file_path = os.path.join(self.temp_dir, "file_as_ckpt_path")
        with open(file_path, "w") as f:
            f.write("not a directory")

        with pytest.raises(SandboxError, match="exists and is a file"):
            self.backend.checkpoint_sandbox(sandbox_id, checkpoint_path=file_path)

    def test_checkpoint_restricts_bundle_permissions(self):
        sandbox_id, _ = self._create_mock_sandbox(rootless=False)
        ckpt_dir = os.path.join(self.temp_dir, "perm_checkpoint")

        stack, mock_run, _ = self._fake_runsc()
        with (
            stack,
            patch("os.geteuid", return_value=1000),
            patch("shutil.which", return_value="/usr/bin/sudo"),
        ):
            self.backend.checkpoint_sandbox(
                sandbox_id, checkpoint_path=ckpt_dir, leave_running=True
            )

            chmod_calls = [
                call[0][0]
                for call in mock_run.call_args_list
                if len(call[0]) > 0
                and isinstance(call[0][0], list)
                and "chmod" in call[0][0]
            ]
            assert len(chmod_calls) >= 1
            for cmd in chmod_calls:
                assert "sudo" in cmd
                # Non-interactive: a worker can't answer a password prompt.
                assert cmd[:2] == ["sudo", "-n"]
                assert "chmod" in cmd
                assert "go+rX" not in " ".join(cmd)
            # Ownership changes skip fs/, so guest files keep their owners.
            for cmd in self._runsc_calls(mock_run, "chown"):
                assert not any(arg.endswith("/fs") or "/fs/" in arg for arg in cmd)
                if "-R" in cmd:
                    assert cmd[-1].endswith("/state")

    def test_checkpoint_failure_preserves_previously_valid_bundle(self):
        sandbox_id, _ = self._create_mock_sandbox(rootless=False)
        ckpt_dir = os.path.join(self.temp_dir, "prev_valid_checkpoint")
        state_dir = os.path.join(ckpt_dir, "state")
        fs_dir = os.path.join(ckpt_dir, "fs", "rootfs")
        os.makedirs(state_dir, exist_ok=True)
        os.makedirs(fs_dir, exist_ok=True)

        with open(os.path.join(ckpt_dir, "manifest.json"), "w") as f:
            f.write('{"version": "1.0", "valid": true}')
        with open(os.path.join(ckpt_dir, "config.json"), "w") as f:
            f.write('{"valid": true}')
        with open(os.path.join(state_dir, "valid.img"), "w") as f:
            f.write("valid dump")
        with open(os.path.join(fs_dir, "valid.txt"), "w") as f:
            f.write("valid overlay")

        stack, _, mock_popen = self._fake_runsc(checkpoint_returncode=1)
        with stack:
            with pytest.raises(SandboxError, match="Failed to checkpoint sandbox"):
                self.backend.checkpoint_sandbox(sandbox_id, checkpoint_path=ckpt_dir)
        # A failed runsc checkpoint leaves the sandbox running: nothing to restore.
        assert not mock_popen.called
        assert self.backend.get_status(sandbox_id) == SandboxStatus.RUNNING

        assert os.path.isfile(os.path.join(ckpt_dir, "manifest.json"))
        with open(os.path.join(ckpt_dir, "manifest.json")) as f:
            assert "valid" in f.read()
        assert os.path.isfile(os.path.join(ckpt_dir, "config.json"))
        assert os.path.isfile(os.path.join(state_dir, "valid.img"))
        assert os.path.isfile(os.path.join(fs_dir, "valid.txt"))

        parent = os.path.dirname(ckpt_dir)
        staging_dirs = [
            d for d in os.listdir(parent) if d.startswith("prev_valid_checkpoint.tmp.")
        ]
        assert len(staging_dirs) == 0

    def test_checkpoint_swap_failure_preserves_backup(self):
        sandbox_id, _ = self._create_mock_sandbox(rootless=False)
        ckpt_dir = os.path.join(self.temp_dir, "swap_fail_checkpoint")
        os.makedirs(ckpt_dir, exist_ok=True)
        with open(os.path.join(ckpt_dir, "manifest.json"), "w") as f:
            f.write('{"version": "1.0", "original": true}')

        real_replace = os.replace
        call_count = 0

        def failing_replace(src, dst):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return real_replace(src, dst)
            raise OSError("simulated disk/filesystem error")

        stack, _, mock_popen = self._fake_runsc()
        with (
            stack,
            patch("os.replace", side_effect=failing_replace),
        ):
            with pytest.raises(OSError, match="simulated disk/filesystem error"):
                self.backend.checkpoint_sandbox(sandbox_id, checkpoint_path=ckpt_dir)

        # The swap failed after runsc stopped the sandbox: it is restored from
        # the image left in staging.
        launches = self._launch_commands(mock_popen)
        assert len(launches) == 1
        assert "swap_fail_checkpoint.tmp." in launches[0]
        assert self.backend.get_status(sandbox_id) == SandboxStatus.RUNNING

        parent = os.path.dirname(ckpt_dir)
        backup_dirs = [
            d for d in os.listdir(parent) if d.startswith("swap_fail_checkpoint.old.")
        ]
        assert len(backup_dirs) == 1
        backup_manifest = os.path.join(parent, backup_dirs[0], "manifest.json")
        assert os.path.isfile(backup_manifest)
        with open(backup_manifest) as f:
            assert "original" in f.read()

    def test_restore_rejects_changes_the_saved_state_depends_on(self):
        ckpt_dir = os.path.join(self.temp_dir, "immutable_checkpoint")
        state_dir = os.path.join(ckpt_dir, "state")
        os.makedirs(state_dir, exist_ok=True)

        manifest = {
            "version": "1.0",
            "image": "busybox:latest",
            "sandbox_id": "ray-sandbox-dns",
            "config": {
                "image": "busybox:latest",
                "workdir": "/workspace",
                "rootless": False,
                "network": "public",
                "dns": ["8.8.8.8", "8.8.4.4"],
                "readonly": True,
            },
        }
        with open(os.path.join(ckpt_dir, "manifest.json"), "w") as f:
            json.dump(manifest, f)

        for override in (
            {"network": "none"},
            {"readonly": False},
            {"workdir": "/elsewhere"},
            {"image": "alpine:latest"},
        ):
            (field,) = override
            with pytest.raises(SandboxError, match=f"Cannot restore with {field}="):
                self.backend.restore_sandbox(ckpt_dir, **override)
        self.image_manager.pull_image.assert_not_called()

    def test_restore_copies_workdir_to_host_workdir(self):
        ckpt_dir = os.path.join(self.temp_dir, "workdir_checkpoint")
        state_dir = os.path.join(ckpt_dir, "state")
        fs_workdir = os.path.join(ckpt_dir, "fs", "workdir")
        os.makedirs(state_dir, exist_ok=True)
        os.makedirs(fs_workdir, exist_ok=True)

        with open(os.path.join(fs_workdir, "app.py"), "w") as f:
            f.write("print('hello from workdir')")

        manifest = {
            "version": "1.0",
            "image": "busybox:latest",
            "sandbox_id": "ray-sandbox-src",
            "config": {
                "image": "busybox:latest",
                "workdir": "/workspace",
                "rootless": False,
                "network": "none",
                "readonly": True,
            },
        }
        with open(os.path.join(ckpt_dir, "manifest.json"), "w") as f:
            json.dump(manifest, f)

        stack, _, _ = self._fake_runsc()
        with stack:
            restored_id = self.backend.restore_sandbox(ckpt_dir, workdir="/workspace")
        meta = self.backend._sandbox_metadata[restored_id]
        assert meta["config"].readonly is True
        assert meta["workdir"] == os.path.join(meta["root_dir"], "workspace")
        restored_file = os.path.join(meta["workdir"], "app.py")
        assert os.path.isfile(restored_file)
        with open(restored_file) as f:
            assert f.read() == "print('hello from workdir')"
        prep = self.image_manager.prepare_oci_bundle.call_args.kwargs
        assert prep["workdir_path"] == meta["workdir"]
        assert prep["container_cwd"] == "/workspace"

    def test_restore_workdir_path_traversal_rejected(self):
        ckpt_dir = os.path.join(self.temp_dir, "traversal_checkpoint")
        state_dir = os.path.join(ckpt_dir, "state")
        os.makedirs(state_dir, exist_ok=True)

        manifest = {
            "version": "1.0",
            "image": "busybox:latest",
            "sandbox_id": "ray-sandbox-trav",
            "config": {
                "image": "busybox:latest",
                "workdir": "/../../../../etc/cron.d",
                "rootless": False,
                "network": "none",
                "readonly": True,
            },
        }
        with open(os.path.join(ckpt_dir, "manifest.json"), "w") as f:
            json.dump(manifest, f)

        stack, _, mock_popen = self._fake_runsc()
        with stack:
            with pytest.raises(SandboxCreationError, match="Path traversal detected"):
                self.backend.restore_sandbox(ckpt_dir)
        assert not mock_popen.called

    def test_restore_preserves_and_applies_oci_spec_transform_fn(self):
        ckpt_dir = os.path.join(self.temp_dir, "transform_checkpoint")
        state_dir = os.path.join(ckpt_dir, "state")
        os.makedirs(state_dir, exist_ok=True)

        manifest = {
            "version": "1.0",
            "image": "busybox:latest",
            "sandbox_id": "ray-sandbox-xfrm",
            "config": {
                "image": "busybox:latest",
                "workdir": "/workspace",
                "rootless": False,
                "network": "none",
                "readonly": True,
            },
        }
        with open(os.path.join(ckpt_dir, "manifest.json"), "w") as f:
            json.dump(manifest, f)

        mock_proc = MagicMock()
        mock_proc.poll.return_value = None
        mock_proc.returncode = 0

        def sample_transform(spec):
            spec["annotations"] = spec.get("annotations", {})
            spec["annotations"]["custom.annotation"] = "custom_value"
            return spec

        with (
            patch("subprocess.Popen", return_value=mock_proc),
            patch("subprocess.run") as mock_run,
            patch("time.sleep"),
            patch.object(
                self.backend._image_manager,
                "prepare_oci_bundle",
                side_effect=self._write_bundle,
            ) as mock_prep,
        ):
            mock_run.return_value = MagicMock(
                returncode=0, stdout=json.dumps({"status": "running"})
            )
            restored_id = self.backend.restore_sandbox(
                ckpt_dir,
                _oci_spec_transform_fn=sample_transform,
            )
            meta = self.backend._sandbox_metadata[restored_id]
            assert meta["config"]._oci_spec_transform_fn is sample_transform
            mock_prep.assert_called_once()
            assert (
                mock_prep.call_args.kwargs["_oci_spec_transform_fn"] is sample_transform
            )

    def test_restore_regenerates_bundle_with_overrides(self):
        ckpt_dir = os.path.join(self.temp_dir, "existing_config_checkpoint")
        state_dir = os.path.join(ckpt_dir, "state")
        os.makedirs(state_dir, exist_ok=True)

        manifest = {
            "version": "1.0",
            "image": "busybox:latest",
            "sandbox_id": "ray-sandbox-cfg",
            "config": {
                "image": "busybox:latest",
                "workdir": "/workspace",
                "rootless": False,
                "network": "public",
                "dns": ["8.8.8.8"],
                "readonly": True,
                "env": {"FOO": "BAR"},
                "disk": "1Gi",
                "cidr_allowlist": ["203.0.113.0/24"],
            },
        }
        with open(os.path.join(ckpt_dir, "manifest.json"), "w") as f:
            json.dump(manifest, f)

        # A real checkpoint holds the source's config.json, resolv.conf and
        # hosts. The bundle is generated again from the restored config, so
        # overrides apply; only the guest-writable hosts file is kept.
        with open(os.path.join(ckpt_dir, "config.json"), "w") as f:
            json.dump({"root": {"path": "/old/path/rootfs"}, "mounts": []}, f)
        with open(os.path.join(ckpt_dir, "resolv.conf"), "w") as f:
            f.write("nameserver 8.8.8.8\n")
        with open(os.path.join(ckpt_dir, "hosts"), "w") as f:
            f.write("127.0.0.1\tlocalhost\n10.0.0.5\tadded-by-guest\n")

        def sample_transform(spec):
            return spec

        stack, _, mock_popen = self._fake_runsc()
        with (
            stack,
            patch("ray.experimental.sandbox.backend.gvisor._write_egress_ruleset"),
            patch(
                "ray.experimental.sandbox.backend.gvisor._holder_namespace_ids",
                return_value=None,
            ),
            patch.object(self.backend, "_unreachable_resolver", return_value=None),
        ):
            restored_id = self.backend.restore_sandbox(
                ckpt_dir,
                env={"FOO": "NEW_BAR", "EXTRA": "VAL"},
                dns=["1.1.1.1"],
                cidr_allowlist=["198.51.100.0/24"],
                _oci_spec_transform_fn=sample_transform,
            )
        meta = self.backend._sandbox_metadata[restored_id]
        prep = self.image_manager.prepare_oci_bundle.call_args.kwargs
        assert prep["root_dir"] == meta["root_dir"]
        assert prep["env_dict"] == {"FOO": "NEW_BAR", "EXTRA": "VAL"}
        assert prep["dns"] == ["1.1.1.1"]
        assert prep["disk"] == "1Gi"
        assert prep["network"] == "public"
        assert prep["readonly"] is True
        assert prep["_oci_spec_transform_fn"] is sample_transform
        assert meta["egress_allowlist"] == ["198.51.100.0/24"]
        assert meta["owner_file"] is not None
        with open(os.path.join(meta["root_dir"], "hosts")) as f:
            assert "added-by-guest" in f.read()
        # Launched with the same wrapping as a created network="public" sandbox.
        (launch,) = self._launch_commands(mock_popen)
        assert "nft -f" in launch
        assert " restore --bundle " + meta["root_dir"] in launch

    def test_copy_fs_tree_preserves_symlinks_without_following(self):
        src_dir = os.path.join(self.temp_dir, "symlink_src")
        dst_dir = os.path.join(self.temp_dir, "symlink_dst")
        os.makedirs(src_dir, exist_ok=True)

        # Create a symlink pointing to an absolute external/host file (e.g. /etc/passwd or /dev/null)
        planted_target = "/etc/hosts"
        symlink_path = os.path.join(src_dir, "leak_symlink")
        os.symlink(planted_target, symlink_path)

        copy_fs_tree(src_dir, dst_dir)

        copied_item = os.path.join(dst_dir, "leak_symlink")
        assert os.path.islink(copied_item), "Symlink was not preserved as a symlink!"
        assert os.readlink(copied_item) == planted_target

    def test_restore_missing_runsc_raises_without_leaking(self):
        ckpt_dir = os.path.join(self.temp_dir, "ckpt_missing_runsc")
        os.makedirs(ckpt_dir, exist_ok=True)
        os.makedirs(os.path.join(ckpt_dir, "state"), exist_ok=True)
        manifest = {
            "image": "ubuntu:latest",
            "config": {
                "image": "ubuntu:latest",
                "rootless": False,
                "network": "none",
            },
        }
        with open(os.path.join(ckpt_dir, "manifest.json"), "w") as f:
            json.dump(manifest, f)

        with (
            patch("shutil.which", return_value=None),
            patch.object(self.backend._image_manager, "pull_image") as mock_pull,
        ):
            with pytest.raises(SandboxCreationError, match="runsc' not found in PATH"):
                self.backend.restore_sandbox(ckpt_dir)
            mock_pull.assert_not_called()

    def test_restore_missing_slirp_for_public_network_raises(self):
        ckpt_dir = os.path.join(self.temp_dir, "ckpt_missing_slirp")
        os.makedirs(ckpt_dir, exist_ok=True)
        os.makedirs(os.path.join(ckpt_dir, "state"), exist_ok=True)
        manifest = {
            "image": "ubuntu:latest",
            "config": {
                "image": "ubuntu:latest",
                "rootless": False,
                "network": "public",
            },
        }
        with open(os.path.join(ckpt_dir, "manifest.json"), "w") as f:
            json.dump(manifest, f)

        def fake_which(bin_name):
            if bin_name == "runsc":
                return "/usr/bin/runsc"
            return None

        with (
            patch("shutil.which", side_effect=fake_which),
            patch.object(self.backend._image_manager, "pull_image") as mock_pull,
        ):
            with pytest.raises(
                SandboxCreationError, match="network='public' isolates each sandbox"
            ):
                self.backend.restore_sandbox(ckpt_dir)
            mock_pull.assert_not_called()

    def test_restore_popen_failure_cleans_up(self):
        ckpt_dir = os.path.join(self.temp_dir, "ckpt_popen_fail")
        os.makedirs(ckpt_dir, exist_ok=True)
        os.makedirs(os.path.join(ckpt_dir, "state"), exist_ok=True)
        manifest = {
            "image": "ubuntu:latest",
            "config": {
                "image": "ubuntu:latest",
                "rootless": False,
                "network": "none",
            },
        }
        with open(os.path.join(ckpt_dir, "manifest.json"), "w") as f:
            json.dump(manifest, f)

        with (
            patch("shutil.which", return_value="/usr/bin/runsc"),
            patch.object(self.backend._image_manager, "pull_image"),
            patch.object(
                self.backend._image_manager,
                "prepare_oci_bundle",
                side_effect=self._write_bundle,
            ),
            patch.object(self.backend._image_manager, "release_image") as mock_release,
            patch(
                "subprocess.Popen", side_effect=FileNotFoundError("missing executable")
            ),
        ):
            with pytest.raises(SandboxCreationError, match="Failed to restore sandbox"):
                self.backend.restore_sandbox(ckpt_dir)
            mock_release.assert_called_once()
            # Verify root_dir was cleaned up
            called_args = mock_release.call_args[0]
            instance_id = called_args[1]
            from ray.experimental.sandbox.backend.gvisor import _RAY_SANDBOX_DIR

            root_dir = os.path.join(_RAY_SANDBOX_DIR, instance_id)
            assert not os.path.exists(root_dir)

    def test_api_exports(self):
        assert hasattr(sandbox_api, "restore")
        assert callable(sandbox_api.restore)
        assert "restore" in sandbox_api.__all__

    def test_top_level_restore_preserves_manifest_resources(self):
        ckpt_dir = os.path.join(self.temp_dir, "manifest_res_ckpt")
        state_dir = os.path.join(ckpt_dir, "state")
        os.makedirs(state_dir, exist_ok=True)
        manifest = {
            "version": "1.0",
            "image": "busybox:latest",
            "config": {
                "image": "busybox:latest",
                "cpu": 4.0,
                "memory": "8Gi",
            },
        }
        with open(os.path.join(ckpt_dir, "manifest.json"), "w") as f:
            json.dump(manifest, f)

        with patch.object(sandbox_api.Sandbox, "options") as mock_options:
            mock_remote = MagicMock()
            mock_options.return_value = mock_remote

            # Restore without explicit cpu/memory overrides
            sandbox_api.restore(ckpt_dir)

            # Verify that actor options inherited recorded cpu and memory from manifest
            mock_options.assert_called_once_with(
                num_cpus=4.0,
                memory=8589934592,  # 8Gi in bytes
            )
            mock_remote.remote.assert_called_once_with(
                restore_from=ckpt_dir,
                cpu=None,
                memory=None,
                ttl_seconds=None,
                timeout_seconds=30.0,
            )

    def test_checkpoint_returns_a_node_pinned_handle(self):
        sandbox_id, _ = self._create_mock_sandbox(rootless=False)
        ckpt_dir = os.path.join(self.temp_dir, "handle_ckpt")
        stack, _, _ = self._fake_runsc()
        with (
            stack,
            patch(
                "ray.experimental.sandbox.backend.gvisor.get_current_node_id",
                return_value="node_A",
            ),
        ):
            res = self.backend.checkpoint_sandbox(
                sandbox_id, checkpoint_path=ckpt_dir, leave_running=True
            )
        assert isinstance(res["checkpoint_path"], sandbox_api.CheckpointPath)
        assert res["checkpoint_path"].node_id == "node_A"
        assert res["node_id"] == "node_A"
        with open(os.path.join(ckpt_dir, "manifest.json")) as f:
            manifest = json.load(f)
        assert manifest["node_id"] == "node_A"
        assert "remote" not in manifest

    def test_restore_on_another_node_is_rejected(self):
        ckpt_dir = os.path.join(self.temp_dir, "cross_node_ckpt")
        state_dir = os.path.join(ckpt_dir, "state")
        os.makedirs(state_dir, exist_ok=True)
        manifest = {
            "version": "1.0",
            "image": "busybox:latest",
            "node_id": "node_A",
            "config": {
                "image": "busybox:latest",
                "rootless": False,
            },
        }
        with open(os.path.join(ckpt_dir, "manifest.json"), "w") as f:
            json.dump(manifest, f)

        stack, _, mock_popen = self._fake_runsc()
        with (
            stack,
            patch(
                "ray.experimental.sandbox.backend.gvisor.get_current_node_id",
                return_value="node_B",
            ),
        ):
            with pytest.raises(SandboxError, match="Checkpoints are node-local"):
                self.backend.restore_sandbox(ckpt_dir)
        assert not mock_popen.called

    def test_top_level_restore_pins_to_the_checkpoint_node(self):
        valid_node_id = "a" * 56
        ckpt = sandbox_api.CheckpointPath(
            os.path.join(self.temp_dir, "ckpt_local"),
            node_id=valid_node_id,
            cpu=2.0,
            memory="1Gi",
        )

        with patch.object(sandbox_api.Sandbox, "options") as mock_options:
            mock_remote = MagicMock()
            mock_options.return_value = mock_remote

            sandbox_api.restore(ckpt)
            call_kwargs = mock_options.call_args[1]
            strategy = call_kwargs["scheduling_strategy"]
            assert isinstance(strategy, NodeAffinitySchedulingStrategy)
            assert strategy.node_id == valid_node_id
            assert strategy.soft is False
            # The checkpointed resources are reserved for the restored actor.
            assert call_kwargs["num_cpus"] == 2.0
            assert call_kwargs["memory"] == 1024**3

    def test_sandbox_actor_forwards_config_on_restore(self):
        with patch("ray.experimental.sandbox.sandbox.SandboxRuntime") as mock_rt_cls:
            mock_rt = mock_rt_cls.return_value
            mock_rt.restore.return_value = "sb-restored-id"

            underlying_class = getattr(
                sandbox_api.Sandbox, "__ray_actor_class__", sandbox_api.Sandbox
            )
            _ = underlying_class(
                restore_from="/test/ckpt",
                env={"MY_VAR": "VAL"},
                workdir="/custom/workdir",
                network="none",
                readonly=False,
                capabilities=["CAP_NET_ADMIN"],
                disk="500M",
            )
            mock_rt.restore.assert_called_once_with(
                checkpoint_path="/test/ckpt",
                cpu=None,
                memory=None,
                ttl_seconds=None,
                timeout_seconds=30.0,
                env={"MY_VAR": "VAL"},
                workdir="/custom/workdir",
                network="none",
                readonly=False,
                capabilities=["CAP_NET_ADMIN"],
                disk="500M",
            )

    def test_runtime_checkpoint_leave_running_false_cancels_ttl_timer(self):
        rt = SandboxRuntime()
        instance_id = "test-timer-id"
        mock_timer = MagicMock()
        rt._ttl_timers[instance_id] = mock_timer

        with patch.object(rt._backend, "checkpoint_sandbox") as mock_ckpt:
            mock_ckpt.return_value = {"checkpoint_path": "/test/ckpt"}
            rt.checkpoint(instance_id, leave_running=False)

            mock_timer.cancel.assert_called_once()
            assert instance_id not in rt._ttl_timers

    def _write_checkpoint(self, name, compression=None):
        ckpt_dir = os.path.join(self.temp_dir, name)
        os.makedirs(os.path.join(ckpt_dir, "state"), exist_ok=True)
        manifest = {
            "version": "1.0",
            "image": "busybox:latest",
            "config": {
                "image": "busybox:latest",
                "rootless": False,
                "network": "none",
            },
        }
        if compression is not None:
            manifest["compression"] = compression
        with open(os.path.join(ckpt_dir, "manifest.json"), "w") as f:
            json.dump(manifest, f)
        return ckpt_dir

    def test_checkpoint_records_compression_and_restore_matches_it(self):
        sandbox_id, _ = self._create_mock_sandbox(rootless=False)
        ckpt_dir = os.path.join(self.temp_dir, "compressed_ckpt")
        stack, mock_run, mock_popen = self._fake_runsc()
        with stack:
            self.backend.checkpoint_sandbox(
                sandbox_id,
                checkpoint_path=ckpt_dir,
                compression="flate-best-speed",
            )
            (cmd,) = self._runsc_calls(mock_run, "checkpoint")
            assert "--compression=flate-best-speed" in cmd
            with open(os.path.join(ckpt_dir, "manifest.json")) as f:
                assert json.load(f)["compression"] == "flate-best-speed"
            # The source is restored in place without --background, which runsc
            # supports only for uncompressed images.
            (relaunch,) = self._launch_commands(mock_popen)
            assert "--background" not in relaunch

            mock_popen.reset_mock()
            self.backend.restore_sandbox(ckpt_dir)
            (launch,) = self._launch_commands(mock_popen)
            assert " restore " in launch
            assert "--background" not in launch

    def test_restore_background_defaults_from_compression(self):
        stack, _, mock_popen = self._fake_runsc()
        with stack:
            # Checkpoints from before compression was recorded are uncompressed.
            self.backend.restore_sandbox(self._write_checkpoint("legacy_ckpt"))
            self.backend.restore_sandbox(
                self._write_checkpoint("plain_ckpt", compression="none")
            )
            self.backend.restore_sandbox(
                self._write_checkpoint("plain_ckpt_2", compression="none"),
                background=False,
            )
        launches = self._launch_commands(mock_popen)
        assert ["--background" in launch for launch in launches] == [
            True,
            True,
            False,
        ]

    def test_restore_background_of_compressed_checkpoint_raises(self):
        ckpt_dir = self._write_checkpoint("flate_ckpt", compression="flate-best-speed")
        stack, _, mock_popen = self._fake_runsc()
        with stack:
            with pytest.raises(SandboxError, match="background=False"):
                self.backend.restore_sandbox(ckpt_dir, background=True)
        assert not mock_popen.called
        self.image_manager.pull_image.assert_not_called()

    def test_checkpoint_rejects_unknown_compression(self):
        sandbox_id, _ = self._create_mock_sandbox(rootless=False)
        stack, mock_run, _ = self._fake_runsc()
        with stack:
            with pytest.raises(ValueError, match="Unsupported checkpoint compression"):
                self.backend.checkpoint_sandbox(sandbox_id, compression="zstd")
        assert not self._runsc_calls(mock_run, "checkpoint")

    def test_checkpoint_resume_failure_reports_a_stopped_sandbox(self):
        sandbox_id, _ = self._create_mock_sandbox(rootless=False)
        ckpt_dir = os.path.join(self.temp_dir, "resume_fail_ckpt")
        stack, _, _ = self._fake_runsc(state="stopped")
        with stack:
            with pytest.raises(SandboxError, match="couldn't resume it afterwards"):
                self.backend.checkpoint_sandbox(
                    sandbox_id, checkpoint_path=ckpt_dir, leave_running=True
                )
        # The checkpoint itself is kept, and the sandbox reads as stopped rather
        # than stuck in PAUSED.
        assert os.path.isfile(os.path.join(ckpt_dir, "manifest.json"))
        assert self.backend.get_status(sandbox_id) == SandboxStatus.TERMINATED

    def test_checkpoint_saves_the_allowlist_in_force(self):
        sandbox_id, _ = self._create_mock_sandbox(rootless=False, network="public")
        # Narrowed with set_egress_allowlist after creation (cidr_allowlist=None).
        self.backend._sandbox_metadata[sandbox_id]["egress_allowlist"] = ["10.0.0.5/32"]
        ckpt_dir = os.path.join(self.temp_dir, "allowlist_ckpt")
        stack, _, _ = self._fake_runsc()
        with stack, patch.object(self.backend, "delete_sandbox"):
            self.backend.checkpoint_sandbox(
                sandbox_id, checkpoint_path=ckpt_dir, leave_running=False
            )
        with open(os.path.join(ckpt_dir, "manifest.json")) as f:
            manifest = json.load(f)
        assert manifest["config"]["cidr_allowlist"] == ["10.0.0.5/32"]

    def test_checkpoint_refuses_a_directory_that_is_not_a_checkpoint(self):
        sandbox_id, _ = self._create_mock_sandbox(rootless=False)
        project = os.path.join(self.temp_dir, "project")
        os.makedirs(project)
        with open(os.path.join(project, "data.csv"), "w") as f:
            f.write("keep me")

        stack, mock_run, _ = self._fake_runsc()
        with stack:
            with pytest.raises(SandboxError, match="doesn't hold a checkpoint"):
                self.backend.checkpoint_sandbox(sandbox_id, checkpoint_path=project)
        assert os.listdir(project) == ["data.csv"]
        assert not self._runsc_calls(mock_run, "checkpoint")
        assert self.backend.get_status(sandbox_id) == SandboxStatus.RUNNING

    def _timing_out_checkpoint(self, mock_run):
        def run(args, *a, **k):
            if isinstance(args, list) and "checkpoint" in args:
                raise subprocess.TimeoutExpired(args, k.get("timeout"))
            return fake_run(args, *a, **k)

        fake_run = mock_run.side_effect
        mock_run.side_effect = run

    def test_checkpoint_timeout_keeps_a_save_in_progress(self):
        sandbox_id, _ = self._create_mock_sandbox(rootless=False)
        ckpt_dir = os.path.join(self.temp_dir, "slow_ckpt")
        proc = self.backend._sandbox_metadata[sandbox_id]["proc"]
        # The save is still going: the launch process doesn't exit.
        proc.wait.side_effect = subprocess.TimeoutExpired("runsc", 1)
        stack, mock_run, mock_popen = self._fake_runsc()
        with stack:
            self._timing_out_checkpoint(mock_run)
            with pytest.raises(SandboxTimeoutError, match="left in place"):
                self.backend.checkpoint_sandbox(
                    sandbox_id, checkpoint_path=ckpt_dir, timeout_seconds=1
                )
        # The staging directory the save writes to survives, and nothing
        # restores over the still-saving sandbox.
        staging = [
            d for d in os.listdir(self.temp_dir) if d.startswith("slow_ckpt.tmp.")
        ]
        assert len(staging) == 1
        assert not mock_popen.called

    def test_checkpoint_timeout_waits_for_the_save_to_finish(self):
        sandbox_id, root_dir = self._create_mock_sandbox(rootless=False)
        ckpt_dir = os.path.join(self.temp_dir, "late_ckpt")
        stack, mock_run, mock_popen = self._fake_runsc()
        with stack:
            self._timing_out_checkpoint(mock_run)
            # The launch process exits once the save stops the sandbox.
            self.backend.checkpoint_sandbox(
                sandbox_id, checkpoint_path=ckpt_dir, timeout_seconds=1
            )
        assert os.path.isfile(os.path.join(ckpt_dir, "manifest.json"))
        (launch,) = self._launch_commands(mock_popen)
        assert " restore --bundle " + root_dir in launch

    def _write_image(self, name, superblock_byte):
        image_dir = os.path.join(self.temp_dir, "images", name)
        os.makedirs(image_dir, exist_ok=True)
        with open(os.path.join(image_dir, "rootfs.erofs"), "wb") as f:
            f.write(b"\0" * 1024 + bytes([superblock_byte]) * 128 + b"\0" * 2944)
        return image_dir

    def test_restore_refuses_a_different_build_of_the_image(self):
        sandbox_id, _ = self._create_mock_sandbox(rootless=False)
        self.backend._sandbox_metadata[sandbox_id]["image_dir"] = self._write_image(
            "source", 1
        )
        ckpt_dir = os.path.join(self.temp_dir, "fingerprint_ckpt")
        stack, _, mock_popen = self._fake_runsc()
        with stack:
            self.backend.checkpoint_sandbox(
                sandbox_id, checkpoint_path=ckpt_dir, leave_running=False
            )
            with open(os.path.join(ckpt_dir, "manifest.json")) as f:
                assert json.load(f)["image_fingerprint"]
            mock_popen.reset_mock()

            # An identical build restores.
            self.image_manager.pull_image.return_value = self._write_image("same", 1)
            self.backend.restore_sandbox(ckpt_dir)
            assert mock_popen.call_count == 1

            # Another build would make gVisor panic: refuse it up front.
            self.image_manager.pull_image.return_value = self._write_image("other", 2)
            with pytest.raises(SandboxCreationError, match="differs from the one"):
                self.backend.restore_sandbox(ckpt_dir)
            assert mock_popen.call_count == 1


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
