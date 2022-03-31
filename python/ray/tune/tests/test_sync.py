import glob
import io
import os
import pickle
import shutil
import sys
import tarfile
import tempfile
import time
import unittest
from unittest.mock import patch
import yaml

from collections import deque

import ray
from ray.rllib import _register_all

from ray import tune
from ray.tune import TuneError
from ray.tune.integration.docker import DockerSyncer
from ray.tune.integration.kubernetes import KubernetesSyncer
from ray.tune.sync_client import NOOP, RemoteTaskClient
from ray.tune.syncer import (
    CommandBasedClient,
    detect_cluster_syncer,
    get_cloud_sync_client,
    SyncerCallback,
)
from ray.tune.utils.callback import create_default_callbacks


class TestSyncFunctionality(unittest.TestCase):
    def setUp(self):
        ray.init(num_cpus=2)

    def tearDown(self):
        ray.shutdown()
        _register_all()  # re-register the evicted objects

    def testSyncConfigDeprecation(self):
        with self.assertWarnsRegex(DeprecationWarning, expected_regex="sync_period"):
            sync_conf = tune.SyncConfig(node_sync_period=4, cloud_sync_period=8)
            self.assertEqual(sync_conf.sync_period, 4)

        with self.assertWarnsRegex(DeprecationWarning, expected_regex="sync_period"):
            sync_conf = tune.SyncConfig(node_sync_period=4)
            self.assertEqual(sync_conf.sync_period, 4)

        with self.assertWarnsRegex(DeprecationWarning, expected_regex="sync_period"):
            sync_conf = tune.SyncConfig(cloud_sync_period=8)
            self.assertEqual(sync_conf.sync_period, 8)

        with self.assertWarnsRegex(DeprecationWarning, expected_regex="syncer"):
            sync_conf = tune.SyncConfig(
                sync_to_driver="a", sync_to_cloud="b", upload_dir=None
            )
            self.assertEqual(sync_conf.syncer, "a")

        with self.assertWarnsRegex(DeprecationWarning, expected_regex="syncer"):
            sync_conf = tune.SyncConfig(
                sync_to_driver="a", sync_to_cloud="b", upload_dir="c"
            )
            self.assertEqual(sync_conf.syncer, "b")

        with self.assertWarnsRegex(DeprecationWarning, expected_regex="syncer"):
            sync_conf = tune.SyncConfig(sync_to_cloud="b", upload_dir=None)
            self.assertEqual(sync_conf.syncer, None)

        with self.assertWarnsRegex(DeprecationWarning, expected_regex="syncer"):
            sync_conf = tune.SyncConfig(sync_to_driver="a", upload_dir="c")
            self.assertEqual(sync_conf.syncer, None)

    @patch("ray.tune.sync_client.S3_PREFIX", "test")
    def testCloudProperString(self):
        with self.assertRaises(ValueError):
            [trial] = tune.run(
                "__fake",
                name="foo",
                max_failures=0,
                stop={"training_iteration": 1},
                sync_config=tune.SyncConfig(
                    **{"upload_dir": "test", "syncer": "ls {target}"}
                ),
            ).trials

        with self.assertRaises(ValueError):
            [trial] = tune.run(
                "__fake",
                name="foo",
                max_failures=0,
                stop={"training_iteration": 1},
                sync_config=tune.SyncConfig(
                    **{"upload_dir": "test", "syncer": "ls {source}"}
                ),
            ).trials

        tmpdir = tempfile.mkdtemp()
        logfile = os.path.join(tmpdir, "test.log")

        [trial] = tune.run(
            "__fake",
            name="foo",
            max_failures=0,
            stop={"training_iteration": 1},
            sync_config=tune.SyncConfig(
                **{
                    "upload_dir": "test",
                    "syncer": "echo {source} {target} > " + logfile,
                }
            ),
        ).trials
        with open(logfile) as f:
            lines = f.read()
            self.assertTrue("test" in lines)
        shutil.rmtree(tmpdir)

    def testClusterProperString(self):
        """Tests that invalid commands throw.."""
        with self.assertRaises(TuneError):
            # This raises ValueError because logger is init in safe zone.
            sync_config = tune.SyncConfig(syncer="ls {target}")
            [trial] = tune.run(
                "__fake",
                name="foo",
                max_failures=0,
                stop={"training_iteration": 1},
                sync_config=sync_config,
            ).trials

        with self.assertRaises(TuneError):
            # This raises ValueError because logger is init in safe zone.
            sync_config = tune.SyncConfig(syncer="ls {source}")
            [trial] = tune.run(
                "__fake",
                name="foo",
                max_failures=0,
                sync_config=sync_config,
                stop={"training_iteration": 1},
            ).trials

        with patch.object(CommandBasedClient, "_execute") as mock_fn:
            with patch("ray.tune.syncer.get_node_ip_address") as mock_sync:
                sync_config = tune.SyncConfig(syncer="echo {source} {target}")
                mock_sync.return_value = "0.0.0.0"
                [trial] = tune.run(
                    "__fake",
                    name="foo",
                    max_failures=0,
                    sync_config=sync_config,
                    stop={"training_iteration": 1},
                ).trials
                self.assertGreater(mock_fn.call_count, 0)

    def testCloudFunctions(self):
        tmpdir = tempfile.mkdtemp()
        tmpdir2 = tempfile.mkdtemp()
        os.mkdir(os.path.join(tmpdir2, "foo"))

        def sync_func(local, remote):
            for filename in glob.glob(os.path.join(local, "*.json")):
                shutil.copy(filename, remote)

        sync_config = tune.SyncConfig(upload_dir=tmpdir2, syncer=sync_func)
        [trial] = tune.run(
            "__fake",
            name="foo",
            max_failures=0,
            local_dir=tmpdir,
            stop={"training_iteration": 1},
            sync_config=sync_config,
        ).trials
        test_file_path = glob.glob(os.path.join(tmpdir2, "foo", "*.json"))
        self.assertTrue(test_file_path)
        shutil.rmtree(tmpdir)
        shutil.rmtree(tmpdir2)

    @patch("ray.tune.sync_client.S3_PREFIX", "test")
    def testCloudSyncPeriod(self):
        """Tests that changing SYNC_PERIOD affects syncing frequency."""
        tmpdir = tempfile.mkdtemp()

        def trainable(config):
            for i in range(10):
                time.sleep(1)
                tune.report(score=i)

        def counter(local, remote):
            count_file = os.path.join(tmpdir, "count.txt")
            if not os.path.exists(count_file):
                count = 0
            else:
                with open(count_file, "rb") as fp:
                    count = pickle.load(fp)
            count += 1
            with open(count_file, "wb") as fp:
                pickle.dump(count, fp)

        sync_config = tune.SyncConfig(upload_dir="test", syncer=counter, sync_period=1)
        # This was originally set to 0.5
        os.environ["TUNE_GLOBAL_CHECKPOINT_S"] = "0"
        self.addCleanup(lambda: os.environ.pop("TUNE_GLOBAL_CHECKPOINT_S", None))
        [trial] = tune.run(
            trainable,
            name="foo",
            max_failures=0,
            local_dir=tmpdir,
            stop={"training_iteration": 10},
            sync_config=sync_config,
        ).trials

        count_file = os.path.join(tmpdir, "count.txt")
        with open(count_file, "rb") as fp:
            count = pickle.load(fp)

        self.assertEqual(count, 12)
        shutil.rmtree(tmpdir)

    def testClusterSyncFunction(self):
        def sync_func_driver(source, target):
            assert ":" in source, "Source {} not a remote path.".format(source)
            assert ":" not in target, "Target is supposed to be local."
            with open(os.path.join(target, "test.log2"), "w") as f:
                print("writing to", f.name)
                f.write(source)

        sync_config = tune.SyncConfig(syncer=sync_func_driver, sync_period=5)

        [trial] = tune.run(
            "__fake",
            name="foo",
            max_failures=0,
            stop={"training_iteration": 1},
            sync_config=sync_config,
        ).trials
        test_file_path = os.path.join(trial.logdir, "test.log2")
        self.assertFalse(os.path.exists(test_file_path))

        with patch("ray.tune.syncer.get_node_ip_address") as mock_sync:
            mock_sync.return_value = "0.0.0.0"
            sync_config = tune.SyncConfig(syncer=sync_func_driver)
            [trial] = tune.run(
                "__fake",
                name="foo",
                max_failures=0,
                stop={"training_iteration": 1},
                sync_config=sync_config,
            ).trials
        test_file_path = os.path.join(trial.logdir, "test.log2")
        self.assertTrue(os.path.exists(test_file_path))
        os.remove(test_file_path)

    def testNoSync(self):
        """Sync should not run on a single node."""

        def sync_func(source, target):
            pass

        sync_config = tune.SyncConfig(syncer=sync_func)

        with patch.object(CommandBasedClient, "_execute") as mock_sync:
            [trial] = tune.run(
                "__fake",
                name="foo",
                max_failures=0,
                stop={"training_iteration": 1},
                sync_config=sync_config,
            ).trials
            self.assertEqual(mock_sync.call_count, 0)

    def testCloudSyncExclude(self):
        captured = deque(maxlen=1)
        captured.append("")

        def always_true(*args, **kwargs):
            return True

        def capture_popen(command, *args, **kwargs):
            captured.append(command)

        with patch("subprocess.Popen", capture_popen), patch(
            "distutils.spawn.find_executable", always_true
        ):
            # S3
            s3_client = get_cloud_sync_client("s3://test-bucket/test-dir")
            s3_client.sync_down(
                "s3://test-bucket/test-dir/remote_source", "local_target"
            )

            self.assertEqual(
                captured[0].strip(),
                "aws s3 sync s3://test-bucket/test-dir/remote_source "
                "local_target --exact-timestamps --only-show-errors",
            )

            s3_client.sync_down(
                "s3://test-bucket/test-dir/remote_source",
                "local_target",
                exclude=["*/checkpoint_*"],
            )
            self.assertEqual(
                captured[0].strip(),
                "aws s3 sync s3://test-bucket/test-dir/remote_source "
                "local_target --exact-timestamps --only-show-errors "
                "--exclude '*/checkpoint_*'",
            )

            s3_client.sync_down(
                "s3://test-bucket/test-dir/remote_source",
                "local_target",
                exclude=["*/checkpoint_*", "*.big"],
            )
            self.assertEqual(
                captured[0].strip(),
                "aws s3 sync s3://test-bucket/test-dir/remote_source "
                "local_target --exact-timestamps --only-show-errors "
                "--exclude '*/checkpoint_*' --exclude '*.big'",
            )

            # GS
            gs_client = get_cloud_sync_client("gs://test-bucket/test-dir")
            gs_client.sync_down(
                "gs://test-bucket/test-dir/remote_source", "local_target"
            )

            self.assertEqual(
                captured[0].strip(),
                "gsutil rsync -r  "
                "gs://test-bucket/test-dir/remote_source "
                "local_target",
            )

            gs_client.sync_down(
                "gs://test-bucket/test-dir/remote_source",
                "local_target",
                exclude=["*/checkpoint_*"],
            )
            self.assertEqual(
                captured[0].strip(),
                "gsutil rsync -r "
                "-x '(.*/checkpoint_.*)' "
                "gs://test-bucket/test-dir/remote_source "
                "local_target",
            )

            gs_client.sync_down(
                "gs://test-bucket/test-dir/remote_source",
                "local_target",
                exclude=["*/checkpoint_*", "*.big"],
            )
            self.assertEqual(
                captured[0].strip(),
                "gsutil rsync -r "
                "-x '(.*/checkpoint_.*)|(.*.big)' "
                "gs://test-bucket/test-dir/remote_source "
                "local_target",
            )

    def testSyncDetection(self):
        kubernetes_conf = {"provider": {"type": "kubernetes", "namespace": "test_ray"}}
        docker_conf = {"docker": {"image": "bogus"}, "provider": {"type": "aws"}}
        aws_conf = {"provider": {"type": "aws"}}

        with tempfile.TemporaryDirectory() as dir:
            kubernetes_file = os.path.join(dir, "kubernetes.yaml")
            with open(kubernetes_file, "wt") as fp:
                yaml.safe_dump(kubernetes_conf, fp)

            docker_file = os.path.join(dir, "docker.yaml")
            with open(docker_file, "wt") as fp:
                yaml.safe_dump(docker_conf, fp)

            aws_file = os.path.join(dir, "aws.yaml")
            with open(aws_file, "wt") as fp:
                yaml.safe_dump(aws_conf, fp)

            kubernetes_syncer = detect_cluster_syncer(None, kubernetes_file)
            self.assertTrue(issubclass(kubernetes_syncer, KubernetesSyncer))
            self.assertEqual(kubernetes_syncer._namespace, "test_ray")

            docker_syncer = detect_cluster_syncer(None, docker_file)
            self.assertTrue(issubclass(docker_syncer, DockerSyncer))

            aws_syncer = detect_cluster_syncer(None, aws_file)
            self.assertEqual(aws_syncer, None)

            # Should still return DockerSyncer, since it was passed explicitly
            syncer = detect_cluster_syncer(
                tune.SyncConfig(syncer=DockerSyncer), kubernetes_file
            )
            self.assertTrue(issubclass(syncer, DockerSyncer))

    @patch(
        "ray.tune.syncer.get_rsync_template_if_available",
        lambda: "rsync {source} {target}",
    )
    def testNoSyncToDriver(self):
        """Test that sync to driver is disabled"""

        class _Trial:
            def __init__(self, id, logdir):
                self.id = (id,)
                self.logdir = logdir

        trial = _Trial("0", "some_dir")

        sync_config = tune.SyncConfig(syncer=None)

        # Create syncer callbacks
        callbacks = create_default_callbacks([], sync_config, loggers=None)
        syncer_callback = callbacks[-1]

        # Sanity check that we got the syncer callback
        self.assertTrue(isinstance(syncer_callback, SyncerCallback))

        # Sync function should be false (no sync to driver)
        self.assertEqual(syncer_callback._sync_function, False)

        # Sync to driver is disabled, so this should be no-op
        trial_syncer = syncer_callback._get_trial_syncer(trial)
        self.assertEqual(trial_syncer.sync_client, NOOP)

    def testSyncWaitRetry(self):
        class CountingClient(CommandBasedClient):
            def __init__(self, *args, **kwargs):
                self._sync_ups = 0
                self._sync_downs = 0
                super(CountingClient, self).__init__(*args, **kwargs)

            def _start_process(self, cmd):
                if "UPLOAD" in cmd:
                    self._sync_ups += 1
                elif "DOWNLOAD" in cmd:
                    self._sync_downs += 1
                    if self._sync_downs == 1:
                        self._last_cmd = "echo DOWNLOAD && true"
                return super(CountingClient, self)._start_process(cmd)

        client = CountingClient(
            "echo UPLOAD {source} {target} && false",
            "echo DOWNLOAD {source} {target} && false",
            "echo DELETE {target}",
        )

        # Fail always
        with self.assertRaisesRegex(TuneError, "Failed sync even after"):
            client.sync_up("test_source", "test_target")
            client.wait_or_retry(max_retries=3, backoff_s=0)

        self.assertEquals(client._sync_ups, 3)

        # Succeed after second try
        client.sync_down("test_source", "test_target")
        client.wait_or_retry(max_retries=3, backoff_s=0)

        self.assertEquals(client._sync_downs, 2)

    def testSyncRemoteTaskOnlyDifferences(self):
        """Tests the RemoteTaskClient sync client.

        In this test we generate a directory with multiple files.
        We then use both ``sync_down`` and ``sync_up`` to synchronize
        these to different directories (on the same node). We then assert
        that the files have been transferred correctly.

        We then edit one of the files and add another one. We then sync
        up/down again. In this sync, we assert that only modified and new
        files are transferred.
        """
        temp_source = tempfile.mkdtemp()
        temp_up_target = tempfile.mkdtemp()
        temp_down_target = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, temp_source)
        self.addCleanup(shutil.rmtree, temp_up_target)
        self.addCleanup(shutil.rmtree, temp_down_target)

        os.makedirs(os.path.join(temp_source, "A", "a1"))
        os.makedirs(os.path.join(temp_source, "A", "a2"))
        os.makedirs(os.path.join(temp_source, "B", "b1"))
        with open(os.path.join(temp_source, "level_0.txt"), "wt") as fp:
            fp.write("Level 0\n")
        with open(os.path.join(temp_source, "A", "level_a1.txt"), "wt") as fp:
            fp.write("Level A1\n")
        with open(os.path.join(temp_source, "A", "a1", "level_a2.txt"), "wt") as fp:
            fp.write("Level A2\n")
        with open(os.path.join(temp_source, "B", "level_b1.txt"), "wt") as fp:
            fp.write("Level B1\n")

        this_node_ip = ray.util.get_node_ip_address()

        # Sync everything up
        client = RemoteTaskClient(store_pack_future=True)
        client.sync_up(source=temp_source, target=(this_node_ip, temp_up_target))
        client.wait()

        # Assume that we synced everything up to second level
        self.assertTrue(
            os.path.exists(os.path.join(temp_up_target, "A", "a1", "level_a2.txt")),
            msg=f"Contents: {os.listdir(temp_up_target)}",
        )
        with open(os.path.join(temp_up_target, "A", "a1", "level_a2.txt"), "rt") as fp:
            self.assertEqual(fp.read(), "Level A2\n")

        # Sync everything down
        client.sync_down(source=(this_node_ip, temp_source), target=temp_down_target)
        client.wait()

        # Assume that we synced everything up to second level
        self.assertTrue(
            os.path.exists(os.path.join(temp_down_target, "A", "a1", "level_a2.txt")),
            msg=f"Contents: {os.listdir(temp_down_target)}",
        )
        with open(
            os.path.join(temp_down_target, "A", "a1", "level_a2.txt"), "rt"
        ) as fp:
            self.assertEqual(fp.read(), "Level A2\n")

        # Now, edit some stuff in our source. Then confirm only these
        # edited files are synced
        with open(os.path.join(temp_source, "A", "a1", "level_a2.txt"), "wt") as fp:
            fp.write("Level X2\n")  # Same length
        with open(os.path.join(temp_source, "A", "level_a1x.txt"), "wt") as fp:
            fp.write("Level A1X\n")  # New file

        # Sync up
        client.sync_up(source=temp_source, target=(this_node_ip, temp_up_target))

        # Hi-jack futures
        files_stats = ray.get(client._last_files_stats)
        tarball = ray.get(client._pack_future)
        client.wait()

        # Existing file should have new content
        with open(os.path.join(temp_up_target, "A", "a1", "level_a2.txt"), "rt") as fp:
            self.assertEqual(fp.read(), "Level X2\n")

        # New file should be there
        with open(os.path.join(temp_up_target, "A", "level_a1x.txt"), "rt") as fp:
            self.assertEqual(fp.read(), "Level A1X\n")

        # Old file should be there
        with open(os.path.join(temp_up_target, "B", "level_b1.txt"), "rt") as fp:
            self.assertEqual(fp.read(), "Level B1\n")

        # In the target dir, level_a1x was not contained
        self.assertIn(os.path.join("A", "a1", "level_a2.txt"), files_stats)
        self.assertNotIn(os.path.join("A", "level_a1x.txt"), files_stats)

        # Inspect tarball
        with tarfile.open(fileobj=io.BytesIO(tarball)) as tar:
            files_in_tar = tar.getnames()
            self.assertIn(os.path.join("A", "a1", "level_a2.txt"), files_in_tar)
            self.assertIn(os.path.join("A", "level_a1x.txt"), files_in_tar)
            self.assertNotIn(os.path.join("A", "level_a1.txt"), files_in_tar)
            # 6 directories (including root) + 2 files
            self.assertEqual(len(files_in_tar), 8, msg=str(files_in_tar))

        # Sync down
        client.sync_down(source=(this_node_ip, temp_source), target=temp_down_target)

        # Hi-jack futures
        files_stats = client._last_files_stats
        tarball = ray.get(client._pack_future)
        client.wait()

        # Existing file should have new content
        with open(
            os.path.join(temp_down_target, "A", "a1", "level_a2.txt"), "rt"
        ) as fp:
            self.assertEqual(fp.read(), "Level X2\n")

        # New file should be there
        with open(os.path.join(temp_down_target, "A", "level_a1x.txt"), "rt") as fp:
            self.assertEqual(fp.read(), "Level A1X\n")

        # Old file should be there
        with open(os.path.join(temp_down_target, "B", "level_b1.txt"), "rt") as fp:
            self.assertEqual(fp.read(), "Level B1\n")

        # In the target dir, level_a1x was not contained
        self.assertIn(os.path.join("A", "a1", "level_a2.txt"), files_stats)
        self.assertNotIn(os.path.join("A", "level_a1x.txt"), files_stats)

        # Inspect tarball
        with tarfile.open(fileobj=io.BytesIO(tarball)) as tar:
            files_in_tar = tar.getnames()
            self.assertIn(os.path.join("A", "a1", "level_a2.txt"), files_in_tar)
            self.assertIn(os.path.join("A", "level_a1x.txt"), files_in_tar)
            self.assertNotIn(os.path.join("A", "level_a1.txt"), files_in_tar)
            # 6 directories (including root) + 2 files
            self.assertEqual(len(files_in_tar), 8, msg=str(files_in_tar))


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
