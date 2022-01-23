import glob
import os
import pickle
import shutil
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
import yaml

from collections import deque

import ray
from ray.rllib import _register_all

from ray import tune
from ray.tune.integration.docker import DockerSyncer
from ray.tune.integration.kubernetes import KubernetesSyncer
from ray.tune.sync_client import NOOP
from ray.tune.syncer import (CommandBasedClient, detect_cluster_syncer,
                             get_cloud_sync_client, SyncerCallback)
from ray.tune.utils.callback import create_default_callbacks


class TestSyncFunctionality(unittest.TestCase):
    def setUp(self):
        # Wait up to 1.5 seconds for placement groups when starting a trial
        os.environ["TUNE_PLACEMENT_GROUP_WAIT_S"] = "1.5"
        # Block for results even when placement groups are pending
        os.environ["TUNE_TRIAL_STARTUP_GRACE_PERIOD"] = "0"
        os.environ["TUNE_TRIAL_RESULT_WAIT_TIME_S"] = "99999"

        ray.init(num_cpus=2)

    def tearDown(self):
        ray.shutdown()
        _register_all()  # re-register the evicted objects

    def testSyncConfigDeprecation(self):
        with self.assertWarnsRegex(
                DeprecationWarning, expected_regex="sync_period"):
            sync_conf = tune.SyncConfig(
                node_sync_period=4, cloud_sync_period=8)
            self.assertEqual(sync_conf.sync_period, 4)

        with self.assertWarnsRegex(
                DeprecationWarning, expected_regex="sync_period"):
            sync_conf = tune.SyncConfig(node_sync_period=4)
            self.assertEqual(sync_conf.sync_period, 4)

        with self.assertWarnsRegex(
                DeprecationWarning, expected_regex="sync_period"):
            sync_conf = tune.SyncConfig(cloud_sync_period=8)
            self.assertEqual(sync_conf.sync_period, 8)

        with self.assertWarnsRegex(
                DeprecationWarning, expected_regex="syncer"):
            sync_conf = tune.SyncConfig(
                sync_to_driver="a", sync_to_cloud="b", upload_dir=None)
            self.assertEqual(sync_conf.syncer, "a")

        with self.assertWarnsRegex(
                DeprecationWarning, expected_regex="syncer"):
            sync_conf = tune.SyncConfig(
                sync_to_driver="a", sync_to_cloud="b", upload_dir="c")
            self.assertEqual(sync_conf.syncer, "b")

        with self.assertWarnsRegex(
                DeprecationWarning, expected_regex="syncer"):
            sync_conf = tune.SyncConfig(sync_to_cloud="b", upload_dir=None)
            self.assertEqual(sync_conf.syncer, None)

        with self.assertWarnsRegex(
                DeprecationWarning, expected_regex="syncer"):
            sync_conf = tune.SyncConfig(sync_to_driver="a", upload_dir="c")
            self.assertEqual(sync_conf.syncer, None)

    @patch("ray.tune.sync_client.S3_PREFIX", "test")
    def testCloudProperString(self):
        with self.assertRaises(ValueError):
            [trial] = tune.run(
                "__fake",
                name="foo",
                max_failures=0,
                stop={
                    "training_iteration": 1
                },
                sync_config=tune.SyncConfig(**{
                    "upload_dir": "test",
                    "syncer": "ls {target}"
                })).trials

        with self.assertRaises(ValueError):
            [trial] = tune.run(
                "__fake",
                name="foo",
                max_failures=0,
                stop={
                    "training_iteration": 1
                },
                sync_config=tune.SyncConfig(**{
                    "upload_dir": "test",
                    "syncer": "ls {source}"
                })).trials

        tmpdir = tempfile.mkdtemp()
        logfile = os.path.join(tmpdir, "test.log")

        [trial] = tune.run(
            "__fake",
            name="foo",
            max_failures=0,
            stop={
                "training_iteration": 1
            },
            sync_config=tune.SyncConfig(
                **{
                    "upload_dir": "test",
                    "syncer": "echo {source} {target} > " + logfile
                })).trials
        with open(logfile) as f:
            lines = f.read()
            self.assertTrue("test" in lines)
        shutil.rmtree(tmpdir)

    def testClusterProperString(self):
        """Tests that invalid commands throw.."""
        with self.assertRaises(ValueError):
            # This raises ValueError because logger is init in safe zone.
            sync_config = tune.SyncConfig(syncer="ls {target}")
            [trial] = tune.run(
                "__fake",
                name="foo",
                max_failures=0,
                stop={
                    "training_iteration": 1
                },
                sync_config=sync_config,
            ).trials

        with self.assertRaises(ValueError):
            # This raises ValueError because logger is init in safe zone.
            sync_config = tune.SyncConfig(syncer="ls {source}")
            [trial] = tune.run(
                "__fake",
                name="foo",
                max_failures=0,
                sync_config=sync_config,
                stop={
                    "training_iteration": 1
                }).trials

        with patch.object(CommandBasedClient, "_execute") as mock_fn:
            with patch("ray.tune.syncer.get_node_ip_address") as mock_sync:
                sync_config = tune.SyncConfig(syncer="echo {source} {target}")
                mock_sync.return_value = "0.0.0.0"
                [trial] = tune.run(
                    "__fake",
                    name="foo",
                    max_failures=0,
                    sync_config=sync_config,
                    stop={
                        "training_iteration": 1
                    }).trials
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
            stop={
                "training_iteration": 1
            },
            sync_config=sync_config).trials
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

        sync_config = tune.SyncConfig(
            upload_dir="test", syncer=counter, sync_period=1)
        # This was originally set to 0.5
        os.environ["TUNE_GLOBAL_CHECKPOINT_S"] = "0"
        self.addCleanup(
            lambda: os.environ.pop("TUNE_GLOBAL_CHECKPOINT_S", None))
        [trial] = tune.run(
            trainable,
            name="foo",
            max_failures=0,
            local_dir=tmpdir,
            stop={
                "training_iteration": 10
            },
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
            stop={
                "training_iteration": 1
            },
            sync_config=sync_config).trials
        test_file_path = os.path.join(trial.logdir, "test.log2")
        self.assertFalse(os.path.exists(test_file_path))

        with patch("ray.tune.syncer.get_node_ip_address") as mock_sync:
            mock_sync.return_value = "0.0.0.0"
            sync_config = tune.SyncConfig(syncer=sync_func_driver)
            [trial] = tune.run(
                "__fake",
                name="foo",
                max_failures=0,
                stop={
                    "training_iteration": 1
                },
                sync_config=sync_config).trials
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
                stop={
                    "training_iteration": 1
                },
                sync_config=sync_config).trials
            self.assertEqual(mock_sync.call_count, 0)

    def testCloudSyncExclude(self):
        captured = deque(maxlen=1)
        captured.append("")

        def always_true(*args, **kwargs):
            return True

        def capture_popen(command, *args, **kwargs):
            captured.append(command)

        with patch("subprocess.Popen", capture_popen), patch(
                "distutils.spawn.find_executable", always_true):
            # S3
            s3_client = get_cloud_sync_client("s3://test-bucket/test-dir")
            s3_client.sync_down("s3://test-bucket/test-dir/remote_source",
                                "local_target")

            self.assertEqual(
                captured[0].strip(),
                "aws s3 sync s3://test-bucket/test-dir/remote_source "
                "local_target --only-show-errors")

            s3_client.sync_down(
                "s3://test-bucket/test-dir/remote_source",
                "local_target",
                exclude=["*/checkpoint_*"])
            self.assertEqual(
                captured[0].strip(),
                "aws s3 sync s3://test-bucket/test-dir/remote_source "
                "local_target --only-show-errors "
                "--exclude '*/checkpoint_*'")

            s3_client.sync_down(
                "s3://test-bucket/test-dir/remote_source",
                "local_target",
                exclude=["*/checkpoint_*", "*.big"])
            self.assertEqual(
                captured[0].strip(),
                "aws s3 sync s3://test-bucket/test-dir/remote_source "
                "local_target --only-show-errors "
                "--exclude '*/checkpoint_*' --exclude '*.big'")

            # GS
            gs_client = get_cloud_sync_client("gs://test-bucket/test-dir")
            gs_client.sync_down("gs://test-bucket/test-dir/remote_source",
                                "local_target")

            self.assertEqual(
                captured[0].strip(), "gsutil rsync -r  "
                "gs://test-bucket/test-dir/remote_source "
                "local_target")

            gs_client.sync_down(
                "gs://test-bucket/test-dir/remote_source",
                "local_target",
                exclude=["*/checkpoint_*"])
            self.assertEqual(
                captured[0].strip(), "gsutil rsync -r "
                "-x '(.*/checkpoint_.*)' "
                "gs://test-bucket/test-dir/remote_source "
                "local_target")

            gs_client.sync_down(
                "gs://test-bucket/test-dir/remote_source",
                "local_target",
                exclude=["*/checkpoint_*", "*.big"])
            self.assertEqual(
                captured[0].strip(), "gsutil rsync -r "
                "-x '(.*/checkpoint_.*)|(.*.big)' "
                "gs://test-bucket/test-dir/remote_source "
                "local_target")

    def testSyncDetection(self):
        kubernetes_conf = {
            "provider": {
                "type": "kubernetes",
                "namespace": "test_ray"
            }
        }
        docker_conf = {
            "docker": {
                "image": "bogus"
            },
            "provider": {
                "type": "aws"
            }
        }
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
                tune.SyncConfig(syncer=DockerSyncer), kubernetes_file)
            self.assertTrue(issubclass(syncer, DockerSyncer))

    @patch("ray.tune.syncer.log_sync_template",
           lambda: "rsync {source} {target}")
    def testNoSyncToDriver(self):
        """Test that sync to driver is disabled"""

        class _Trial:
            def __init__(self, id, logdir):
                self.id = id,
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


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
