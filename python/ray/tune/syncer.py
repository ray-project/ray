from typing import Any, Callable, Dict, List, TYPE_CHECKING, Type, Union

import distutils
import logging
import os
import time
from dataclasses import dataclass

from inspect import isclass
from shlex import quote

import ray
import yaml
from ray import services
from ray.tune import TuneError
from ray.tune.callback import Callback
from ray.tune.checkpoint_manager import Checkpoint
from ray.tune.result import NODE_IP
from ray.util.debug import log_once
from ray.tune.utils.util import env_integer
from ray.tune.cluster_info import get_ssh_key, get_ssh_user
from ray.tune.sync_client import (CommandBasedClient, get_sync_client,
                                  get_cloud_sync_client, NOOP)

if TYPE_CHECKING:
    from ray.tune.trial import Trial

logger = logging.getLogger(__name__)

# Syncing period for syncing local checkpoints to cloud.
# In env variable is not set, sync happens every 300 seconds.
CLOUD_SYNC_PERIOD = 300

# Syncing period for syncing worker logs to driver.
NODE_SYNC_PERIOD = 300

_log_sync_warned = False
_syncers = {}


def wait_for_sync():
    for syncer in _syncers.values():
        syncer.wait()


def set_sync_periods(sync_config):
    """Sets sync periods from config."""
    global CLOUD_SYNC_PERIOD
    global NODE_SYNC_PERIOD
    if os.environ.get("TUNE_CLOUD_SYNC_S"):
        logger.warning("'TUNE_CLOUD_SYNC_S' is deprecated. Set "
                       "`cloud_sync_period` via tune.SyncConfig instead.")
        CLOUD_SYNC_PERIOD = env_integer(key="TUNE_CLOUD_SYNC_S", default=300)
    NODE_SYNC_PERIOD = int(sync_config.node_sync_period)
    CLOUD_SYNC_PERIOD = int(sync_config.cloud_sync_period)


def log_sync_template(options=""):
    """Template enabling syncs between driver and worker when possible.
    Requires ray cluster to be started with the autoscaler. Also requires
    rsync to be installed.

    Args:
        options (str): Additional rsync options.

    Returns:
        Sync template with source and target parameters. None if rsync
        unavailable.
    """
    if not distutils.spawn.find_executable("rsync"):
        if log_once("tune:rsync"):
            logger.error("Log sync requires rsync to be installed.")
        return None
    global _log_sync_warned
    ssh_key = get_ssh_key()
    if ssh_key is None:
        if not _log_sync_warned:
            logger.debug("Log sync requires cluster to be setup with "
                         "`ray up`.")
            _log_sync_warned = True
        return None

    rsh = "ssh -i {ssh_key} -o ConnectTimeout=120s -o StrictHostKeyChecking=no"
    rsh = rsh.format(ssh_key=quote(ssh_key))
    template = "rsync {options} -savz -e {rsh} {{source}} {{target}}"
    return template.format(options=options, rsh=quote(rsh))


@dataclass
class SyncConfig:
    """Configuration object for syncing.

    Args:
        upload_dir (str): Optional URI to sync training results and checkpoints
            to (e.g. ``s3://bucket``, ``gs://bucket`` or ``hdfs://path``).
        sync_to_cloud (func|str): Function for syncing the local_dir to and
            from upload_dir. If string, then it must be a string template that
            includes `{source}` and `{target}` for the syncer to run. If not
            provided, the sync command defaults to standard S3, gsutil or HDFS
            sync commands. By default local_dir is synced to remote_dir every
            300 seconds. To change this, set the TUNE_CLOUD_SYNC_S
            environment variable in the driver machine.
        sync_to_driver (func|str|bool): Function for syncing trial logdir from
            remote node to local. If string, then it must be a string template
            that includes `{source}` and `{target}` for the syncer to run.
            If True or not provided, it defaults to using rsync. If False,
            syncing to driver is disabled.
        sync_on_checkpoint (bool): Force sync-down of trial checkpoint to
            driver. If set to False, checkpoint syncing from worker to driver
            is asynchronous and best-effort. This does not affect persistent
            storage syncing. Defaults to True.
        node_sync_period (int): Syncing period for syncing worker logs to
            driver. Defaults to 300.
        cloud_sync_period (int): Syncing period for syncing local
            checkpoints to cloud. Defaults to 300.
    """
    upload_dir: str = None
    sync_to_cloud: Any = None
    sync_to_driver: Any = None
    sync_on_checkpoint: bool = True
    node_sync_period: int = 300
    cloud_sync_period: int = 300


class Syncer:
    def __init__(self, local_dir, remote_dir, sync_client=NOOP):
        """Syncs between two directories with the sync_function.

        Arguments:
            local_dir (str): Directory to sync. Uniquely identifies the syncer.
            remote_dir (str): Remote directory to sync with.
            sync_client (SyncClient): Client for syncing between local_dir and
                remote_dir. Defaults to a Noop.
        """
        self._local_dir = (os.path.join(local_dir, "")
                           if local_dir else local_dir)
        self._remote_dir = remote_dir
        self.last_sync_up_time = float("-inf")
        self.last_sync_down_time = float("-inf")
        self.sync_client = sync_client

    def sync_up_if_needed(self, sync_period):
        """Syncs up if time since last sync up is greather than sync_period.

        Arguments:
            sync_period (int): Time period between subsequent syncs.
        """

        if time.time() - self.last_sync_up_time > sync_period:
            self.sync_up()

    def sync_down_if_needed(self, sync_period):
        """Syncs down if time since last sync down is greather than sync_period.

        Arguments:
            sync_period (int): Time period between subsequent syncs.
        """
        if time.time() - self.last_sync_down_time > sync_period:
            self.sync_down()

    def sync_up(self):
        """Attempts to start the sync-up to the remote path.

        Returns:
            Whether the sync (if feasible) was successfully started.
        """
        result = False
        if self.validate_hosts(self._local_dir, self._remote_path):
            try:
                result = self.sync_client.sync_up(self._local_dir,
                                                  self._remote_path)
                self.last_sync_up_time = time.time()
            except Exception:
                logger.exception("Sync execution failed.")
        return result

    def sync_down(self):
        """Attempts to start the sync-down from the remote path.

        Returns:
             Whether the sync (if feasible) was successfully started.
        """
        result = False
        if self.validate_hosts(self._local_dir, self._remote_path):
            try:
                result = self.sync_client.sync_down(self._remote_path,
                                                    self._local_dir)
                self.last_sync_down_time = time.time()
            except Exception:
                logger.exception("Sync execution failed.")
        return result

    def validate_hosts(self, source, target):
        if not (source and target):
            logger.debug("Source or target is empty, skipping log sync for "
                         "{}".format(self._local_dir))
            return False
        return True

    def wait(self):
        """Waits for the sync client to complete the current sync."""
        self.sync_client.wait()

    def reset(self):
        self.last_sync_up_time = float("-inf")
        self.last_sync_down_time = float("-inf")
        self.sync_client.reset()

    def close(self):
        self.sync_client.close()

    @property
    def _remote_path(self):
        return self._remote_dir


class CloudSyncer(Syncer):
    """Syncer for syncing files to/from the cloud."""

    def __init__(self, local_dir, remote_dir, sync_client):
        super(CloudSyncer, self).__init__(local_dir, remote_dir, sync_client)

    def sync_up_if_needed(self):
        return super(CloudSyncer, self).sync_up_if_needed(CLOUD_SYNC_PERIOD)

    def sync_down_if_needed(self):
        return super(CloudSyncer, self).sync_down_if_needed(CLOUD_SYNC_PERIOD)


class NodeSyncer(Syncer):
    """Syncer for syncing files to/from a remote dir to a local dir."""

    def __init__(self, local_dir, remote_dir, sync_client):
        self.local_ip = services.get_node_ip_address()
        self.worker_ip = None
        super(NodeSyncer, self).__init__(local_dir, remote_dir, sync_client)

    def set_worker_ip(self, worker_ip):
        """Sets the worker IP to sync logs from."""
        self.worker_ip = worker_ip

    def has_remote_target(self):
        """Returns whether the Syncer has a remote target."""
        if not self.worker_ip:
            logger.debug("Worker IP unknown, skipping sync for %s",
                         self._local_dir)
            return False
        if self.worker_ip == self.local_ip:
            logger.debug("Worker IP is local IP, skipping sync for %s",
                         self._local_dir)
            return False
        return True

    def sync_up_if_needed(self):
        if not self.has_remote_target():
            return True
        return super(NodeSyncer, self).sync_up_if_needed(NODE_SYNC_PERIOD)

    def sync_down_if_needed(self):
        if not self.has_remote_target():
            return True
        return super(NodeSyncer, self).sync_down_if_needed(NODE_SYNC_PERIOD)

    def sync_up_to_new_location(self, worker_ip):
        if worker_ip != self.worker_ip:
            logger.debug("Setting new worker IP to %s", worker_ip)
            self.set_worker_ip(worker_ip)
            self.reset()
            if not self.sync_up():
                logger.warning(
                    "Sync up to new location skipped. This should not occur.")
        else:
            logger.warning("Sync attempted to same IP %s.", worker_ip)

    def sync_up(self):
        if not self.has_remote_target():
            return True
        return super(NodeSyncer, self).sync_up()

    def sync_down(self):
        if not self.has_remote_target():
            return True
        logger.debug("Syncing from %s to %s", self._remote_path,
                     self._local_dir)
        return super(NodeSyncer, self).sync_down()

    @property
    def _remote_path(self):
        ssh_user = get_ssh_user()
        global _log_sync_warned
        if not self.has_remote_target():
            return None
        if ssh_user is None:
            if not _log_sync_warned:
                logger.error("Syncer requires cluster to be setup with "
                             "`ray up`.")
                _log_sync_warned = True
            return None
        return "{}@{}:{}/".format(ssh_user, self.worker_ip, self._remote_dir)


def get_cloud_syncer(local_dir, remote_dir=None, sync_function=None):
    """Returns a Syncer.

    This syncer is in charge of syncing the local_dir with upload_dir.

    Args:
        local_dir (str): Source directory for syncing.
        remote_dir (str): Target directory for syncing. If not provided, a
            no-op Syncer is returned.
        sync_function (func | str): Function for syncing the local_dir to
            remote_dir. If string, then it must be a string template for
            syncer to run. If not provided, it defaults
            to standard S3, gsutil or HDFS sync commands.

    Raises:
        ValueError if malformed remote_dir.
    """
    key = (local_dir, remote_dir)

    if key in _syncers:
        return _syncers[key]

    if not remote_dir:
        _syncers[key] = CloudSyncer(local_dir, remote_dir, NOOP)
        return _syncers[key]

    client = get_sync_client(sync_function)

    if client:
        _syncers[key] = CloudSyncer(local_dir, remote_dir, client)
        return _syncers[key]
    sync_client = get_cloud_sync_client(remote_dir)
    _syncers[key] = CloudSyncer(local_dir, remote_dir, sync_client)
    return _syncers[key]


def get_node_syncer(local_dir, remote_dir=None, sync_function=None):
    """Returns a NodeSyncer.

    Args:
        local_dir (str): Source directory for syncing.
        remote_dir (str): Target directory for syncing. If not provided, a
            noop Syncer is returned.
        sync_function (func|str|bool): Function for syncing the local_dir to
            remote_dir. If string, then it must be a string template for
            syncer to run. If True or not provided, it defaults rsync. If
            False, a noop Syncer is returned.
    """
    key = (local_dir, remote_dir)
    if key in _syncers:
        return _syncers[key]
    elif isclass(sync_function) and issubclass(sync_function, Syncer):
        _syncers[key] = sync_function(local_dir, remote_dir, None)
        return _syncers[key]
    elif not remote_dir or sync_function is False:
        sync_client = NOOP
    elif sync_function and sync_function is not True:
        sync_client = get_sync_client(sync_function)
    else:
        sync = log_sync_template()
        if sync:
            sync_client = CommandBasedClient(sync, sync)
            sync_client.set_logdir(local_dir)
        else:
            sync_client = NOOP

    _syncers[key] = NodeSyncer(local_dir, remote_dir, sync_client)
    return _syncers[key]


class SyncerCallback(Callback):
    def __init__(self, sync_function: Union[None, bool, Callable]):
        self._sync_function = sync_function
        self._syncers: Dict["Trial", NodeSyncer] = {}

    def _get_trial_syncer(self, trial: "Trial"):
        if trial not in self._syncers:
            self._syncers[trial] = self._create_trial_syncer(trial)
        return self._syncers[trial]

    def _create_trial_syncer(self, trial: "Trial"):
        return get_node_syncer(
            trial.logdir,
            remote_dir=trial.logdir,
            sync_function=self._sync_function)

    def _sync_trial_checkpoint(self, trial: "Trial", checkpoint: Checkpoint):
        if checkpoint.storage == Checkpoint.MEMORY:
            return

        # Local import to avoid circular dependencies between syncer and
        # trainable
        from ray.tune.durable_trainable import DurableTrainable

        trial_syncer = self._get_trial_syncer(trial)
        if trial.sync_on_checkpoint:
            try:
                # Wait for any other syncs to finish. We need to sync again
                # after this to handle checkpoints taken mid-sync.
                trial_syncer.wait()
            except TuneError as e:
                # Errors occurring during this wait are not fatal for this
                # checkpoint, so it should just be logged.
                logger.error(
                    "Trial %s: An error occurred during the "
                    "checkpoint pre-sync wait - %s", trial, str(e))
            # Force sync down and wait before tracking the new checkpoint.
            try:
                if trial_syncer.sync_down():
                    trial_syncer.wait()
                else:
                    logger.error(
                        "Trial %s: Checkpoint sync skipped. "
                        "This should not happen.", trial)
            except TuneError as e:
                if issubclass(trial.get_trainable_cls(), DurableTrainable):
                    # Even though rsync failed the trainable can restore
                    # from remote durable storage.
                    logger.error("Trial %s: Sync error - %s", trial, str(e))
                else:
                    # If the trainable didn't have remote storage to upload
                    # to then this checkpoint may have been lost, so we
                    # shouldn't track it with the checkpoint_manager.
                    raise e
            if not issubclass(trial.get_trainable_cls(), DurableTrainable):
                if not os.path.exists(checkpoint.value):
                    raise TuneError("Trial {}: Checkpoint path {} not "
                                    "found after successful sync down.".format(
                                        trial, checkpoint.value))

    def on_trial_start(self, iteration: int, trials: List["Trial"],
                       trial: "Trial", **info):
        self._get_trial_syncer(trial)

    def on_trial_result(self, iteration: int, trials: List["Trial"],
                        trial: "Trial", result: Dict, **info):
        trial_syncer = self._get_trial_syncer(trial)
        trial_syncer.set_worker_ip(result.get(NODE_IP))
        trial_syncer.sync_down_if_needed()

    def on_trial_complete(self, iteration: int, trials: List["Trial"],
                          trial: "Trial", **info):
        trial_syncer = self._get_trial_syncer(trial)
        if NODE_IP in trial.last_result:
            trainable_ip = trial.last_result[NODE_IP]
        else:
            trainable_ip = ray.get(trial.runner.get_current_ip.remote())
        trial_syncer.set_worker_ip(trainable_ip)
        trial_syncer.sync_down_if_needed()
        trial_syncer.close()

    def on_checkpoint(self, iteration: int, trials: List["Trial"],
                      trial: "Trial", checkpoint: Checkpoint, **info):
        self._sync_trial_checkpoint(trial, checkpoint)


def detect_sync_to_driver(
        sync_to_driver: Union[None, bool, Type],
        cluster_config_file: str = "~/ray_bootstrap_config.yaml"):
    from ray.tune.integration.docker import DockerSyncer

    if isinstance(sync_to_driver, Type):
        return sync_to_driver
    elif isinstance(sync_to_driver, bool) and sync_to_driver is False:
        return sync_to_driver

    # Else: True or None. Auto-detect.
    cluster_config_file = os.path.expanduser(cluster_config_file)
    if not os.path.exists(cluster_config_file):
        return sync_to_driver

    with open(cluster_config_file, "rt") as fp:
        config = yaml.safe_load(fp.read())

    if config.get("docker"):
        logger.debug(
            "Detected docker autoscaling environment. Using `DockerSyncer` "
            "as sync client. If this is not correct or leads to errors, "
            "please pass a `sync_to_driver` parameter in the `SyncConfig` to "
            "`tune.run().` to manually configure syncing behavior.")
        return DockerSyncer

    if config.get("provider", {}).get("type", "") == "kubernetes":
        from ray.tune.integration.kubernetes import (
            NamespacedKubernetesSyncer, try_import_kubernetes)
        if not try_import_kubernetes():
            logger.warning(
                "Detected Ray autoscaling environment on Kubernetes, "
                "but Kubernetes Python CLI is not installed. "
                "Checkpoint syncing may not work properly across "
                "multiple pods. Be sure to install 'kubernetes' on "
                "each container.")

        namespace = config["provider"].get("namespace", "ray")
        logger.debug(
            f"Detected Ray autoscaling environment on Kubernetes. Using "
            f"`NamespacedKubernetesSyncer` with namespace `{namespace}` "
            f"as sync client. If this is not correct or leads to errors, "
            f"please pass a `sync_to_driver` parameter in the `SyncConfig` "
            f"to `tune.run()` to manually configure syncing behavior..")
        return NamespacedKubernetesSyncer(namespace)

    return sync_to_driver
