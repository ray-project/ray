from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
from types import FunctionType

from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.evaluation.rollout_worker import RolloutWorker, \
    _validate_multiagent_config
from ray.rllib.offline import NoopOutput, JsonReader, MixedInput, JsonWriter, \
    ShuffledInput
from ray.rllib.utils import merge_dicts, try_import_tf
from ray.rllib.utils.memory import ray_get_and_free

tf = try_import_tf()

logger = logging.getLogger(__name__)


@DeveloperAPI
class WorkerSet(object):
    """Represents a set of RolloutWorkers.

    There must be one local worker copy, and zero or more remote workers.
    """

    def __init__(self,
                 env_creator,
                 policy,
                 trainer_config=None,
                 num_workers=0,
                 logdir=None,
                 _setup=True):
        """Create a new WorkerSet and initialize its workers.

        Arguments:
            env_creator (func): Function that returns env given env config.
            policy (cls): rllib.policy.Policy class.
            trainer_config (dict): Optional dict that extends the common
                config of the Trainer class.
            num_workers (int): Number of remote rollout workers to create.
            logdir (str): Optional logging directory for workers.
            _setup (bool): Whether to setup workers. This is only for testing.
        """

        if not trainer_config:
            from ray.rllib.agents.trainer import COMMON_CONFIG
            trainer_config = COMMON_CONFIG

        self._env_creator = env_creator
        self._policy = policy
        self._remote_config = trainer_config
        self._num_workers = num_workers
        self._logdir = logdir

        if _setup:
            self._local_config = merge_dicts(
                trainer_config,
                {"tf_session_args": trainer_config["local_tf_session_args"]})

            # Always create a local worker
            self._local_worker = self._make_worker(
                RolloutWorker, env_creator, policy, 0, self._local_config)

            # Create a number of remote workers
            self._remote_workers = []
            self.add_workers(num_workers)

    def local_worker(self):
        """Return the local rollout worker."""
        return self._local_worker

    def remote_workers(self):
        """Return a list of remote rollout workers."""
        return self._remote_workers

    def add_workers(self, num_workers):
        """Create and add a number of remote workers to this worker set."""
        remote_args = {
            "num_cpus": self._remote_config["num_cpus_per_worker"],
            "num_gpus": self._remote_config["num_gpus_per_worker"],
            "resources": self._remote_config["custom_resources_per_worker"],
        }
        cls = RolloutWorker.as_remote(**remote_args).remote
        self._remote_workers.extend([
            self._make_worker(cls, self._env_creator, self._policy, i + 1,
                              self._remote_config) for i in range(num_workers)
        ])

    def reset(self, new_remote_workers):
        """Called to change the set of remote workers."""
        self._remote_workers = new_remote_workers

    def stop(self):
        """Stop all rollout workers."""
        self.local_worker().stop()
        for w in self.remote_workers():
            w.stop.remote()
            w.__ray_terminate__.remote()

    @DeveloperAPI
    def foreach_worker(self, func):
        """Apply the given function to each worker instance."""

        local_result = [func(self.local_worker())]
        remote_results = ray_get_and_free(
            [w.apply.remote(func) for w in self.remote_workers()])
        return local_result + remote_results

    @DeveloperAPI
    def foreach_worker_with_index(self, func):
        """Apply the given function to each worker instance.

        The index will be passed as the second arg to the given function.
        """

        local_result = [func(self.local_worker(), 0)]
        remote_results = ray_get_and_free([
            w.apply.remote(func, i + 1)
            for i, w in enumerate(self.remote_workers())
        ])
        return local_result + remote_results

    @staticmethod
    def _from_existing(local_worker, remote_workers=None):
        workers = WorkerSet(None, None, {}, _setup=False)
        workers._local_worker = local_worker
        workers._remote_workers = remote_workers or []
        return workers

    def _make_worker(self, cls, env_creator, policy, worker_index, config):
        def session_creator():
            logger.debug("Creating TF session {}".format(
                config["tf_session_args"]))
            return tf.Session(
                config=tf.ConfigProto(**config["tf_session_args"]))

        if isinstance(config["input"], FunctionType):
            input_creator = config["input"]
        elif config["input"] == "sampler":
            input_creator = (lambda ioctx: ioctx.default_sampler_input())
        elif isinstance(config["input"], dict):
            input_creator = (lambda ioctx: ShuffledInput(
                MixedInput(config["input"], ioctx), config[
                    "shuffle_buffer_size"]))
        else:
            input_creator = (lambda ioctx: ShuffledInput(
                JsonReader(config["input"], ioctx), config[
                    "shuffle_buffer_size"]))

        if isinstance(config["output"], FunctionType):
            output_creator = config["output"]
        elif config["output"] is None:
            output_creator = (lambda ioctx: NoopOutput())
        elif config["output"] == "logdir":
            output_creator = (lambda ioctx: JsonWriter(
                ioctx.log_dir,
                ioctx,
                max_file_size=config["output_max_file_size"],
                compress_columns=config["output_compress_columns"]))
        else:
            output_creator = (lambda ioctx: JsonWriter(
                config["output"],
                ioctx,
                max_file_size=config["output_max_file_size"],
                compress_columns=config["output_compress_columns"]))

        if config["input"] == "sampler":
            input_evaluation = []
        else:
            input_evaluation = config["input_evaluation"]

        # Fill in the default policy if 'None' is specified in multiagent
        if config["multiagent"]["policies"]:
            tmp = config["multiagent"]["policies"]
            _validate_multiagent_config(tmp, allow_none_graph=True)
            for k, v in tmp.items():
                if v[0] is None:
                    tmp[k] = (policy, v[1], v[2], v[3])
            policy = tmp

        return cls(
            env_creator,
            policy,
            policy_mapping_fn=config["multiagent"]["policy_mapping_fn"],
            policies_to_train=config["multiagent"]["policies_to_train"],
            tf_session_creator=(session_creator
                                if config["tf_session_args"] else None),
            batch_steps=config["sample_batch_size"],
            batch_mode=config["batch_mode"],
            episode_horizon=config["horizon"],
            preprocessor_pref=config["preprocessor_pref"],
            sample_async=config["sample_async"],
            compress_observations=config["compress_observations"],
            num_envs=config["num_envs_per_worker"],
            observation_filter=config["observation_filter"],
            clip_rewards=config["clip_rewards"],
            clip_actions=config["clip_actions"],
            env_config=config["env_config"],
            model_config=config["model"],
            policy_config=config,
            worker_index=worker_index,
            monitor_path=self._logdir if config["monitor"] else None,
            log_dir=self._logdir,
            log_level=config["log_level"],
            callbacks=config["callbacks"],
            input_creator=input_creator,
            input_evaluation=input_evaluation,
            output_creator=output_creator,
            remote_worker_envs=config["remote_worker_envs"],
            remote_env_batch_wait_ms=config["remote_env_batch_wait_ms"],
            soft_horizon=config["soft_horizon"],
            no_done_at_end=config["no_done_at_end"],
            seed=(config["seed"] + worker_index)
            if config["seed"] is not None else None,
            _fake_sampler=config.get("_fake_sampler", False))
