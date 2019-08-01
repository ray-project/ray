from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from datetime import datetime
import copy
import logging
import os
import pickle
import six
import time
import tempfile

import ray
from ray.exceptions import RayError
from ray.rllib.models import MODEL_DEFAULTS
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.evaluation.metrics import collect_metrics
from ray.rllib.optimizers.policy_optimizer import PolicyOptimizer
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.utils.annotations import override, PublicAPI, DeveloperAPI
from ray.rllib.utils import FilterManager, deep_update, merge_dicts
from ray.rllib.utils.memory import ray_get_and_free
from ray.rllib.utils import try_import_tf
from ray.tune.registry import ENV_CREATOR, register_env, _global_registry
from ray.tune.trainable import Trainable
from ray.tune.trial import ExportFormat
from ray.tune.resources import Resources
from ray.tune.logger import UnifiedLogger
from ray.tune.result import DEFAULT_RESULTS_DIR

tf = try_import_tf()

logger = logging.getLogger(__name__)

# Max number of times to retry a worker failure. We shouldn't try too many
# times in a row since that would indicate a persistent cluster issue.
MAX_WORKER_FAILURE_RETRIES = 3

# yapf: disable
# __sphinx_doc_begin__
COMMON_CONFIG = {
    # === Debugging ===
    # Whether to write episode stats and videos to the agent log dir
    "monitor": False,
    # Set the ray.rllib.* log level for the agent process and its workers.
    # Should be one of DEBUG, INFO, WARN, or ERROR. The DEBUG level will also
    # periodically print out summaries of relevant internal dataflow (this is
    # also printed out once at startup at the INFO level).
    "log_level": "INFO",
    # Callbacks that will be run during various phases of training. These all
    # take a single "info" dict as an argument. For episode callbacks, custom
    # metrics can be attached to the episode by updating the episode object's
    # custom metrics dict (see examples/custom_metrics_and_callbacks.py). You
    # may also mutate the passed in batch data in your callback.
    "callbacks": {
        "on_episode_start": None,     # arg: {"env": .., "episode": ...}
        "on_episode_step": None,      # arg: {"env": .., "episode": ...}
        "on_episode_end": None,       # arg: {"env": .., "episode": ...}
        "on_sample_end": None,        # arg: {"samples": .., "worker": ...}
        "on_train_result": None,      # arg: {"trainer": ..., "result": ...}
        "on_postprocess_traj": None,  # arg: {
                                      #   "agent_id": ..., "episode": ...,
                                      #   "pre_batch": (before processing),
                                      #   "post_batch": (after processing),
                                      #   "all_pre_batches": (other agent ids),
                                      # }
    },
    # Whether to attempt to continue training if a worker crashes.
    "ignore_worker_failures": False,
    # Log system resource metrics to results.
    "log_sys_usage": True,

    # === Policy ===
    # Arguments to pass to model. See models/catalog.py for a full list of the
    # available model options.
    "model": MODEL_DEFAULTS,
    # Arguments to pass to the policy optimizer. These vary by optimizer.
    "optimizer": {},

    # === Environment ===
    # Discount factor of the MDP
    "gamma": 0.99,
    # Number of steps after which the episode is forced to terminate. Defaults
    # to `env.spec.max_episode_steps` (if present) for Gym envs.
    "horizon": None,
    # Calculate rewards but don't reset the environment when the horizon is
    # hit. This allows value estimation and RNN state to span across logical
    # episodes denoted by horizon. This only has an effect if horizon != inf.
    "soft_horizon": False,
    # Arguments to pass to the env creator
    "env_config": {},
    # Environment name can also be passed via config
    "env": None,
    # Whether to clip rewards prior to experience postprocessing. Setting to
    # None means clip for Atari only.
    "clip_rewards": None,
    # Whether to np.clip() actions to the action space low/high range spec.
    "clip_actions": True,
    # Whether to use rllib or deepmind preprocessors by default
    "preprocessor_pref": "deepmind",
    # The default learning rate
    "lr": 0.0001,

    # === Evaluation ===
    # Evaluate with every `evaluation_interval` training iterations.
    # The evaluation stats will be reported under the "evaluation" metric key.
    # Note that evaluation is currently not parallelized, and that for Ape-X
    # metrics are already only reported for the lowest epsilon workers.
    "evaluation_interval": None,
    # Number of episodes to run per evaluation period.
    "evaluation_num_episodes": 10,
    # Extra arguments to pass to evaluation workers.
    # Typical usage is to pass extra args to evaluation env creator
    # and to disable exploration by computing deterministic actions
    # TODO(kismuz): implement determ. actions and include relevant keys hints
    "evaluation_config": {},

    # === Resources ===
    # Number of actors used for parallelism
    "num_workers": 2,
    # Number of GPUs to allocate to the driver. Note that not all algorithms
    # can take advantage of driver GPUs. This can be fraction (e.g., 0.3 GPUs).
    "num_gpus": 0,
    # Number of CPUs to allocate per worker.
    "num_cpus_per_worker": 1,
    # Number of GPUs to allocate per worker. This can be fractional.
    "num_gpus_per_worker": 0,
    # Any custom resources to allocate per worker.
    "custom_resources_per_worker": {},
    # Number of CPUs to allocate for the driver. Note: this only takes effect
    # when running in Tune.
    "num_cpus_for_driver": 1,

    # === Execution ===
    # Number of environments to evaluate vectorwise per worker.
    "num_envs_per_worker": 1,
    # Default sample batch size (unroll length). Batches of this size are
    # collected from workers until train_batch_size is met. When using
    # multiple envs per worker, this is multiplied by num_envs_per_worker.
    "sample_batch_size": 200,
    # Training batch size, if applicable. Should be >= sample_batch_size.
    # Samples batches will be concatenated together to this size for training.
    "train_batch_size": 200,
    # Whether to rollout "complete_episodes" or "truncate_episodes"
    "batch_mode": "truncate_episodes",
    # Use a background thread for sampling (slightly off-policy, usually not
    # advisable to turn on unless your env specifically requires it)
    "sample_async": False,
    # Element-wise observation filter, either "NoFilter" or "MeanStdFilter"
    "observation_filter": "NoFilter",
    # Whether to synchronize the statistics of remote filters.
    "synchronize_filters": True,
    # Configure TF for single-process operation by default
    "tf_session_args": {
        # note: overriden by `local_tf_session_args`
        "intra_op_parallelism_threads": 2,
        "inter_op_parallelism_threads": 2,
        "gpu_options": {
            "allow_growth": True,
        },
        "log_device_placement": False,
        "device_count": {
            "CPU": 1
        },
        "allow_soft_placement": True,  # required by PPO multi-gpu
    },
    # Override the following tf session args on the local worker
    "local_tf_session_args": {
        # Allow a higher level of parallelism by default, but not unlimited
        # since that can cause crashes with many concurrent drivers.
        "intra_op_parallelism_threads": 8,
        "inter_op_parallelism_threads": 8,
    },
    # Whether to LZ4 compress individual observations
    "compress_observations": False,
    # Wait for metric batches for at most this many seconds. Those that
    # have not returned in time will be collected in the next iteration.
    "collect_metrics_timeout": 180,
    # Smooth metrics over this many episodes.
    "metrics_smoothing_episodes": 100,
    # If using num_envs_per_worker > 1, whether to create those new envs in
    # remote processes instead of in the same worker. This adds overheads, but
    # can make sense if your envs can take much time to step / reset
    # (e.g., for StarCraft). Use this cautiously; overheads are significant.
    "remote_worker_envs": False,
    # Timeout that remote workers are waiting when polling environments.
    # 0 (continue when at least one env is ready) is a reasonable default,
    # but optimal value could be obtained by measuring your environment
    # step / reset and model inference perf.
    "remote_env_batch_wait_ms": 0,
    # Minimum time per iteration
    "min_iter_time_s": 0,
    # Minimum env steps to optimize for per train call. This value does
    # not affect learning, only the length of iterations.
    "timesteps_per_iteration": 0,
    # This argument, in conjunction with worker_index, sets the random seed of
    # each worker, so that identically configured trials will have identical
    # results. This makes experiments reproducible.
    "seed": None,

    # === Offline Datasets ===
    # Specify how to generate experiences:
    #  - "sampler": generate experiences via online simulation (default)
    #  - a local directory or file glob expression (e.g., "/tmp/*.json")
    #  - a list of individual file paths/URIs (e.g., ["/tmp/1.json",
    #    "s3://bucket/2.json"])
    #  - a dict with string keys and sampling probabilities as values (e.g.,
    #    {"sampler": 0.4, "/tmp/*.json": 0.4, "s3://bucket/expert.json": 0.2}).
    #  - a function that returns a rllib.offline.InputReader
    "input": "sampler",
    # Specify how to evaluate the current policy. This only has an effect when
    # reading offline experiences. Available options:
    #  - "wis": the weighted step-wise importance sampling estimator.
    #  - "is": the step-wise importance sampling estimator.
    #  - "simulation": run the environment in the background, but use
    #    this data for evaluation only and not for learning.
    "input_evaluation": ["is", "wis"],
    # Whether to run postprocess_trajectory() on the trajectory fragments from
    # offline inputs. Note that postprocessing will be done using the *current*
    # policy, not the *behaviour* policy, which is typically undesirable for
    # on-policy algorithms.
    "postprocess_inputs": False,
    # If positive, input batches will be shuffled via a sliding window buffer
    # of this number of batches. Use this if the input data is not in random
    # enough order. Input is delayed until the shuffle buffer is filled.
    "shuffle_buffer_size": 0,
    # Specify where experiences should be saved:
    #  - None: don't save any experiences
    #  - "logdir" to save to the agent log dir
    #  - a path/URI to save to a custom output directory (e.g., "s3://bucket/")
    #  - a function that returns a rllib.offline.OutputWriter
    "output": None,
    # What sample batch columns to LZ4 compress in the output data.
    "output_compress_columns": ["obs", "new_obs"],
    # Max output file size before rolling over to a new file.
    "output_max_file_size": 64 * 1024 * 1024,

    # === Multiagent ===
    "multiagent": {
        # Map from policy ids to tuples of (policy_cls, obs_space,
        # act_space, config). See rollout_worker.py for more info.
        "policies": {},
        # Function mapping agent ids to policy ids.
        "policy_mapping_fn": None,
        # Optional whitelist of policies to train, or None for all policies.
        "policies_to_train": None,
    },
}
# __sphinx_doc_end__
# yapf: enable


@DeveloperAPI
def with_common_config(extra_config):
    """Returns the given config dict merged with common agent confs."""

    return with_base_config(COMMON_CONFIG, extra_config)


def with_base_config(base_config, extra_config):
    """Returns the given config dict merged with a base agent conf."""

    config = copy.deepcopy(base_config)
    config.update(extra_config)
    return config


@PublicAPI
class Trainer(Trainable):
    """A trainer coordinates the optimization of one or more RL policies.

    All RLlib trainers extend this base class, e.g., the A3CTrainer implements
    the A3C algorithm for single and multi-agent training.

    Trainer objects retain internal model state between calls to train(), so
    you should create a new trainer instance for each training session.

    Attributes:
        env_creator (func): Function that creates a new training env.
        config (obj): Algorithm-specific configuration data.
        logdir (str): Directory in which training outputs should be placed.
    """

    _allow_unknown_configs = False
    _allow_unknown_subkeys = [
        "tf_session_args", "env_config", "model", "optimizer", "multiagent",
        "custom_resources_per_worker", "evaluation_config"
    ]

    @PublicAPI
    def __init__(self, config=None, env=None, logger_creator=None):
        """Initialize an RLLib trainer.

        Args:
            config (dict): Algorithm-specific configuration data.
            env (str): Name of the environment to use. Note that this can also
                be specified as the `env` key in config.
            logger_creator (func): Function that creates a ray.tune.Logger
                object. If unspecified, a default logger is created.
        """

        config = config or {}

        # Vars to synchronize to workers on each train call
        self.global_vars = {"timestep": 0}

        # Trainers allow env ids to be passed directly to the constructor.
        self._env_id = self._register_if_needed(env or config.get("env"))

        # Create a default logger creator if no logger_creator is specified
        if logger_creator is None:
            timestr = datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
            logdir_prefix = "{}_{}_{}".format(self._name, self._env_id,
                                              timestr)

            def default_logger_creator(config):
                """Creates a Unified logger with a default logdir prefix
                containing the agent name and the env id
                """
                if not os.path.exists(DEFAULT_RESULTS_DIR):
                    os.makedirs(DEFAULT_RESULTS_DIR)
                logdir = tempfile.mkdtemp(
                    prefix=logdir_prefix, dir=DEFAULT_RESULTS_DIR)
                return UnifiedLogger(config, logdir, None)

            logger_creator = default_logger_creator

        Trainable.__init__(self, config, logger_creator)

    @classmethod
    @override(Trainable)
    def default_resource_request(cls, config):
        cf = dict(cls._default_config, **config)
        Trainer._validate_config(cf)
        # TODO(ekl): add custom resources here once tune supports them
        return Resources(
            cpu=cf["num_cpus_for_driver"],
            gpu=cf["num_gpus"],
            extra_cpu=cf["num_cpus_per_worker"] * cf["num_workers"],
            extra_gpu=cf["num_gpus_per_worker"] * cf["num_workers"])

    @override(Trainable)
    @PublicAPI
    def train(self):
        """Overrides super.train to synchronize global vars."""

        if self._has_policy_optimizer():
            self.global_vars["timestep"] = self.optimizer.num_steps_sampled
            self.optimizer.workers.local_worker().set_global_vars(
                self.global_vars)
            for w in self.optimizer.workers.remote_workers():
                w.set_global_vars.remote(self.global_vars)
            logger.debug("updated global vars: {}".format(self.global_vars))

        result = None
        for _ in range(1 + MAX_WORKER_FAILURE_RETRIES):
            try:
                result = Trainable.train(self)
            except RayError as e:
                if self.config["ignore_worker_failures"]:
                    logger.exception(
                        "Error in train call, attempting to recover")
                    self._try_recover()
                else:
                    logger.info(
                        "Worker crashed during call to train(). To attempt to "
                        "continue training without the failed worker, set "
                        "`'ignore_worker_failures': True`.")
                    raise e
            except Exception as e:
                time.sleep(0.5)  # allow logs messages to propagate
                raise e
            else:
                break
        if result is None:
            raise RuntimeError("Failed to recover from worker crash")

        if (self.config.get("observation_filter", "NoFilter") != "NoFilter"
                and hasattr(self, "workers")
                and isinstance(self.workers, WorkerSet)):
            FilterManager.synchronize(
                self.workers.local_worker().filters,
                self.workers.remote_workers(),
                update_remote=self.config["synchronize_filters"])
            logger.debug("synchronized filters: {}".format(
                self.workers.local_worker().filters))

        if self._has_policy_optimizer():
            result["num_healthy_workers"] = len(
                self.optimizer.workers.remote_workers())

        if self.config["evaluation_interval"]:
            if self._iteration % self.config["evaluation_interval"] == 0:
                evaluation_metrics = self._evaluate()
                assert isinstance(evaluation_metrics, dict), \
                    "_evaluate() needs to return a dict."
                result.update(evaluation_metrics)

        return result

    @override(Trainable)
    def _log_result(self, result):
        if self.config["callbacks"].get("on_train_result"):
            self.config["callbacks"]["on_train_result"]({
                "trainer": self,
                "result": result,
            })
        # log after the callback is invoked, so that the user has a chance
        # to mutate the result
        Trainable._log_result(self, result)

    @override(Trainable)
    def _setup(self, config):
        env = self._env_id
        if env:
            config["env"] = env
            if _global_registry.contains(ENV_CREATOR, env):
                self.env_creator = _global_registry.get(ENV_CREATOR, env)
            else:
                import gym  # soft dependency
                self.env_creator = lambda env_config: gym.make(env)
        else:
            self.env_creator = lambda env_config: None

        # Merge the supplied config with the class default
        merged_config = copy.deepcopy(self._default_config)
        merged_config = deep_update(merged_config, config,
                                    self._allow_unknown_configs,
                                    self._allow_unknown_subkeys)
        self.raw_user_config = config
        self.config = merged_config
        Trainer._validate_config(self.config)
        if self.config.get("log_level"):
            logging.getLogger("ray.rllib").setLevel(self.config["log_level"])

        def get_scope():
            if tf:
                return tf.Graph().as_default()
            else:
                return open("/dev/null")  # fake a no-op scope

        with get_scope():
            self._init(self.config, self.env_creator)

            # Evaluation related
            if self.config.get("evaluation_interval"):
                # Update env_config with evaluation settings:
                extra_config = copy.deepcopy(self.config["evaluation_config"])
                extra_config.update({
                    "batch_mode": "complete_episodes",
                    "batch_steps": 1,
                })
                logger.debug(
                    "using evaluation_config: {}".format(extra_config))
                self.evaluation_workers = self._make_workers(
                    self.env_creator,
                    self._policy,
                    merge_dicts(self.config, extra_config),
                    num_workers=0)
                self.evaluation_metrics = self._evaluate()

    @override(Trainable)
    def _stop(self):
        if hasattr(self, "workers"):
            self.workers.stop()
        if hasattr(self, "optimizer"):
            self.optimizer.stop()

    @override(Trainable)
    def _save(self, checkpoint_dir):
        checkpoint_path = os.path.join(checkpoint_dir,
                                       "checkpoint-{}".format(self.iteration))
        pickle.dump(self.__getstate__(), open(checkpoint_path, "wb"))
        return checkpoint_path

    @override(Trainable)
    def _restore(self, checkpoint_path):
        extra_data = pickle.load(open(checkpoint_path, "rb"))
        self.__setstate__(extra_data)

    @DeveloperAPI
    def _make_workers(self, env_creator, policy, config, num_workers):
        return WorkerSet(
            env_creator,
            policy,
            config,
            num_workers=num_workers,
            logdir=self.logdir)

    @DeveloperAPI
    def _init(self, config, env_creator):
        """Subclasses should override this for custom initialization."""

        raise NotImplementedError

    @DeveloperAPI
    def _evaluate(self):
        """Evaluates current policy under `evaluation_config` settings.

        Note that this default implementation does not do anything beyond
        merging evaluation_config with the normal trainer config.
        """

        if not self.config["evaluation_config"]:
            raise ValueError(
                "No evaluation_config specified. It doesn't make sense "
                "to enable evaluation without specifying any config "
                "overrides, since the results will be the "
                "same as reported during normal policy evaluation.")

        logger.info("Evaluating current policy for {} episodes".format(
            self.config["evaluation_num_episodes"]))
        self._before_evaluate()
        self.evaluation_workers.local_worker().restore(
            self.workers.local_worker().save())
        for _ in range(self.config["evaluation_num_episodes"]):
            self.evaluation_workers.local_worker().sample()

        metrics = collect_metrics(self.evaluation_workers.local_worker())
        return {"evaluation": metrics}

    @DeveloperAPI
    def _before_evaluate(self):
        """Pre-evaluation callback."""
        pass

    @PublicAPI
    def compute_action(self,
                       observation,
                       state=None,
                       prev_action=None,
                       prev_reward=None,
                       info=None,
                       policy_id=DEFAULT_POLICY_ID,
                       full_fetch=False):
        """Computes an action for the specified policy.

        Note that you can also access the policy object through
        self.get_policy(policy_id) and call compute_actions() on it directly.

        Arguments:
            observation (obj): observation from the environment.
            state (list): RNN hidden state, if any. If state is not None,
                          then all of compute_single_action(...) is returned
                          (computed action, rnn state, logits dictionary).
                          Otherwise compute_single_action(...)[0] is
                          returned (computed action).
            prev_action (obj): previous action value, if any
            prev_reward (int): previous reward, if any
            info (dict): info object, if any
            policy_id (str): policy to query (only applies to multi-agent).
            full_fetch (bool): whether to return extra action fetch results.
                This is always set to true if RNN state is specified.

        Returns:
            Just the computed action if full_fetch=False, or the full output
            of policy.compute_actions() otherwise.
        """

        if state is None:
            state = []
        preprocessed = self.workers.local_worker().preprocessors[
            policy_id].transform(observation)
        filtered_obs = self.workers.local_worker().filters[policy_id](
            preprocessed, update=False)
        if state:
            return self.get_policy(policy_id).compute_single_action(
                filtered_obs,
                state,
                prev_action,
                prev_reward,
                info,
                clip_actions=self.config["clip_actions"])
        res = self.get_policy(policy_id).compute_single_action(
            filtered_obs,
            state,
            prev_action,
            prev_reward,
            info,
            clip_actions=self.config["clip_actions"])
        if full_fetch:
            return res
        else:
            return res[0]  # backwards compatibility

    @property
    def _name(self):
        """Subclasses should override this to declare their name."""

        raise NotImplementedError

    @property
    def _default_config(self):
        """Subclasses should override this to declare their default config."""

        raise NotImplementedError

    @PublicAPI
    def get_policy(self, policy_id=DEFAULT_POLICY_ID):
        """Return policy for the specified id, or None.

        Arguments:
            policy_id (str): id of policy to return.
        """

        return self.workers.local_worker().get_policy(policy_id)

    @PublicAPI
    def get_weights(self, policies=None):
        """Return a dictionary of policy ids to weights.

        Arguments:
            policies (list): Optional list of policies to return weights for,
                or None for all policies.
        """
        return self.workers.local_worker().get_weights(policies)

    @PublicAPI
    def set_weights(self, weights):
        """Set policy weights by policy id.

        Arguments:
            weights (dict): Map of policy ids to weights to set.
        """
        self.workers.local_worker().set_weights(weights)

    @DeveloperAPI
    def export_policy_model(self, export_dir, policy_id=DEFAULT_POLICY_ID):
        """Export policy model with given policy_id to local directory.

        Arguments:
            export_dir (string): Writable local directory.
            policy_id (string): Optional policy id to export.

        Example:
            >>> trainer = MyTrainer()
            >>> for _ in range(10):
            >>>     trainer.train()
            >>> trainer.export_policy_model("/tmp/export_dir")
        """
        self.workers.local_worker().export_policy_model(export_dir, policy_id)

    @DeveloperAPI
    def export_policy_checkpoint(self,
                                 export_dir,
                                 filename_prefix="model",
                                 policy_id=DEFAULT_POLICY_ID):
        """Export tensorflow policy model checkpoint to local directory.

        Arguments:
            export_dir (string): Writable local directory.
            filename_prefix (string): file name prefix of checkpoint files.
            policy_id (string): Optional policy id to export.

        Example:
            >>> trainer = MyTrainer()
            >>> for _ in range(10):
            >>>     trainer.train()
            >>> trainer.export_policy_checkpoint("/tmp/export_dir")
        """
        self.workers.local_worker().export_policy_checkpoint(
            export_dir, filename_prefix, policy_id)

    @DeveloperAPI
    def collect_metrics(self, selected_workers=None):
        """Collects metrics from the remote workers of this agent.

        This is the same data as returned by a call to train().
        """
        return self.optimizer.collect_metrics(
            self.config["collect_metrics_timeout"],
            min_history=self.config["metrics_smoothing_episodes"],
            selected_workers=selected_workers)

    @classmethod
    def resource_help(cls, config):
        return ("\n\nYou can adjust the resource requests of RLlib agents by "
                "setting `num_workers`, `num_gpus`, and other configs. See "
                "the DEFAULT_CONFIG defined by each agent for more info.\n\n"
                "The config of this agent is: {}".format(config))

    @staticmethod
    def _validate_config(config):
        if "policy_graphs" in config["multiagent"]:
            logger.warning(
                "The `policy_graphs` config has been renamed to `policies`.")
            # Backwards compatibility
            config["multiagent"]["policies"] = config["multiagent"][
                "policy_graphs"]
            del config["multiagent"]["policy_graphs"]
        if "gpu" in config:
            raise ValueError(
                "The `gpu` config is deprecated, please use `num_gpus=0|1` "
                "instead.")
        if "gpu_fraction" in config:
            raise ValueError(
                "The `gpu_fraction` config is deprecated, please use "
                "`num_gpus=<fraction>` instead.")
        if "use_gpu_for_workers" in config:
            raise ValueError(
                "The `use_gpu_for_workers` config is deprecated, please use "
                "`num_gpus_per_worker=1` instead.")
        if type(config["input_evaluation"]) != list:
            raise ValueError(
                "`input_evaluation` must be a list of strings, got {}".format(
                    config["input_evaluation"]))

    def _try_recover(self):
        """Try to identify and blacklist any unhealthy workers.

        This method is called after an unexpected remote error is encountered
        from a worker. It issues check requests to all current workers and
        blacklists any that respond with error. If no healthy workers remain,
        an error is raised.
        """

        if not self._has_policy_optimizer():
            raise NotImplementedError(
                "Recovery is not supported for this algorithm")

        logger.info("Health checking all workers...")
        checks = []
        for ev in self.optimizer.workers.remote_workers():
            _, obj_id = ev.sample_with_count.remote()
            checks.append(obj_id)

        healthy_workers = []
        for i, obj_id in enumerate(checks):
            w = self.optimizer.workers.remote_workers()[i]
            try:
                ray_get_and_free(obj_id)
                healthy_workers.append(w)
                logger.info("Worker {} looks healthy".format(i + 1))
            except RayError:
                logger.exception("Blacklisting worker {}".format(i + 1))
                try:
                    w.__ray_terminate__.remote()
                except Exception:
                    logger.exception("Error terminating unhealthy worker")

        if len(healthy_workers) < 1:
            raise RuntimeError(
                "Not enough healthy workers remain to continue.")

        self.optimizer.reset(healthy_workers)

    def _has_policy_optimizer(self):
        return hasattr(self, "optimizer") and isinstance(
            self.optimizer, PolicyOptimizer)

    @override(Trainable)
    def _export_model(self, export_formats, export_dir):
        ExportFormat.validate(export_formats)
        exported = {}
        if ExportFormat.CHECKPOINT in export_formats:
            path = os.path.join(export_dir, ExportFormat.CHECKPOINT)
            self.export_policy_checkpoint(path)
            exported[ExportFormat.CHECKPOINT] = path
        if ExportFormat.MODEL in export_formats:
            path = os.path.join(export_dir, ExportFormat.MODEL)
            self.export_policy_model(path)
            exported[ExportFormat.MODEL] = path
        return exported

    def __getstate__(self):
        state = {}
        if hasattr(self, "workers"):
            state["worker"] = self.workers.local_worker().save()
        if hasattr(self, "optimizer") and hasattr(self.optimizer, "save"):
            state["optimizer"] = self.optimizer.save()
        return state

    def __setstate__(self, state):
        if "worker" in state:
            self.workers.local_worker().restore(state["worker"])
            remote_state = ray.put(state["worker"])
            for r in self.workers.remote_workers():
                r.restore.remote(remote_state)
        if "optimizer" in state:
            self.optimizer.restore(state["optimizer"])

    def _register_if_needed(self, env_object):
        if isinstance(env_object, six.string_types):
            return env_object
        elif isinstance(env_object, type):
            name = env_object.__name__
            register_env(name, lambda config: env_object(config))
            return name
        raise ValueError(
            "{} is an invalid env specification. ".format(env_object) +
            "You can specify a custom env as either a class "
            "(e.g., YourEnvCls) or a registered env id (e.g., \"your_env\").")
