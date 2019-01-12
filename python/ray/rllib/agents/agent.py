from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from datetime import datetime
import copy
import logging
import os
import pickle
import six
import tempfile
import tensorflow as tf
from types import FunctionType

import ray
from ray.rllib.offline import NoopOutput, JsonReader, MixedInput, JsonWriter
from ray.rllib.models import MODEL_DEFAULTS
from ray.rllib.evaluation.policy_evaluator import PolicyEvaluator
from ray.rllib.evaluation.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.optimizers.policy_optimizer import PolicyOptimizer
from ray.rllib.utils.annotations import override
from ray.rllib.utils import FilterManager, deep_update, merge_dicts
from ray.tune.registry import ENV_CREATOR, register_env, _global_registry
from ray.tune.trainable import Trainable
from ray.tune.trial import Resources
from ray.tune.logger import UnifiedLogger
from ray.tune.result import DEFAULT_RESULTS_DIR

logger = logging.getLogger(__name__)

# yapf: disable
# __sphinx_doc_begin__
COMMON_CONFIG = {
    # === Debugging ===
    # Whether to write episode stats and videos to the agent log dir
    "monitor": False,
    # Set the ray.rllib.* log level for the agent process and its evaluators
    "log_level": "INFO",
    # Callbacks that will be run during various phases of training. These all
    # take a single "info" dict as an argument. For episode callbacks, custom
    # metrics can be attached to the episode by updating the episode object's
    # custom metrics dict (see examples/custom_metrics_and_callbacks.py).
    "callbacks": {
        "on_episode_start": None,  # arg: {"env": .., "episode": ...}
        "on_episode_step": None,   # arg: {"env": .., "episode": ...}
        "on_episode_end": None,    # arg: {"env": .., "episode": ...}
        "on_sample_end": None,     # arg: {"samples": .., "evaluator": ...}
        "on_train_result": None,   # arg: {"agent": ..., "result": ...}
    },

    # === Policy ===
    # Arguments to pass to model. See models/catalog.py for a full list of the
    # available model options.
    "model": MODEL_DEFAULTS,
    # Arguments to pass to the policy optimizer. These vary by optimizer.
    "optimizer": {},

    # === Environment ===
    # Discount factor of the MDP
    "gamma": 0.99,
    # Number of steps after which the episode is forced to terminate
    "horizon": None,
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
    # Default sample batch size
    "sample_batch_size": 200,
    # Training batch size, if applicable. Should be >= sample_batch_size.
    # Samples batches will be concatenated together to this size for training.
    "train_batch_size": 200,
    # Whether to rollout "complete_episodes" or "truncate_episodes"
    "batch_mode": "truncate_episodes",
    # Whether to use a background thread for sampling (slightly off-policy)
    "sample_async": False,
    # Element-wise observation filter, either "NoFilter" or "MeanStdFilter"
    "observation_filter": "NoFilter",
    # Whether to synchronize the statistics of remote filters.
    "synchronize_filters": True,
    # Configure TF for single-process operation by default
    "tf_session_args": {
        # note: overriden by `local_evaluator_tf_session_args`
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
    # Override the following tf session args on the local evaluator
    "local_evaluator_tf_session_args": {
        # Allow a higher level of parallelism by default, but not unlimited
        # since that can cause crashes with many concurrent drivers.
        "intra_op_parallelism_threads": 8,
        "inter_op_parallelism_threads": 8,
    },
    # Whether to LZ4 compress individual observations
    "compress_observations": False,
    # Drop metric batches from unresponsive workers after this many seconds
    "collect_metrics_timeout": 180,

    # === Offline Data Input / Output ===
    # __sphinx_doc_input_begin__
    # Specify how to generate experiences:
    #  - "sampler": generate experiences via online simulation (default)
    #  - a local directory or file glob expression (e.g., "/tmp/*.json")
    #  - a list of individual file paths/URIs (e.g., ["/tmp/1.json",
    #    "s3://bucket/2.json"])
    #  - a dict with string keys and sampling probabilities as values (e.g.,
    #    {"sampler": 0.4, "/tmp/*.json": 0.4, "s3://bucket/expert.json": 0.2}).
    #  - a function that returns a rllib.offline.InputReader
    "input": "sampler",
    # Specify how to evaluate the current policy. This only makes sense to set
    # when the input is not already generating simulation data:
    #  - None: don't evaluate the policy. The episode reward and other
    #    metrics will be NaN if using offline data.
    #  - "simulation": run the environment in the background, but use
    #    this data for evaluation only and not for learning.
    "input_evaluation": None,
    # Whether to run postprocess_trajectory() on the trajectory fragments from
    # offline inputs. Note that postprocessing will be done using the *current*
    # policy, not the *behaviour* policy, which is typically undesirable for
    # on-policy algorithms.
    "postprocess_inputs": False,
    # __sphinx_doc_input_end__
    # __sphinx_doc_output_begin__
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
    # __sphinx_doc_output_end__

    # === Multiagent ===
    "multiagent": {
        # Map from policy ids to tuples of (policy_graph_cls, obs_space,
        # act_space, config). See policy_evaluator.py for more info.
        "policy_graphs": {},
        # Function mapping agent ids to policy ids.
        "policy_mapping_fn": None,
        # Optional whitelist of policies to train, or None for all policies.
        "policies_to_train": None,
    },
}
# __sphinx_doc_end__
# yapf: enable


def with_common_config(extra_config):
    """Returns the given config dict merged with common agent confs."""

    config = copy.deepcopy(COMMON_CONFIG)
    config.update(extra_config)
    return config


class Agent(Trainable):
    """All RLlib agents extend this base class.

    Agent objects retain internal model state between calls to train(), so
    you should create a new agent instance for each training session.

    Attributes:
        env_creator (func): Function that creates a new training env.
        config (obj): Algorithm-specific configuration data.
        logdir (str): Directory in which training outputs should be placed.
    """

    _allow_unknown_configs = False
    _allow_unknown_subkeys = [
        "tf_session_args", "env_config", "model", "optimizer", "multiagent",
        "custom_resources_per_worker"
    ]

    def __init__(self, config=None, env=None, logger_creator=None):
        """Initialize an RLLib agent.

        Args:
            config (dict): Algorithm-specific configuration data.
            env (str): Name of the environment to use. Note that this can also
                be specified as the `env` key in config.
            logger_creator (func): Function that creates a ray.tune.Logger
                object. If unspecified, a default logger is created.
        """

        config = config or {}

        # Vars to synchronize to evaluators on each train call
        self.global_vars = {"timestep": 0}

        # Agents allow env ids to be passed directly to the constructor.
        self._env_id = _register_if_needed(env or config.get("env"))

        # Create a default logger creator if no logger_creator is specified
        if logger_creator is None:
            timestr = datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
            logdir_prefix = "{}_{}_{}".format(self._agent_name, self._env_id,
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
        Agent._validate_config(cf)
        # TODO(ekl): add custom resources here once tune supports them
        return Resources(
            cpu=cf["num_cpus_for_driver"],
            gpu=cf["num_gpus"],
            extra_cpu=cf["num_cpus_per_worker"] * cf["num_workers"],
            extra_gpu=cf["num_gpus_per_worker"] * cf["num_workers"])

    @override(Trainable)
    def train(self):
        """Overrides super.train to synchronize global vars."""

        if hasattr(self, "optimizer") and isinstance(self.optimizer,
                                                     PolicyOptimizer):
            self.global_vars["timestep"] = self.optimizer.num_steps_sampled
            self.optimizer.local_evaluator.set_global_vars(self.global_vars)
            for ev in self.optimizer.remote_evaluators:
                ev.set_global_vars.remote(self.global_vars)
            logger.debug("updated global vars: {}".format(self.global_vars))

        if (self.config.get("observation_filter", "NoFilter") != "NoFilter"
                and hasattr(self, "local_evaluator")):
            FilterManager.synchronize(
                self.local_evaluator.filters,
                self.remote_evaluators,
                update_remote=self.config["synchronize_filters"])
            logger.debug("synchronized filters: {}".format(
                self.local_evaluator.filters))

        result = Trainable.train(self)
        if self.config["callbacks"].get("on_train_result"):
            self.config["callbacks"]["on_train_result"]({
                "agent": self,
                "result": result,
            })
        return result

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
        self.config = merged_config
        Agent._validate_config(self.config)
        if self.config.get("log_level"):
            logging.getLogger("ray.rllib").setLevel(self.config["log_level"])

        # TODO(ekl) setting the graph is unnecessary for PyTorch agents
        with tf.Graph().as_default():
            self._init()

    @override(Trainable)
    def _stop(self):
        # workaround for https://github.com/ray-project/ray/issues/1516
        if hasattr(self, "remote_evaluators"):
            for ev in self.remote_evaluators:
                ev.__ray_terminate__.remote()
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

    def _init(self):
        """Subclasses should override this for custom initialization."""

        raise NotImplementedError

    def compute_action(self,
                       observation,
                       state=None,
                       prev_action=None,
                       prev_reward=None,
                       info=None,
                       policy_id="default"):
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
        """

        if state is None:
            state = []
        preprocessed = self.local_evaluator.preprocessors[policy_id].transform(
            observation)
        filtered_obs = self.local_evaluator.filters[policy_id](
            preprocessed, update=False)
        if state:
            return self.get_policy(policy_id).compute_single_action(
                filtered_obs, state, prev_action, prev_reward, info)
        return self.get_policy(policy_id).compute_single_action(
            filtered_obs, state, prev_action, prev_reward, info)[0]

    @property
    def iteration(self):
        """Current training iter, auto-incremented with each train() call."""

        return self._iteration

    @property
    def _agent_name(self):
        """Subclasses should override this to declare their name."""

        raise NotImplementedError

    @property
    def _default_config(self):
        """Subclasses should override this to declare their default config."""

        raise NotImplementedError

    def get_policy(self, policy_id=DEFAULT_POLICY_ID):
        """Return policy graph for the specified id, or None.

        Arguments:
            policy_id (str): id of policy graph to return.
        """

        return self.local_evaluator.get_policy(policy_id)

    def get_weights(self, policies=None):
        """Return a dictionary of policy ids to weights.

        Arguments:
            policies (list): Optional list of policies to return weights for,
                or None for all policies.
        """
        return self.local_evaluator.get_weights(policies)

    def set_weights(self, weights):
        """Set policy weights by policy id.

        Arguments:
            weights (dict): Map of policy ids to weights to set.
        """
        self.local_evaluator.set_weights(weights)

    def make_local_evaluator(self, env_creator, policy_graph):
        """Convenience method to return configured local evaluator."""

        return self._make_evaluator(
            PolicyEvaluator,
            env_creator,
            policy_graph,
            0,
            # important: allow local tf to use more CPUs for optimization
            merge_dicts(self.config, {
                "tf_session_args": self.
                config["local_evaluator_tf_session_args"]
            }))

    def make_remote_evaluators(self, env_creator, policy_graph, count):
        """Convenience method to return a number of remote evaluators."""

        remote_args = {
            "num_cpus": self.config["num_cpus_per_worker"],
            "num_gpus": self.config["num_gpus_per_worker"],
            "resources": self.config["custom_resources_per_worker"],
        }

        cls = PolicyEvaluator.as_remote(**remote_args).remote
        return [
            self._make_evaluator(cls, env_creator, policy_graph, i + 1,
                                 self.config) for i in range(count)
        ]

    def export_policy_model(self, export_dir, policy_id=DEFAULT_POLICY_ID):
        """Export policy model with given policy_id to local directory.

        Arguments:
            export_dir (string): Writable local directory.
            policy_id (string): Optional policy id to export.

        Example:
            >>> agent = MyAgent()
            >>> for _ in range(10):
            >>>     agent.train()
            >>> agent.export_policy_model("/tmp/export_dir")
        """
        self.local_evaluator.export_policy_model(export_dir, policy_id)

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
            >>> agent = MyAgent()
            >>> for _ in range(10):
            >>>     agent.train()
            >>> agent.export_policy_checkpoint("/tmp/export_dir")
        """
        self.local_evaluator.export_policy_checkpoint(
            export_dir, filename_prefix, policy_id)

    @classmethod
    def resource_help(cls, config):
        return ("\n\nYou can adjust the resource requests of RLlib agents by "
                "setting `num_workers` and other configs. See the "
                "DEFAULT_CONFIG defined by each agent for more info.\n\n"
                "The config of this agent is: {}".format(config))

    @staticmethod
    def _validate_config(config):
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
        if (config["input"] == "sampler"
                and config["input_evaluation"] is not None):
            raise ValueError(
                "`input_evaluation` should not be set when input=sampler")

    def _make_evaluator(self, cls, env_creator, policy_graph, worker_index,
                        config):
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
            input_creator = (lambda ioctx: MixedInput(config["input"], ioctx))
        else:
            input_creator = (lambda ioctx: JsonReader(config["input"], ioctx))

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

        return cls(
            env_creator,
            self.config["multiagent"]["policy_graphs"] or policy_graph,
            policy_mapping_fn=self.config["multiagent"]["policy_mapping_fn"],
            policies_to_train=self.config["multiagent"]["policies_to_train"],
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
            monitor_path=self.logdir if config["monitor"] else None,
            log_dir=self.logdir,
            log_level=config["log_level"],
            callbacks=config["callbacks"],
            input_creator=input_creator,
            input_evaluation_method=config["input_evaluation"],
            output_creator=output_creator)

    def __getstate__(self):
        state = {}
        if hasattr(self, "local_evaluator"):
            state["evaluator"] = self.local_evaluator.save()
        if hasattr(self, "optimizer") and hasattr(self.optimizer, "save"):
            state["optimizer"] = self.optimizer.save()
        return state

    def __setstate__(self, state):
        if "evaluator" in state:
            self.local_evaluator.restore(state["evaluator"])
            remote_state = ray.put(state["evaluator"])
            for r in self.remote_evaluators:
                r.restore.remote(remote_state)
        if "optimizer" in state:
            self.optimizer.restore(state["optimizer"])


def _register_if_needed(env_object):
    if isinstance(env_object, six.string_types):
        return env_object
    elif isinstance(env_object, type):
        name = env_object.__name__
        register_env(name, lambda config: env_object(config))
        return name
