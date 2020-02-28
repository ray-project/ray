import copy
import logging
import os
import tempfile
import time
from datetime import datetime

import ray
import tensorflow as tf
from ray.exceptions import RayError
from ray.rllib.evaluation.metrics import collect_metrics
from ray.rllib.optimizers.policy_optimizer import PolicyOptimizer
from ray.rllib.utils import FilterManager, deep_update, merge_dicts
from ray.rllib.utils.annotations import DeveloperAPI, PublicAPI, override
from ray.tune.logger import UnifiedLogger
from ray.tune.registry import ENV_CREATOR, _global_registry
from ray.tune.trainable import Trainable

from ray.rllib.agents import Trainer as UnpatchedTrainer

from ray.rllib.agents.sac.sac.rllib_proxy._added._envs import NormalizeActionWrapper
from ray.rllib.agents.sac.sac.rllib_proxy._constants import DEFAULT_POLICY_ID
from ray.rllib.agents.sac.sac.rllib_proxy._moved import Resources
from ray.rllib.agents.sac.sac.rllib_proxy._unchanged import MAX_WORKER_FAILURE_RETRIES, DEFAULT_RESULTS_DIR
from ray.rllib.agents.sac.sac.dev_utils import using_ray_8, ray_8_only

logger = logging.getLogger(__name__)


@PublicAPI
class Trainer(UnpatchedTrainer):
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
    if using_ray_8():
        _allow_unknown_subkeys = [
            "tf_session_args", "local_tf_session_args", "env_config", "model",
            "optimizer", "multiagent", "custom_resources_per_worker",
            "evaluation_config"
        ]
    else:
        _allow_unknown_subkeys = [
            "tf_session_args", "env_config", "model", "optimizer", "multiagent",
            "custom_resources_per_worker"
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

        if using_ray_8():
            if tf and config.get("eager"):
                tf.enable_eager_execution()
                logger.info("Executing eagerly, with eager_tracing={}".format(
                    "True" if config.get("eager_tracing") else "False"))

            if tf and not tf.executing_eagerly():
                logger.info("Tip: set 'eager': true or the --eager flag to enable "
                            "TensorFlow eager execution")
        else:
            self.local_evaluator = None
            self.remote_evaluators = []

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
                if using_ray_8():
                    return UnifiedLogger(config, logdir, loggers=None)
                else:
                    return UnifiedLogger(config, logdir, None)

            logger_creator = default_logger_creator

        Trainable.__init__(self, config, logger_creator)

    @classmethod
    @override(Trainable)
    def default_resource_request(cls, config):
        cf = dict(cls._default_config, **config)
        Trainer._validate_config(cf)
        # TODO(ekl): add custom resources here once tune supports them
        if using_ray_8():
            return Resources(
                cpu=cf["num_cpus_for_driver"],
                gpu=cf["num_gpus"],
                memory=cf["memory"],
                object_store_memory=cf["object_store_memory"],
                extra_cpu=cf["num_cpus_per_worker"] * cf["num_workers"],
                extra_gpu=cf["num_gpus_per_worker"] * cf["num_workers"],
                extra_memory=cf["memory_per_worker"] * cf["num_workers"],
                extra_object_store_memory=cf["object_store_memory_per_worker"] *
                                          cf["num_workers"])
        else:
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
            if using_ray_8():
                self.optimizer.workers.local_worker().set_global_vars(
                    self.global_vars)
                for w in self.optimizer.workers.remote_workers():
                    w.set_global_vars.remote(self.global_vars)
            else:
                self.optimizer.local_evaluator.set_global_vars(self.global_vars)
                for ev in self.optimizer.remote_evaluators:
                    ev.set_global_vars.remote(self.global_vars)
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
            and hasattr(self, "local_evaluator")):
            FilterManager.synchronize(
                self.local_evaluator.filters,
                self.remote_evaluators,
                update_remote=self.config["synchronize_filters"])
            logger.debug("synchronized filters: {}".format(
                self.local_evaluator.filters))

        if using_ray_8():
            if self._has_policy_optimizer():
                result["num_healthy_workers"] = len(
                    self.optimizer.workers.remote_workers())
        else:
            if self._has_policy_optimizer():
                result["num_healthy_workers"] = len(
                    self.optimizer.remote_evaluators)

        if using_ray_8():
            if self.config["evaluation_interval"]:
                if self._iteration % self.config["evaluation_interval"] == 0:
                    evaluation_metrics = self._evaluate()
                    assert isinstance(evaluation_metrics, dict), \
                        "_evaluate() needs to return a dict."
                    result.update(evaluation_metrics)

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
        self.raw_user_config = config
        self.config = merged_config
        if self.config["normalize_actions"]:
            inner = self.env_creator
            self.env_creator = (
                lambda env_config: NormalizeActionWrapper(inner(env_config)))

        Trainer._validate_config(self.config)
        if using_ray_8():
            log_level = self.config.get("log_level")
            if log_level in ["WARN", "ERROR"]:
                logger.info("Current log_level is {}. For more information, "
                            "set 'log_level': 'INFO' / 'DEBUG' or use the -v and "
                            "-vv flags.".format(log_level))
        if self.config.get("log_level"):
            logging.getLogger("ray.rllib").setLevel(self.config["log_level"])

        def get_scope():
            if tf and not tf.executing_eagerly():
                return tf.Graph().as_default()
            else:
                return open("/dev/null")  # fake a no-op scope

        with get_scope():
            self._init(self.config, self.env_creator)
            if using_ray_8():
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
        if using_ray_8():
            if hasattr(self, "workers"):
                self.workers.stop()
        else:
            # Call stop on all evaluators to release resources
            if hasattr(self, "local_evaluator"):
                self.local_evaluator.stop()
            if hasattr(self, "remote_evaluators"):
                for ev in self.remote_evaluators:
                    ev.stop.remote()

            # workaround for https://github.com/ray-project/ray/issues/1516
            if hasattr(self, "remote_evaluators"):
                for ev in self.remote_evaluators:
                    ev.__ray_terminate__.remote()

        if hasattr(self, "optimizer"):
            self.optimizer.stop()

    @DeveloperAPI
    def _make_workers(self, env_creator, policy, config, num_workers):
        config.setdefault(
            "local_evaluator_tf_session_args",
            config.get("local_tf_session_args")
        )
        self.config["multiagent"].setdefault(
            # PolicyGraph has been renamed to Policy in v 0.8.1
            "policy_graphs",
            self.config["multiagent"].get("policies")
        )
        self.local_evaluator = self.make_local_evaluator(env_creator, policy, config)
        self.remote_evaluators = self.make_remote_evaluators(env_creator, policy, num_workers)
        return self.local_evaluator, self.remote_evaluators

    @ray_8_only
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

    @ray_8_only
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
        if using_ray_8():
            preprocessed = self.workers.local_worker().preprocessors[
                policy_id].transform(observation)
            filtered_obs = self.workers.local_worker().filters[policy_id](
                preprocessed, update=False)
        else:
            preprocessed = self.local_evaluator.preprocessors[policy_id].transform(
                observation)
            filtered_obs = self.local_evaluator.filters[policy_id](
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
        if using_ray_8():
            return self.workers.local_worker().get_policy(policy_id)
        else:
            return self.local_evaluator.get_policy(policy_id)

    @PublicAPI
    def get_weights(self, policies=None):
        """Return a dictionary of policy ids to weights.

        Arguments:
            policies (list): Optional list of policies to return weights for,
                or None for all policies.
        """
        if using_ray_8():
            return self.workers.local_worker().get_weights(policies)
        else:
            return self.local_evaluator.get_weights(policies)

    @PublicAPI
    def set_weights(self, weights):
        """Set policy weights by policy id.

        Arguments:
            weights (dict): Map of policy ids to weights to set.
        """
        if using_ray_8():
            self.workers.local_worker().set_weights(weights)
        else:
            self.local_evaluator.set_weights(weights)

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
        if using_ray_8():
            self.workers.local_worker().export_policy_model(export_dir, policy_id)
        else:
            self.local_evaluator.export_policy_model(export_dir, policy_id)

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
        if using_ray_8():
            self.workers.local_worker().export_policy_checkpoint(
                export_dir, filename_prefix, policy_id)
        else:
            self.local_evaluator.export_policy_checkpoint(
                export_dir, filename_prefix, policy_id)

    @DeveloperAPI
    def collect_metrics(self, selected_workers=None):
        """Collects metrics from the remote workers of this agent.

        This is the same data as returned by a call to train().
        """
        if using_ray_8():
            return self.optimizer.collect_metrics(
                self.config["collect_metrics_timeout"],
                min_history=self.config["metrics_smoothing_episodes"],
                selected_workers=selected_workers)
        else:
            return self.optimizer.collect_metrics(
                self.config["collect_metrics_timeout"],
                min_history=self.config["metrics_smoothing_episodes"],
                selected_evaluators=selected_workers)

    @classmethod
    def resource_help(cls, config):
        return ("\n\nYou can adjust the resource requests of RLlib agents by "
                "setting `num_workers`, `num_gpus`, and other configs. See "
                "the DEFAULT_CONFIG defined by each agent for more info.\n\n"
                "The config of this agent is: {}".format(config))

    @staticmethod
    def _validate_config(config):
        if using_ray_8():
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
        if using_ray_8():
            for ev in self.optimizer.workers.remote_workers():
                _, obj_id = ev.sample_with_count.remote()
                checks.append(obj_id)
        else:
            for ev in self.optimizer.remote_evaluators:
                _, obj_id = ev.sample_with_count.remote()
                checks.append(obj_id)

        healthy_workers = []
        for i, obj_id in enumerate(checks):
            if using_ray_8():
                w = self.optimizer.workers.remote_workers()[i]
            else:
                ev = self.optimizer.remote_evaluators[i]
                w = ev
            try:
                ray.get(obj_id)
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

    def __getstate__(self):
        state = {}
        if using_ray_8():
            if hasattr(self, "workers"):
                state["worker"] = self.workers.local_worker().save()
            if hasattr(self, "optimizer") and hasattr(self.optimizer, "save"):
                state["optimizer"] = self.optimizer.save()
        else:
            if hasattr(self, "local_evaluator"):
                state["evaluator"] = self.local_evaluator.save()
            if hasattr(self, "optimizer") and hasattr(self.optimizer, "save"):
                state["optimizer"] = self.optimizer.save()
        return state

    def __setstate__(self, state):
        if using_ray_8():
            if "worker" in state:
                self.workers.local_worker().restore(state["worker"])
                remote_state = ray.put(state["worker"])
                for r in self.workers.remote_workers():
                    r.restore.remote(remote_state)
            if "optimizer" in state:
                self.optimizer.restore(state["optimizer"])
        else:
            if "evaluator" in state:
                self.local_evaluator.restore(state["evaluator"])
                remote_state = ray.put(state["evaluator"])
                for r in self.remote_evaluators:
                    r.restore.remote(remote_state)
            if "optimizer" in state:
                self.optimizer.restore(state["optimizer"])

    @DeveloperAPI
    def make_local_evaluator(self,
                             env_creator,
                             policy_graph,
                             extra_config=None):
        """Convenience method to return configured local evaluator."""
        from ray.rllib.agents.sac.sac.rllib_proxy._patched._policy_evaluator import PolicyEvaluator

        return self._make_evaluator(
            PolicyEvaluator,
            env_creator,
            policy_graph,
            0,
            merge_dicts(
                # important: allow local tf to use more CPUs for optimization
                merge_dicts(
                    self.config, {
                        "tf_session_args": self.
                            config["local_evaluator_tf_session_args"]
                    }),
                extra_config or {}))

    @DeveloperAPI
    def make_remote_evaluators(self, env_creator, policy_graph, count):
        """Convenience method to return a number of remote evaluators."""
        from ray.rllib.agents.sac.sac.rllib_proxy._patched._policy_evaluator import PolicyEvaluator

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

    def _make_evaluator(self, cls, env_creator, policy_graph, worker_index,
                        config):
        from types import FunctionType
        from ray.rllib.offline import NoopOutput, JsonReader, MixedInput, JsonWriter, \
            ShuffledInput
        from ray.rllib.agents.sac.sac.rllib_proxy._patched._policy_evaluator import _validate_multiagent_config
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

        # Fill in the default policy graph if 'None' is specified in multiagent
        if self.config["multiagent"]["policy_graphs"]:
            tmp = self.config["multiagent"]["policy_graphs"]
            _validate_multiagent_config(tmp, allow_none_graph=True)
            for k, v in tmp.items():
                if v[0] is None:
                    tmp[k] = (policy_graph, v[1], v[2], v[3])
            policy_graph = tmp

        return cls(
            env_creator,
            policy_graph,
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
            input_evaluation=input_evaluation,
            output_creator=output_creator,
            remote_worker_envs=config["remote_worker_envs"],
            remote_env_batch_wait_ms=config["remote_env_batch_wait_ms"],
            soft_horizon=config["soft_horizon"],
            no_done_at_end=config["no_done_at_end"],
            _fake_sampler=config.get("_fake_sampler", False))

