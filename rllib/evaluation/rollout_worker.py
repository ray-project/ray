import copy
import importlib.util
import logging
import os
import platform
import threading
from collections import defaultdict
from types import FunctionType
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Collection,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
)

from gymnasium.spaces import Space

import ray
from ray import ObjectRef
from ray import cloudpickle as pickle
from ray.rllib.connectors.util import (
    create_connectors_for_policy,
    maybe_get_filters_for_syncing,
)
from ray.rllib.core.rl_module import validate_module_id
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.env.base_env import BaseEnv, convert_to_base_env
from ray.rllib.env.env_context import EnvContext
from ray.rllib.env.env_runner import EnvRunner
from ray.rllib.env.external_multi_agent_env import ExternalMultiAgentEnv
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.env.wrappers.atari_wrappers import is_atari, wrap_deepmind
from ray.rllib.evaluation.metrics import RolloutMetrics
from ray.rllib.evaluation.sampler import SyncSampler
from ray.rllib.models import ModelCatalog
from ray.rllib.models.preprocessors import Preprocessor
from ray.rllib.offline import (
    D4RLReader,
    DatasetReader,
    DatasetWriter,
    InputReader,
    IOContext,
    JsonReader,
    JsonWriter,
    MixedInput,
    NoopOutput,
    OutputWriter,
    ShuffledInput,
)
from ray.rllib.policy.policy import Policy, PolicySpec
from ray.rllib.policy.policy_map import PolicyMap
from ray.rllib.policy.sample_batch import (
    DEFAULT_POLICY_ID,
    MultiAgentBatch,
    concat_samples,
    convert_ma_batch_to_sample_batch,
)
from ray.rllib.policy.torch_policy import TorchPolicy
from ray.rllib.policy.torch_policy_v2 import TorchPolicyV2
from ray.rllib.utils import force_list
from ray.rllib.utils.annotations import OldAPIStack, override
from ray.rllib.utils.debug import summarize, update_global_seed_if_necessary
from ray.rllib.utils.error import ERR_MSG_NO_GPUS, HOWTO_CHANGE_CONFIG
from ray.rllib.utils.filter import Filter, NoFilter
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.from_config import from_config
from ray.rllib.utils.policy import create_policy_for_framework
from ray.rllib.utils.sgd import do_minibatch_sgd
from ray.rllib.utils.tf_run_builder import _TFRunBuilder
from ray.rllib.utils.tf_utils import get_gpu_devices as get_tf_gpu_devices
from ray.rllib.utils.tf_utils import get_tf_eager_cls_if_necessary
from ray.rllib.utils.typing import (
    AgentID,
    EnvCreator,
    EnvType,
    ModelGradients,
    ModelWeights,
    MultiAgentPolicyConfigDict,
    PartialAlgorithmConfigDict,
    PolicyID,
    PolicyState,
    SampleBatchType,
    T,
)
from ray.tune.registry import registry_contains_input, registry_get_input
from ray.util.annotations import PublicAPI
from ray.util.debug import disable_log_once_globally, enable_periodic_logging, log_once
from ray.util.iter import ParallelIteratorWorker

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
    from ray.rllib.callbacks.callbacks import RLlibCallback

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()

logger = logging.getLogger(__name__)

# Handle to the current rollout worker, which will be set to the most recently
# created RolloutWorker in this process. This can be helpful to access in
# custom env or policy classes for debugging or advanced use cases.
_global_worker: Optional["RolloutWorker"] = None


@OldAPIStack
def get_global_worker() -> "RolloutWorker":
    """Returns a handle to the active rollout worker in this process."""

    global _global_worker
    return _global_worker


def _update_env_seed_if_necessary(
    env: EnvType, seed: int, worker_idx: int, vector_idx: int
):
    """Set a deterministic random seed on environment.

    NOTE: this may not work with remote environments (issue #18154).
    """
    if seed is None:
        return

    # A single RL job is unlikely to have more than 10K
    # rollout workers.
    max_num_envs_per_env_runner: int = 1000
    assert (
        worker_idx < max_num_envs_per_env_runner
    ), "Too many envs per worker. Random seeds may collide."
    computed_seed: int = worker_idx * max_num_envs_per_env_runner + vector_idx + seed

    # Gymnasium.env.
    # This will silently fail for most Farama-foundation gymnasium environments.
    # (they do nothing and return None per default)
    if not hasattr(env, "reset"):
        if log_once("env_has_no_reset_method"):
            logger.info(f"Env {env} doesn't have a `reset()` method. Cannot seed.")
    else:
        try:
            env.reset(seed=computed_seed)
        except Exception:
            logger.info(
                f"Env {env} doesn't support setting a seed via its `reset()` "
                "method! Implement this method as `reset(self, *, seed=None, "
                "options=None)` for it to abide to the correct API. Cannot seed."
            )


@OldAPIStack
class RolloutWorker(ParallelIteratorWorker, EnvRunner):
    """Common experience collection class.

    This class wraps a policy instance and an environment class to
    collect experiences from the environment. You can create many replicas of
    this class as Ray actors to scale RL training.

    This class supports vectorized and multi-agent policy evaluation (e.g.,
    VectorEnv, MultiAgentEnv, etc.)

    .. testcode::
        :skipif: True

        # Create a rollout worker and using it to collect experiences.
        import gymnasium as gym
        from ray.rllib.evaluation.rollout_worker import RolloutWorker
        from ray.rllib.algorithms.ppo.ppo_tf_policy import PPOTF1Policy
        worker = RolloutWorker(
          env_creator=lambda _: gym.make("CartPole-v1"),
          default_policy_class=PPOTF1Policy)
        print(worker.sample())

        # Creating a multi-agent rollout worker
        from gymnasium.spaces import Discrete, Box
        import random
        MultiAgentTrafficGrid = ...
        worker = RolloutWorker(
          env_creator=lambda _: MultiAgentTrafficGrid(num_cars=25),
          config=AlgorithmConfig().multi_agent(
            policies={
              # Use an ensemble of two policies for car agents
              "car_policy1":
                (PGTFPolicy, Box(...), Discrete(...),
                 AlgorithmConfig.overrides(gamma=0.99)),
              "car_policy2":
                (PGTFPolicy, Box(...), Discrete(...),
                 AlgorithmConfig.overrides(gamma=0.95)),
              # Use a single shared policy for all traffic lights
              "traffic_light_policy":
                (PGTFPolicy, Box(...), Discrete(...), {}),
            },
            policy_mapping_fn=(
              lambda agent_id, episode, **kwargs:
              random.choice(["car_policy1", "car_policy2"])
              if agent_id.startswith("car_") else "traffic_light_policy"),
            ),
        )
        print(worker.sample())

    .. testoutput::

        SampleBatch({
            "obs": [[...]], "actions": [[...]], "rewards": [[...]],
            "terminateds": [[...]], "truncateds": [[...]], "new_obs": [[...]]}
        )

        MultiAgentBatch({
            "car_policy1": SampleBatch(...),
            "car_policy2": SampleBatch(...),
            "traffic_light_policy": SampleBatch(...)}
        )

    """

    def __init__(
        self,
        *,
        env_creator: EnvCreator,
        validate_env: Optional[Callable[[EnvType, EnvContext], None]] = None,
        config: Optional["AlgorithmConfig"] = None,
        worker_index: int = 0,
        num_workers: Optional[int] = None,
        recreated_worker: bool = False,
        log_dir: Optional[str] = None,
        spaces: Optional[Dict[PolicyID, Tuple[Space, Space]]] = None,
        default_policy_class: Optional[Type[Policy]] = None,
        dataset_shards: Optional[List[ray.data.Dataset]] = None,
        **kwargs,
    ):
        """Initializes a RolloutWorker instance.

        Args:
            env_creator: Function that returns a gym.Env given an EnvContext
                wrapped configuration.
            validate_env: Optional callable to validate the generated
                environment (only on worker=0).
            worker_index: For remote workers, this should be set to a
                non-zero and unique value. This index is passed to created envs
                through EnvContext so that envs can be configured per worker.
            recreated_worker: Whether this worker is a recreated one. Workers are
                recreated by an Algorithm (via EnvRunnerGroup) in case
                `restart_failed_env_runners=True` and one of the original workers (or
                an already recreated one) has failed. They don't differ from original
                workers other than the value of this flag (`self.recreated_worker`).
            log_dir: Directory where logs can be placed.
            spaces: An optional space dict mapping policy IDs
                to (obs_space, action_space)-tuples. This is used in case no
                Env is created on this RolloutWorker.
        """
        self._original_kwargs: dict = locals().copy()
        del self._original_kwargs["self"]

        global _global_worker
        _global_worker = self

        from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

        # Default config needed?
        if config is None or isinstance(config, dict):
            config = AlgorithmConfig().update_from_dict(config or {})
        # Freeze config, so no one else can alter it from here on.
        config.freeze()

        # Set extra python env variables before calling super constructor.
        if config.extra_python_environs_for_driver and worker_index == 0:
            for key, value in config.extra_python_environs_for_driver.items():
                os.environ[key] = str(value)
        elif config.extra_python_environs_for_worker and worker_index > 0:
            for key, value in config.extra_python_environs_for_worker.items():
                os.environ[key] = str(value)

        def gen_rollouts():
            while True:
                yield self.sample()

        ParallelIteratorWorker.__init__(self, gen_rollouts, False)
        EnvRunner.__init__(self, config=config)

        self.num_workers = (
            num_workers if num_workers is not None else self.config.num_env_runners
        )
        # In case we are reading from distributed datasets, store the shards here
        # and pick our shard by our worker-index.
        self._ds_shards = dataset_shards
        self.worker_index: int = worker_index

        # Lock to be able to lock this entire worker
        # (via `self.lock()` and `self.unlock()`).
        # This might be crucial to prevent a race condition in case
        # `config.policy_states_are_swappable=True` and you are using an Algorithm
        # with a learner thread. In this case, the thread might update a policy
        # that is being swapped (during the update) by the Algorithm's
        # training_step's `RolloutWorker.get_weights()` call (to sync back the
        # new weights to all remote workers).
        self._lock = threading.Lock()

        if (
            tf1
            and (config.framework_str == "tf2" or config.enable_tf1_exec_eagerly)
            # This eager check is necessary for certain all-framework tests
            # that use tf's eager_mode() context generator.
            and not tf1.executing_eagerly()
        ):
            tf1.enable_eager_execution()

        if self.config.log_level:
            logging.getLogger("ray.rllib").setLevel(self.config.log_level)

        if self.worker_index > 1:
            disable_log_once_globally()  # only need 1 worker to log
        elif self.config.log_level == "DEBUG":
            enable_periodic_logging()

        env_context = EnvContext(
            self.config.env_config,
            worker_index=self.worker_index,
            vector_index=0,
            num_workers=self.num_workers,
            remote=self.config.remote_worker_envs,
            recreated_worker=recreated_worker,
        )
        self.env_context = env_context
        self.config: AlgorithmConfig = config
        self.callbacks: RLlibCallback = self.config.callbacks_class()
        self.recreated_worker: bool = recreated_worker

        # Setup current policy_mapping_fn. Start with the one from the config, which
        # might be None in older checkpoints (nowadays AlgorithmConfig has a proper
        # default for this); Need to cover this situation via the backup lambda here.
        self.policy_mapping_fn = (
            lambda agent_id, episode, worker, **kw: DEFAULT_POLICY_ID
        )
        self.set_policy_mapping_fn(self.config.policy_mapping_fn)

        self.env_creator: EnvCreator = env_creator
        # Resolve possible auto-fragment length.
        configured_rollout_fragment_length = self.config.get_rollout_fragment_length(
            worker_index=self.worker_index
        )
        self.total_rollout_fragment_length: int = (
            configured_rollout_fragment_length * self.config.num_envs_per_env_runner
        )
        self.preprocessing_enabled: bool = not config._disable_preprocessor_api
        self.last_batch: Optional[SampleBatchType] = None
        self.global_vars: dict = {
            # TODO(sven): Make this per-policy!
            "timestep": 0,
            # Counter for performed gradient updates per policy in `self.policy_map`.
            # Allows for compiling metrics on the off-policy'ness of an update given
            # that the number of gradient updates of the sampling policies are known
            # to the learner (and can be compared to the learner version of the same
            # policy).
            "num_grad_updates_per_policy": defaultdict(int),
        }

        # If seed is provided, add worker index to it and 10k iff evaluation worker.
        self.seed = (
            None
            if self.config.seed is None
            else self.config.seed
            + self.worker_index
            + self.config.in_evaluation * 10000
        )

        # Update the global seed for numpy/random/tf-eager/torch if we are not
        # the local worker, otherwise, this was already done in the Algorithm
        # object itself.
        if self.worker_index > 0:
            update_global_seed_if_necessary(self.config.framework_str, self.seed)

        # A single environment provided by the user (via config.env). This may
        # also remain None.
        # 1) Create the env using the user provided env_creator. This may
        #    return a gym.Env (incl. MultiAgentEnv), an already vectorized
        #    VectorEnv, BaseEnv, ExternalEnv, or an ActorHandle (remote env).
        # 2) Wrap - if applicable - with Atari/rendering wrappers.
        # 3) Seed the env, if necessary.
        # 4) Vectorize the existing single env by creating more clones of
        #    this env and wrapping it with the RLlib BaseEnv class.
        self.env = self.make_sub_env_fn = None

        # Create a (single) env for this worker.
        if not (
            self.worker_index == 0
            and self.num_workers > 0
            and not self.config.create_local_env_runner
        ):
            # Run the `env_creator` function passing the EnvContext.
            self.env = env_creator(copy.deepcopy(self.env_context))

        clip_rewards = self.config.clip_rewards

        if self.env is not None:
            # Custom validation function given, typically a function attribute of the
            # Algorithm.
            if validate_env is not None:
                validate_env(self.env, self.env_context)

            # We can't auto-wrap a BaseEnv.
            if isinstance(self.env, (BaseEnv, ray.actor.ActorHandle)):

                def wrap(env):
                    return env

            # Atari type env and "deepmind" preprocessor pref.
            elif is_atari(self.env) and self.config.preprocessor_pref == "deepmind":
                # Deepmind wrappers already handle all preprocessing.
                self.preprocessing_enabled = False

                # If clip_rewards not explicitly set to False, switch it
                # on here (clip between -1.0 and 1.0).
                if self.config.clip_rewards is None:
                    clip_rewards = True

                # Framestacking is used.
                use_framestack = self.config.model.get("framestack") is True

                def wrap(env):
                    env = wrap_deepmind(
                        env,
                        dim=self.config.model.get("dim"),
                        framestack=use_framestack,
                        noframeskip=self.config.env_config.get("frameskip", 0) == 1,
                    )
                    return env

            elif self.config.preprocessor_pref is None:
                # Only turn off preprocessing
                self.preprocessing_enabled = False

                def wrap(env):
                    return env

            else:

                def wrap(env):
                    return env

            # Wrap env through the correct wrapper.
            self.env: EnvType = wrap(self.env)
            # Ideally, we would use the same make_sub_env() function below
            # to create self.env, but wrap(env) and self.env has a cyclic
            # dependency on each other right now, so we would settle on
            # duplicating the random seed setting logic for now.
            _update_env_seed_if_necessary(self.env, self.seed, self.worker_index, 0)
            # Call custom callback function `on_sub_environment_created`.
            self.callbacks.on_sub_environment_created(
                worker=self,
                sub_environment=self.env,
                env_context=self.env_context,
            )

            self.make_sub_env_fn = self._get_make_sub_env_fn(
                env_creator, env_context, validate_env, wrap, self.seed
            )

        self.spaces = spaces
        self.default_policy_class = default_policy_class
        self.policy_dict, self.is_policy_to_train = self.config.get_multi_agent_setup(
            env=self.env,
            spaces=self.spaces,
            default_policy_class=self.default_policy_class,
        )

        self.policy_map: Optional[PolicyMap] = None
        # TODO(jungong) : clean up after non-connector env_runner is fully deprecated.
        self.preprocessors: Dict[PolicyID, Preprocessor] = None

        # Check available number of GPUs.
        num_gpus = (
            self.config.num_gpus
            if self.worker_index == 0
            else self.config.num_gpus_per_env_runner
        )

        # Error if we don't find enough GPUs.
        if (
            ray.is_initialized()
            and ray._private.worker._mode() != ray._private.worker.LOCAL_MODE
            and not config._fake_gpus
        ):
            devices = []
            if self.config.framework_str in ["tf2", "tf"]:
                devices = get_tf_gpu_devices()
            elif self.config.framework_str == "torch":
                devices = list(range(torch.cuda.device_count()))

            if len(devices) < num_gpus:
                raise RuntimeError(
                    ERR_MSG_NO_GPUS.format(len(devices), devices) + HOWTO_CHANGE_CONFIG
                )
        # Warn, if running in local-mode and actual GPUs (not faked) are
        # requested.
        elif (
            ray.is_initialized()
            and ray._private.worker._mode() == ray._private.worker.LOCAL_MODE
            and num_gpus > 0
            and not self.config._fake_gpus
        ):
            logger.warning(
                "You are running ray with `local_mode=True`, but have "
                f"configured {num_gpus} GPUs to be used! In local mode, "
                f"Policies are placed on the CPU and the `num_gpus` setting "
                f"is ignored."
            )

        self.filters: Dict[PolicyID, Filter] = defaultdict(NoFilter)

        # If RLModule API is enabled, multi_rl_module_spec holds the specs of the
        # RLModules.
        self.multi_rl_module_spec = None
        self._update_policy_map(policy_dict=self.policy_dict)

        # Update Policy's view requirements from Model, only if Policy directly
        # inherited from base `Policy` class. At this point here, the Policy
        # must have it's Model (if any) defined and ready to output an initial
        # state.
        for pol in self.policy_map.values():
            if not pol._model_init_state_automatically_added:
                pol._update_model_view_requirements_from_init_state()

        if (
            self.config.is_multi_agent
            and self.env is not None
            and not isinstance(
                self.env,
                (BaseEnv, ExternalMultiAgentEnv, MultiAgentEnv, ray.actor.ActorHandle),
            )
        ):
            raise ValueError(
                f"You are running a multi-agent setup, but the env {self.env} is not a "
                f"subclass of BaseEnv, MultiAgentEnv, ActorHandle, or "
                f"ExternalMultiAgentEnv!"
            )

        if self.worker_index == 0:
            logger.info("Built filter map: {}".format(self.filters))

        # This RolloutWorker has no env.
        if self.env is None:
            self.async_env = None
        # Use a custom env-vectorizer and call it providing self.env.
        elif "custom_vector_env" in self.config:
            self.async_env = self.config.custom_vector_env(self.env)
        # Default: Vectorize self.env via the make_sub_env function. This adds
        # further clones of self.env and creates a RLlib BaseEnv (which is
        # vectorized under the hood).
        else:
            # Always use vector env for consistency even if num_envs_per_env_runner=1.
            self.async_env: BaseEnv = convert_to_base_env(
                self.env,
                make_env=self.make_sub_env_fn,
                num_envs=self.config.num_envs_per_env_runner,
                remote_envs=self.config.remote_worker_envs,
                remote_env_batch_wait_ms=self.config.remote_env_batch_wait_ms,
                worker=self,
                restart_failed_sub_environments=(
                    self.config.restart_failed_sub_environments
                ),
            )

        # `truncate_episodes`: Allow a batch to contain more than one episode
        # (fragments) and always make the batch `rollout_fragment_length`
        # long.
        rollout_fragment_length_for_sampler = configured_rollout_fragment_length
        if self.config.batch_mode == "truncate_episodes":
            pack = True
        # `complete_episodes`: Never cut episodes and sampler will return
        # exactly one (complete) episode per poll.
        else:
            assert self.config.batch_mode == "complete_episodes"
            rollout_fragment_length_for_sampler = float("inf")
            pack = False

        # Create the IOContext for this worker.
        self.io_context: IOContext = IOContext(
            log_dir, self.config, self.worker_index, self
        )

        render = False
        if self.config.render_env is True and (
            self.num_workers == 0 or self.worker_index == 1
        ):
            render = True

        if self.env is None:
            self.sampler = None
        else:
            self.sampler = SyncSampler(
                worker=self,
                env=self.async_env,
                clip_rewards=clip_rewards,
                rollout_fragment_length=rollout_fragment_length_for_sampler,
                count_steps_by=self.config.count_steps_by,
                callbacks=self.callbacks,
                multiple_episodes_in_batch=pack,
                normalize_actions=self.config.normalize_actions,
                clip_actions=self.config.clip_actions,
                observation_fn=self.config.observation_fn,
                sample_collector_class=self.config.sample_collector,
                render=render,
            )

        self.input_reader: InputReader = self._get_input_creator_from_config()(
            self.io_context
        )
        self.output_writer: OutputWriter = self._get_output_creator_from_config()(
            self.io_context
        )

        # The current weights sequence number (version). May remain None for when
        # not tracking weights versions.
        self.weights_seq_no: Optional[int] = None

    @override(EnvRunner)
    def make_env(self):
        # Override this method, b/c it's abstract and must be overridden.
        # However, we see no point in implementing it for the old API stack any longer
        # (the RolloutWorker class will be deprecated soon).
        raise NotImplementedError

    @override(EnvRunner)
    def assert_healthy(self):
        is_healthy = self.policy_map and self.input_reader and self.output_writer
        assert is_healthy, (
            f"RolloutWorker {self} (idx={self.worker_index}; "
            f"num_workers={self.num_workers}) not healthy!"
        )

    @override(EnvRunner)
    def sample(self, **kwargs) -> SampleBatchType:
        """Returns a batch of experience sampled from this worker.

        This method must be implemented by subclasses.

        Returns:
            A columnar batch of experiences (e.g., tensors) or a MultiAgentBatch.

        .. testcode::
            :skipif: True

            import gymnasium as gym
            from ray.rllib.evaluation.rollout_worker import RolloutWorker
            from ray.rllib.algorithms.ppo.ppo_tf_policy import PPOTF1Policy
            worker = RolloutWorker(
              env_creator=lambda _: gym.make("CartPole-v1"),
              default_policy_class=PPOTF1Policy,
              config=AlgorithmConfig(),
            )
            print(worker.sample())

        .. testoutput::

            SampleBatch({"obs": [...], "action": [...], ...})
        """
        if self.config.fake_sampler and self.last_batch is not None:
            return self.last_batch
        elif self.input_reader is None:
            raise ValueError(
                "RolloutWorker has no `input_reader` object! "
                "Cannot call `sample()`. You can try setting "
                "`create_local_env_runner` to True."
            )

        if log_once("sample_start"):
            logger.info(
                "Generating sample batch of size {}".format(
                    self.total_rollout_fragment_length
                )
            )

        batches = [self.input_reader.next()]
        steps_so_far = (
            batches[0].count
            if self.config.count_steps_by == "env_steps"
            else batches[0].agent_steps()
        )

        # In truncate_episodes mode, never pull more than 1 batch per env.
        # This avoids over-running the target batch size.
        if (
            self.config.batch_mode == "truncate_episodes"
            and not self.config.offline_sampling
        ):
            max_batches = self.config.num_envs_per_env_runner
        else:
            max_batches = float("inf")
        while steps_so_far < self.total_rollout_fragment_length and (
            len(batches) < max_batches
        ):
            batch = self.input_reader.next()
            steps_so_far += (
                batch.count
                if self.config.count_steps_by == "env_steps"
                else batch.agent_steps()
            )
            batches.append(batch)

        batch = concat_samples(batches)

        self.callbacks.on_sample_end(worker=self, samples=batch)

        # Always do writes prior to compression for consistency and to allow
        # for better compression inside the writer.
        self.output_writer.write(batch)

        if log_once("sample_end"):
            logger.info("Completed sample batch:\n\n{}\n".format(summarize(batch)))

        if self.config.compress_observations:
            batch.compress(bulk=self.config.compress_observations == "bulk")

        if self.config.fake_sampler:
            self.last_batch = batch

        return batch

    @override(EnvRunner)
    def get_spaces(self) -> Dict[str, Tuple[Space, Space]]:
        spaces = self.foreach_policy(
            lambda p, pid: (pid, p.observation_space, p.action_space)
        )
        spaces = {e[0]: (getattr(e[1], "original_space", e[1]), e[2]) for e in spaces}
        # Try to add the actual env's obs/action spaces.
        env_spaces = self.foreach_env(
            lambda env: (env.observation_space, env.action_space)
        )
        if env_spaces:
            from ray.rllib.env import INPUT_ENV_SPACES

            spaces[INPUT_ENV_SPACES] = env_spaces[0]
        return spaces

    @ray.method(num_returns=2)
    def sample_with_count(self) -> Tuple[SampleBatchType, int]:
        """Same as sample() but returns the count as a separate value.

        Returns:
            A columnar batch of experiences (e.g., tensors) and the
                size of the collected batch.

        .. testcode::
            :skipif: True

            import gymnasium as gym
            from ray.rllib.evaluation.rollout_worker import RolloutWorker
            from ray.rllib.algorithms.ppo.ppo_tf_policy import PPOTF1Policy
            worker = RolloutWorker(
              env_creator=lambda _: gym.make("CartPole-v1"),
              default_policy_class=PPOTFPolicy)
            print(worker.sample_with_count())

        .. testoutput::

            (SampleBatch({"obs": [...], "action": [...], ...}), 3)
        """
        batch = self.sample()
        return batch, batch.count

    def learn_on_batch(self, samples: SampleBatchType) -> Dict:
        """Update policies based on the given batch.

        This is the equivalent to apply_gradients(compute_gradients(samples)),
        but can be optimized to avoid pulling gradients into CPU memory.

        Args:
            samples: The SampleBatch or MultiAgentBatch to learn on.

        Returns:
            Dictionary of extra metadata from compute_gradients().

        .. testcode::
            :skipif: True

            import gymnasium as gym
            from ray.rllib.evaluation.rollout_worker import RolloutWorker
            from ray.rllib.algorithms.ppo.ppo_tf_policy import PPOTF1Policy
            worker = RolloutWorker(
              env_creator=lambda _: gym.make("CartPole-v1"),
              default_policy_class=PPOTF1Policy)
            batch = worker.sample()
            info = worker.learn_on_batch(samples)
        """
        if log_once("learn_on_batch"):
            logger.info(
                "Training on concatenated sample batches:\n\n{}\n".format(
                    summarize(samples)
                )
            )

        info_out = {}
        if isinstance(samples, MultiAgentBatch):
            builders = {}
            to_fetch = {}
            for pid, batch in samples.policy_batches.items():
                if self.is_policy_to_train is not None and not self.is_policy_to_train(
                    pid, samples
                ):
                    continue
                # Decompress SampleBatch, in case some columns are compressed.
                batch.decompress_if_needed()

                policy = self.policy_map[pid]
                tf_session = policy.get_session()
                if tf_session and hasattr(policy, "_build_learn_on_batch"):
                    builders[pid] = _TFRunBuilder(tf_session, "learn_on_batch")
                    to_fetch[pid] = policy._build_learn_on_batch(builders[pid], batch)
                else:
                    info_out[pid] = policy.learn_on_batch(batch)

            info_out.update({pid: builders[pid].get(v) for pid, v in to_fetch.items()})
        else:
            if self.is_policy_to_train is None or self.is_policy_to_train(
                DEFAULT_POLICY_ID, samples
            ):
                info_out.update(
                    {
                        DEFAULT_POLICY_ID: self.policy_map[
                            DEFAULT_POLICY_ID
                        ].learn_on_batch(samples)
                    }
                )
        if log_once("learn_out"):
            logger.debug("Training out:\n\n{}\n".format(summarize(info_out)))
        return info_out

    def sample_and_learn(
        self,
        expected_batch_size: int,
        num_sgd_iter: int,
        sgd_minibatch_size: str,
        standardize_fields: List[str],
    ) -> Tuple[dict, int]:
        """Sample and batch and learn on it.

        This is typically used in combination with distributed allreduce.

        Args:
            expected_batch_size: Expected number of samples to learn on.
            num_sgd_iter: Number of SGD iterations.
            sgd_minibatch_size: SGD minibatch size.
            standardize_fields: List of sample fields to normalize.

        Returns:
            A tuple consisting of a dictionary of extra metadata returned from
                the policies' `learn_on_batch()` and the number of samples
                learned on.
        """
        batch = self.sample()
        assert batch.count == expected_batch_size, (
            "Batch size possibly out of sync between workers, expected:",
            expected_batch_size,
            "got:",
            batch.count,
        )
        logger.info(
            "Executing distributed minibatch SGD "
            "with epoch size {}, minibatch size {}".format(
                batch.count, sgd_minibatch_size
            )
        )
        info = do_minibatch_sgd(
            batch,
            self.policy_map,
            self,
            num_sgd_iter,
            sgd_minibatch_size,
            standardize_fields,
        )
        return info, batch.count

    def compute_gradients(
        self,
        samples: SampleBatchType,
        single_agent: bool = None,
    ) -> Tuple[ModelGradients, dict]:
        """Returns a gradient computed w.r.t the specified samples.

        Uses the Policy's/ies' compute_gradients method(s) to perform the
        calculations. Skips policies that are not trainable as per
        `self.is_policy_to_train()`.

        Args:
            samples: The SampleBatch or MultiAgentBatch to compute gradients
                for using this worker's trainable policies.

        Returns:
            In the single-agent case, a tuple consisting of ModelGradients and
            info dict of the worker's policy.
            In the multi-agent case, a tuple consisting of a dict mapping
            PolicyID to ModelGradients and a dict mapping PolicyID to extra
            metadata info.
            Note that the first return value (grads) can be applied as is to a
            compatible worker using the worker's `apply_gradients()` method.

        .. testcode::
            :skipif: True

            import gymnasium as gym
            from ray.rllib.evaluation.rollout_worker import RolloutWorker
            from ray.rllib.algorithms.ppo.ppo_tf_policy import PPOTF1Policy
            worker = RolloutWorker(
              env_creator=lambda _: gym.make("CartPole-v1"),
              default_policy_class=PPOTF1Policy)
            batch = worker.sample()
            grads, info = worker.compute_gradients(samples)
        """
        if log_once("compute_gradients"):
            logger.info("Compute gradients on:\n\n{}\n".format(summarize(samples)))

        if single_agent is True:
            samples = convert_ma_batch_to_sample_batch(samples)
            grad_out, info_out = self.policy_map[DEFAULT_POLICY_ID].compute_gradients(
                samples
            )
            info_out["batch_count"] = samples.count
            return grad_out, info_out

        # Treat everything as is multi-agent.
        samples = samples.as_multi_agent()

        # Calculate gradients for all policies.
        grad_out, info_out = {}, {}
        if self.config.framework_str == "tf":
            for pid, batch in samples.policy_batches.items():
                if self.is_policy_to_train is not None and not self.is_policy_to_train(
                    pid, samples
                ):
                    continue
                policy = self.policy_map[pid]
                builder = _TFRunBuilder(policy.get_session(), "compute_gradients")
                grad_out[pid], info_out[pid] = policy._build_compute_gradients(
                    builder, batch
                )
            grad_out = {k: builder.get(v) for k, v in grad_out.items()}
            info_out = {k: builder.get(v) for k, v in info_out.items()}
        else:
            for pid, batch in samples.policy_batches.items():
                if self.is_policy_to_train is not None and not self.is_policy_to_train(
                    pid, samples
                ):
                    continue
                grad_out[pid], info_out[pid] = self.policy_map[pid].compute_gradients(
                    batch
                )

        info_out["batch_count"] = samples.count
        if log_once("grad_out"):
            logger.info("Compute grad info:\n\n{}\n".format(summarize(info_out)))

        return grad_out, info_out

    def apply_gradients(
        self,
        grads: Union[ModelGradients, Dict[PolicyID, ModelGradients]],
    ) -> None:
        """Applies the given gradients to this worker's models.

        Uses the Policy's/ies' apply_gradients method(s) to perform the
        operations.

        Args:
            grads: Single ModelGradients (single-agent case) or a dict
                mapping PolicyIDs to the respective model gradients
                structs.

        .. testcode::
            :skipif: True

            import gymnasium as gym
            from ray.rllib.evaluation.rollout_worker import RolloutWorker
            from ray.rllib.algorithms.ppo.ppo_tf_policy import PPOTF1Policy
            worker = RolloutWorker(
              env_creator=lambda _: gym.make("CartPole-v1"),
              default_policy_class=PPOTF1Policy)
            samples = worker.sample()
            grads, info = worker.compute_gradients(samples)
            worker.apply_gradients(grads)
        """
        if log_once("apply_gradients"):
            logger.info("Apply gradients:\n\n{}\n".format(summarize(grads)))
        # Grads is a dict (mapping PolicyIDs to ModelGradients).
        # Multi-agent case.
        if isinstance(grads, dict):
            for pid, g in grads.items():
                if self.is_policy_to_train is None or self.is_policy_to_train(
                    pid, None
                ):
                    self.policy_map[pid].apply_gradients(g)
        # Grads is a ModelGradients type. Single-agent case.
        elif self.is_policy_to_train is None or self.is_policy_to_train(
            DEFAULT_POLICY_ID, None
        ):
            self.policy_map[DEFAULT_POLICY_ID].apply_gradients(grads)

    @override(EnvRunner)
    def get_metrics(self) -> List[RolloutMetrics]:
        """Returns the thus-far collected metrics from this worker's rollouts.

        Returns:
             List of RolloutMetrics collected thus-far.
        """
        # Get metrics from sampler (if any).
        if self.sampler is not None:
            out = self.sampler.get_metrics()
        else:
            out = []

        return out

    def foreach_env(self, func: Callable[[EnvType], T]) -> List[T]:
        """Calls the given function with each sub-environment as arg.

        Args:
            func: The function to call for each underlying
                sub-environment (as only arg).

        Returns:
             The list of return values of all calls to `func([env])`.
        """

        if self.async_env is None:
            return []

        envs = self.async_env.get_sub_environments()
        # Empty list (not implemented): Call function directly on the
        # BaseEnv.
        if not envs:
            return [func(self.async_env)]
        # Call function on all underlying (vectorized) sub environments.
        else:
            return [func(e) for e in envs]

    def foreach_env_with_context(
        self, func: Callable[[EnvType, EnvContext], T]
    ) -> List[T]:
        """Calls given function with each sub-env plus env_ctx as args.

        Args:
            func: The function to call for each underlying
                sub-environment and its EnvContext (as the args).

        Returns:
             The list of return values of all calls to `func([env, ctx])`.
        """

        if self.async_env is None:
            return []

        envs = self.async_env.get_sub_environments()
        # Empty list (not implemented): Call function directly on the
        # BaseEnv.
        if not envs:
            return [func(self.async_env, self.env_context)]
        # Call function on all underlying (vectorized) sub environments.
        else:
            ret = []
            for i, e in enumerate(envs):
                ctx = self.env_context.copy_with_overrides(vector_index=i)
                ret.append(func(e, ctx))
            return ret

    def get_policy(self, policy_id: PolicyID = DEFAULT_POLICY_ID) -> Optional[Policy]:
        """Return policy for the specified id, or None.

        Args:
            policy_id: ID of the policy to return. None for DEFAULT_POLICY_ID
                (in the single agent case).

        Returns:
            The policy under the given ID (or None if not found).
        """
        return self.policy_map.get(policy_id)

    def add_policy(
        self,
        policy_id: PolicyID,
        policy_cls: Optional[Type[Policy]] = None,
        policy: Optional[Policy] = None,
        *,
        observation_space: Optional[Space] = None,
        action_space: Optional[Space] = None,
        config: Optional[PartialAlgorithmConfigDict] = None,
        policy_state: Optional[PolicyState] = None,
        policy_mapping_fn=None,
        policies_to_train: Optional[
            Union[Collection[PolicyID], Callable[[PolicyID, SampleBatchType], bool]]
        ] = None,
        module_spec: Optional[RLModuleSpec] = None,
    ) -> Policy:
        """Adds a new policy to this RolloutWorker.

        Args:
            policy_id: ID of the policy to add.
            policy_cls: The Policy class to use for constructing the new Policy.
                Note: Only one of `policy_cls` or `policy` must be provided.
            policy: The Policy instance to add to this algorithm.
                Note: Only one of `policy_cls` or `policy` must be provided.
            observation_space: The observation space of the policy to add.
            action_space: The action space of the policy to add.
            config: The config overrides for the policy to add.
            policy_state: Optional state dict to apply to the new
                policy instance, right after its construction.
            policy_mapping_fn: An optional (updated) policy mapping function
                to use from here on. Note that already ongoing episodes will
                not change their mapping but will use the old mapping till
                the end of the episode.
            policies_to_train: An optional collection of policy IDs to be
                trained or a callable taking PolicyID and - optionally -
                SampleBatchType and returning a bool (trainable or not?).
                If None, will keep the existing setup in place.
                Policies, whose IDs are not in the list (or for which the
                callable returns False) will not be updated.
            module_spec: In the new RLModule API we need to pass in the module_spec for
                the new module that is supposed to be added. Knowing the policy spec is
                not sufficient.

        Returns:
            The newly added policy.

        Raises:
            ValueError: If both `policy_cls` AND `policy` are provided.
            KeyError: If the given `policy_id` already exists in this worker's
                PolicyMap.
        """
        validate_module_id(policy_id, error=False)

        if module_spec is not None:
            raise ValueError(
                "If you pass in module_spec to the policy, the RLModule API needs "
                "to be enabled."
            )

        if policy_id in self.policy_map:
            raise KeyError(
                f"Policy ID '{policy_id}' already exists in policy map! "
                "Make sure you use a Policy ID that has not been taken yet."
                " Policy IDs that are already in your policy map: "
                f"{list(self.policy_map.keys())}"
            )
        if (policy_cls is None) == (policy is None):
            raise ValueError(
                "Only one of `policy_cls` or `policy` must be provided to "
                "RolloutWorker.add_policy()!"
            )

        if policy is None:
            policy_dict_to_add, _ = self.config.get_multi_agent_setup(
                policies={
                    policy_id: PolicySpec(
                        policy_cls, observation_space, action_space, config
                    )
                },
                env=self.env,
                spaces=self.spaces,
                default_policy_class=self.default_policy_class,
            )
        else:
            policy_dict_to_add = {
                policy_id: PolicySpec(
                    type(policy),
                    policy.observation_space,
                    policy.action_space,
                    policy.config,
                )
            }

        self.policy_dict.update(policy_dict_to_add)
        self._update_policy_map(
            policy_dict=policy_dict_to_add,
            policy=policy,
            policy_states={policy_id: policy_state},
            single_agent_rl_module_spec=module_spec,
        )

        self.set_policy_mapping_fn(policy_mapping_fn)
        if policies_to_train is not None:
            self.set_is_policy_to_train(policies_to_train)

        return self.policy_map[policy_id]

    def remove_policy(
        self,
        *,
        policy_id: PolicyID = DEFAULT_POLICY_ID,
        policy_mapping_fn: Optional[Callable[[AgentID], PolicyID]] = None,
        policies_to_train: Optional[
            Union[Collection[PolicyID], Callable[[PolicyID, SampleBatchType], bool]]
        ] = None,
    ) -> None:
        """Removes a policy from this RolloutWorker.

        Args:
            policy_id: ID of the policy to be removed. None for
                DEFAULT_POLICY_ID.
            policy_mapping_fn: An optional (updated) policy mapping function
                to use from here on. Note that already ongoing episodes will
                not change their mapping but will use the old mapping till
                the end of the episode.
            policies_to_train: An optional collection of policy IDs to be
                trained or a callable taking PolicyID and - optionally -
                SampleBatchType and returning a bool (trainable or not?).
                If None, will keep the existing setup in place.
                Policies, whose IDs are not in the list (or for which the
                callable returns False) will not be updated.
        """
        if policy_id not in self.policy_map:
            raise ValueError(f"Policy ID '{policy_id}' not in policy map!")
        del self.policy_map[policy_id]
        del self.preprocessors[policy_id]
        self.set_policy_mapping_fn(policy_mapping_fn)
        if policies_to_train is not None:
            self.set_is_policy_to_train(policies_to_train)

    def set_policy_mapping_fn(
        self,
        policy_mapping_fn: Optional[Callable[[AgentID, Any], PolicyID]] = None,
    ) -> None:
        """Sets `self.policy_mapping_fn` to a new callable (if provided).

        Args:
            policy_mapping_fn: The new mapping function to use. If None,
                will keep the existing mapping function in place.
        """
        if policy_mapping_fn is not None:
            self.policy_mapping_fn = policy_mapping_fn
            if not callable(self.policy_mapping_fn):
                raise ValueError("`policy_mapping_fn` must be a callable!")

    def set_is_policy_to_train(
        self,
        is_policy_to_train: Union[
            Collection[PolicyID], Callable[[PolicyID, Optional[SampleBatchType]], bool]
        ],
    ) -> None:
        """Sets `self.is_policy_to_train()` to a new callable.

        Args:
            is_policy_to_train: A collection of policy IDs to be
                trained or a callable taking PolicyID and - optionally -
                SampleBatchType and returning a bool (trainable or not?).
                If None, will keep the existing setup in place.
                Policies, whose IDs are not in the list (or for which the
                callable returns False) will not be updated.
        """
        # If collection given, construct a simple default callable returning True
        # if the PolicyID is found in the list/set of IDs.
        if not callable(is_policy_to_train):
            assert isinstance(is_policy_to_train, (list, set, tuple)), (
                "ERROR: `is_policy_to_train`must be a [list|set|tuple] or a "
                "callable taking PolicyID and SampleBatch and returning "
                "True|False (trainable or not?)."
            )
            pols = set(is_policy_to_train)

            def is_policy_to_train(pid, batch=None):
                return pid in pols

        self.is_policy_to_train = is_policy_to_train

    @PublicAPI(stability="alpha")
    def get_policies_to_train(
        self, batch: Optional[SampleBatchType] = None
    ) -> Set[PolicyID]:
        """Returns all policies-to-train, given an optional batch.

        Loops through all policies currently in `self.policy_map` and checks
        the return value of `self.is_policy_to_train(pid, batch)`.

        Args:
            batch: An optional SampleBatchType for the
                `self.is_policy_to_train(pid, [batch]?)` check.

        Returns:
            The set of currently trainable policy IDs, given the optional
            `batch`.
        """
        return {
            pid
            for pid in self.policy_map.keys()
            if self.is_policy_to_train is None or self.is_policy_to_train(pid, batch)
        }

    def for_policy(
        self,
        func: Callable[[Policy, Optional[Any]], T],
        policy_id: Optional[PolicyID] = DEFAULT_POLICY_ID,
        **kwargs,
    ) -> T:
        """Calls the given function with the specified policy as first arg.

        Args:
            func: The function to call with the policy as first arg.
            policy_id: The PolicyID of the policy to call the function with.

        Keyword Args:
            kwargs: Additional kwargs to be passed to the call.

        Returns:
            The return value of the function call.
        """

        return func(self.policy_map[policy_id], **kwargs)

    def foreach_policy(
        self, func: Callable[[Policy, PolicyID, Optional[Any]], T], **kwargs
    ) -> List[T]:
        """Calls the given function with each (policy, policy_id) tuple.

        Args:
            func: The function to call with each (policy, policy ID) tuple.

        Keyword Args:
            kwargs: Additional kwargs to be passed to the call.

        Returns:
             The list of return values of all calls to
                `func([policy, pid, **kwargs])`.
        """
        return [func(policy, pid, **kwargs) for pid, policy in self.policy_map.items()]

    def foreach_policy_to_train(
        self, func: Callable[[Policy, PolicyID, Optional[Any]], T], **kwargs
    ) -> List[T]:
        """
        Calls the given function with each (policy, policy_id) tuple.

        Only those policies/IDs will be called on, for which
        `self.is_policy_to_train()` returns True.

        Args:
            func: The function to call with each (policy, policy ID) tuple,
                for only those policies that `self.is_policy_to_train`
                returns True.

        Keyword Args:
            kwargs: Additional kwargs to be passed to the call.

        Returns:
            The list of return values of all calls to
            `func([policy, pid, **kwargs])`.
        """
        return [
            # Make sure to only iterate over keys() and not items(). Iterating over
            # items will access policy_map elements even for pids that we do not need,
            # i.e. those that are not in policy_to_train. Access to policy_map elements
            # can cause disk access for policies that were offloaded to disk. Since
            # these policies will be skipped in the for-loop accessing them is
            # unnecessary, making subsequent disk access unnecessary.
            func(self.policy_map[pid], pid, **kwargs)
            for pid in self.policy_map.keys()
            if self.is_policy_to_train is None or self.is_policy_to_train(pid, None)
        ]

    def sync_filters(self, new_filters: dict) -> None:
        """Changes self's filter to given and rebases any accumulated delta.

        Args:
            new_filters: Filters with new state to update local copy.
        """
        assert all(k in new_filters for k in self.filters)
        for k in self.filters:
            self.filters[k].sync(new_filters[k])

    def get_filters(self, flush_after: bool = False) -> Dict:
        """Returns a snapshot of filters.

        Args:
            flush_after: Clears the filter buffer state.

        Returns:
            Dict for serializable filters
        """
        return_filters = {}
        for k, f in self.filters.items():
            return_filters[k] = f.as_serializable()
            if flush_after:
                f.reset_buffer()
        return return_filters

    def get_state(self) -> dict:
        filters = self.get_filters(flush_after=True)
        policy_states = {}
        for pid in self.policy_map.keys():
            # If required by the user, only capture policies that are actually
            # trainable. Otherwise, capture all policies (for saving to disk).
            if (
                not self.config.checkpoint_trainable_policies_only
                or self.is_policy_to_train is None
                or self.is_policy_to_train(pid)
            ):
                policy_states[pid] = self.policy_map[pid].get_state()

        return {
            # List all known policy IDs here for convenience. When an Algorithm gets
            # restored from a checkpoint, it will not have access to the list of
            # possible IDs as each policy is stored in its own sub-dir
            # (see "policy_states").
            "policy_ids": list(self.policy_map.keys()),
            # Note that this field will not be stored in the algorithm checkpoint's
            # state file, but each policy will get its own state file generated in
            # a sub-dir within the algo's checkpoint dir.
            "policy_states": policy_states,
            # Also store current mapping fn and which policies to train.
            "policy_mapping_fn": self.policy_mapping_fn,
            "is_policy_to_train": self.is_policy_to_train,
            # TODO: Filters will be replaced by connectors.
            "filters": filters,
        }

    def set_state(self, state: dict) -> None:
        # Backward compatibility (old checkpoints' states would have the local
        # worker state as a bytes object, not a dict).
        if isinstance(state, bytes):
            state = pickle.loads(state)

        # TODO: Once filters are handled by connectors, get rid of the "filters"
        #  key in `state` entirely (will be part of the policies then).
        self.sync_filters(state["filters"])

        # Support older checkpoint versions (< 1.0), in which the policy_map
        # was stored under the "state" key, not "policy_states".
        policy_states = (
            state["policy_states"] if "policy_states" in state else state["state"]
        )
        for pid, policy_state in policy_states.items():
            # If - for some reason - we have an invalid PolicyID in the state,
            # this might be from an older checkpoint (pre v1.0). Just warn here.
            validate_module_id(pid, error=False)

            if pid not in self.policy_map:
                spec = policy_state.get("policy_spec", None)
                if spec is None:
                    logger.warning(
                        f"PolicyID '{pid}' was probably added on-the-fly (not"
                        " part of the static `multagent.policies` config) and"
                        " no PolicySpec objects found in the pickled policy "
                        f"state. Will not add `{pid}`, but ignore it for now."
                    )
                else:
                    policy_spec = (
                        PolicySpec.deserialize(spec) if isinstance(spec, dict) else spec
                    )
                    self.add_policy(
                        policy_id=pid,
                        policy_cls=policy_spec.policy_class,
                        observation_space=policy_spec.observation_space,
                        action_space=policy_spec.action_space,
                        config=policy_spec.config,
                    )
            if pid in self.policy_map:
                self.policy_map[pid].set_state(policy_state)

        # Also restore mapping fn and which policies to train.
        if "policy_mapping_fn" in state:
            self.set_policy_mapping_fn(state["policy_mapping_fn"])
        if state.get("is_policy_to_train") is not None:
            self.set_is_policy_to_train(state["is_policy_to_train"])

    def get_weights(
        self,
        policies: Optional[Collection[PolicyID]] = None,
        inference_only: bool = False,
    ) -> Dict[PolicyID, ModelWeights]:
        """Returns each policies' model weights of this worker.

        Args:
            policies: List of PolicyIDs to get the weights from.
                Use None for all policies.
            inference_only: This argument is only added for interface
                consistency with the new api stack.

        Returns:
            Dict mapping PolicyIDs to ModelWeights.

        .. testcode::
            :skipif: True

            from ray.rllib.evaluation.rollout_worker import RolloutWorker
            # Create a RolloutWorker.
            worker = ...
            weights = worker.get_weights()
            print(weights)

        .. testoutput::

            {"default_policy": {"layer1": array(...), "layer2": ...}}
        """
        if policies is None:
            policies = list(self.policy_map.keys())
        policies = force_list(policies)

        return {
            # Make sure to only iterate over keys() and not items(). Iterating over
            # items will access policy_map elements even for pids that we do not need,
            # i.e. those that are not in policies. Access to policy_map elements can
            # cause disk access for policies that were offloaded to disk. Since these
            # policies will be skipped in the for-loop accessing them is unnecessary,
            # making subsequent disk access unnecessary.
            pid: self.policy_map[pid].get_weights()
            for pid in self.policy_map.keys()
            if pid in policies
        }

    def set_weights(
        self,
        weights: Dict[PolicyID, ModelWeights],
        global_vars: Optional[Dict] = None,
        weights_seq_no: Optional[int] = None,
    ) -> None:
        """Sets each policies' model weights of this worker.

        Args:
            weights: Dict mapping PolicyIDs to the new weights to be used.
            global_vars: An optional global vars dict to set this
                worker to. If None, do not update the global_vars.
            weights_seq_no: If needed, a sequence number for the weights version
                can be passed into this method. If not None, will store this seq no
                (in self.weights_seq_no) and in future calls - if the seq no did not
                change wrt. the last call - will ignore the call to save on performance.

        .. testcode::
            :skipif: True

            from ray.rllib.evaluation.rollout_worker import RolloutWorker
            # Create a RolloutWorker.
            worker = ...
            weights = worker.get_weights()
            # Set `global_vars` (timestep) as well.
            worker.set_weights(weights, {"timestep": 42})
        """
        # Only update our weights, if no seq no given OR given seq no is different
        # from ours.
        if weights_seq_no is None or weights_seq_no != self.weights_seq_no:
            # If per-policy weights are object refs, `ray.get()` them first.
            if weights and isinstance(next(iter(weights.values())), ObjectRef):
                actual_weights = ray.get(list(weights.values()))
                weights = {
                    pid: actual_weights[i] for i, pid in enumerate(weights.keys())
                }

            for pid, w in weights.items():
                if pid in self.policy_map:
                    self.policy_map[pid].set_weights(w)
                elif log_once("set_weights_on_non_existent_policy"):
                    logger.warning(
                        "`RolloutWorker.set_weights()` used with weights from "
                        f"policyID={pid}, but this policy cannot be found on this "
                        f"worker! Skipping ..."
                    )

        self.weights_seq_no = weights_seq_no

        if global_vars:
            self.set_global_vars(global_vars)

    def get_global_vars(self) -> dict:
        """Returns the current `self.global_vars` dict of this RolloutWorker.

        Returns:
            The current `self.global_vars` dict of this RolloutWorker.

        .. testcode::
            :skipif: True

            from ray.rllib.evaluation.rollout_worker import RolloutWorker
            # Create a RolloutWorker.
            worker = ...
            global_vars = worker.get_global_vars()
            print(global_vars)

        .. testoutput::

            {"timestep": 424242}
        """
        return self.global_vars

    def set_global_vars(
        self,
        global_vars: dict,
        policy_ids: Optional[List[PolicyID]] = None,
    ) -> None:
        """Updates this worker's and all its policies' global vars.

        Updates are done using the dict's update method.

        Args:
            global_vars: The global_vars dict to update the `self.global_vars` dict
                from.
            policy_ids: Optional list of Policy IDs to update. If None, will update all
                policies on the to-be-updated workers.

        .. testcode::
            :skipif: True

            worker = ...
            global_vars = worker.set_global_vars(
            ...     {"timestep": 4242})
        """
        # Handle per-policy values.
        global_vars_copy = global_vars.copy()
        gradient_updates_per_policy = global_vars_copy.pop(
            "num_grad_updates_per_policy", {}
        )
        self.global_vars["num_grad_updates_per_policy"].update(
            gradient_updates_per_policy
        )
        # Only update explicitly provided policies or those that that are being
        # trained, in order to avoid superfluous access of policies, which might have
        # been offloaded to the object store.
        # Important b/c global vars are constantly being updated.
        for pid in policy_ids if policy_ids is not None else self.policy_map.keys():
            if self.is_policy_to_train is None or self.is_policy_to_train(pid, None):
                self.policy_map[pid].on_global_var_update(
                    dict(
                        global_vars_copy,
                        # If count is None, Policy won't update the counter.
                        **{"num_grad_updates": gradient_updates_per_policy.get(pid)},
                    )
                )

        # Update all other global vars.
        self.global_vars.update(global_vars_copy)

    @override(EnvRunner)
    def stop(self) -> None:
        """Releases all resources used by this RolloutWorker."""

        # If we have an env -> Release its resources.
        if self.env is not None:
            self.async_env.stop()

        # Close all policies' sessions (if tf static graph).
        for policy in self.policy_map.cache.values():
            sess = policy.get_session()
            # Closes the tf session, if any.
            if sess is not None:
                sess.close()

    def lock(self) -> None:
        """Locks this RolloutWorker via its own threading.Lock."""
        self._lock.acquire()

    def unlock(self) -> None:
        """Unlocks this RolloutWorker via its own threading.Lock."""
        self._lock.release()

    def setup_torch_data_parallel(
        self, url: str, world_rank: int, world_size: int, backend: str
    ) -> None:
        """Join a torch process group for distributed SGD."""

        logger.info(
            "Joining process group, url={}, world_rank={}, "
            "world_size={}, backend={}".format(url, world_rank, world_size, backend)
        )
        torch.distributed.init_process_group(
            backend=backend, init_method=url, rank=world_rank, world_size=world_size
        )

        for pid, policy in self.policy_map.items():
            if not isinstance(policy, (TorchPolicy, TorchPolicyV2)):
                raise ValueError(
                    "This policy does not support torch distributed", policy
                )
            policy.distributed_world_size = world_size

    def creation_args(self) -> dict:
        """Returns the kwargs dict used to create this worker."""
        return self._original_kwargs

    def get_host(self) -> str:
        """Returns the hostname of the process running this evaluator."""
        return platform.node()

    def get_node_ip(self) -> str:
        """Returns the IP address of the node that this worker runs on."""
        return ray.util.get_node_ip_address()

    def find_free_port(self) -> int:
        """Finds a free port on the node that this worker runs on."""
        from ray.air._internal.util import find_free_port

        return find_free_port()

    def _update_policy_map(
        self,
        *,
        policy_dict: MultiAgentPolicyConfigDict,
        policy: Optional[Policy] = None,
        policy_states: Optional[Dict[PolicyID, PolicyState]] = None,
        single_agent_rl_module_spec: Optional[RLModuleSpec] = None,
    ) -> None:
        """Updates the policy map (and other stuff) on this worker.

        It performs the following:
            1. It updates the observation preprocessors and updates the policy_specs
                with the postprocessed observation_spaces.
            2. It updates the policy_specs with the complete algorithm_config (merged
                with the policy_spec's config).
            3. If needed it will update the self.multi_rl_module_spec on this worker
            3. It updates the policy map with the new policies
            4. It updates the filter dict
            5. It calls the on_create_policy() hook of the callbacks on the newly added
                policies.

        Args:
            policy_dict: The policy dict to update the policy map with.
            policy: The policy to update the policy map with.
            policy_states: The policy states to update the policy map with.
            single_agent_rl_module_spec: The RLModuleSpec to add to the
                MultiRLModuleSpec. If None, the config's
                `get_default_rl_module_spec` method's output will be used to create
                the policy with.
        """

        # Update the input policy dict with the postprocessed observation spaces and
        # merge configs. Also updates the preprocessor dict.
        updated_policy_dict = self._get_complete_policy_specs_dict(policy_dict)

        # Builds the self.policy_map dict
        self._build_policy_map(
            policy_dict=updated_policy_dict,
            policy=policy,
            policy_states=policy_states,
        )

        # Initialize the filter dict
        self._update_filter_dict(updated_policy_dict)

        # Call callback policy init hooks (only if the added policy did not exist
        # before).
        if policy is None:
            self._call_callbacks_on_create_policy()

        if self.worker_index == 0:
            logger.info(f"Built policy map: {self.policy_map}")
            logger.info(f"Built preprocessor map: {self.preprocessors}")

    def _get_complete_policy_specs_dict(
        self, policy_dict: MultiAgentPolicyConfigDict
    ) -> MultiAgentPolicyConfigDict:
        """Processes the policy dict and creates a new copy with the processed attrs.

        This processes the observation_space and prepares them for passing to rl module
        construction. It also merges the policy configs with the algorithm config.
        During this processing, we will also construct the preprocessors dict.
        """
        from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

        updated_policy_dict = copy.deepcopy(policy_dict)
        # If our preprocessors dict does not exist yet, create it here.
        self.preprocessors = self.preprocessors or {}
        # Loop through given policy-dict and add each entry to our map.
        for name, policy_spec in sorted(updated_policy_dict.items()):
            logger.debug("Creating policy for {}".format(name))

            # Policy brings its own complete AlgorithmConfig -> Use it for this policy.
            if isinstance(policy_spec.config, AlgorithmConfig):
                merged_conf = policy_spec.config
            else:
                # Update the general config with the specific config
                # for this particular policy.
                merged_conf: "AlgorithmConfig" = self.config.copy(copy_frozen=False)
                merged_conf.update_from_dict(policy_spec.config or {})

            # Update num_workers and worker_index.
            merged_conf.worker_index = self.worker_index

            # Preprocessors.
            obs_space = policy_spec.observation_space
            # Initialize preprocessor for this policy to None.
            self.preprocessors[name] = None
            if self.preprocessing_enabled:
                # Policies should deal with preprocessed (automatically flattened)
                # observations if preprocessing is enabled.
                preprocessor = ModelCatalog.get_preprocessor_for_space(
                    obs_space,
                    merged_conf.model,
                    include_multi_binary=False,
                )
                # Original observation space should be accessible at
                # obs_space.original_space after this step.
                if preprocessor is not None:
                    obs_space = preprocessor.observation_space

            policy_spec.config = merged_conf
            policy_spec.observation_space = obs_space

        return updated_policy_dict

    def _update_policy_dict_with_multi_rl_module(
        self, policy_dict: MultiAgentPolicyConfigDict
    ) -> MultiAgentPolicyConfigDict:
        for name, policy_spec in policy_dict.items():
            policy_spec.config["__multi_rl_module_spec"] = self.multi_rl_module_spec
        return policy_dict

    def _build_policy_map(
        self,
        *,
        policy_dict: MultiAgentPolicyConfigDict,
        policy: Optional[Policy] = None,
        policy_states: Optional[Dict[PolicyID, PolicyState]] = None,
    ) -> None:
        """Adds the given policy_dict to `self.policy_map`.

        Args:
            policy_dict: The MultiAgentPolicyConfigDict to be added to this
                worker's PolicyMap.
            policy: If the policy to add already exists, user can provide it here.
            policy_states: Optional dict from PolicyIDs to PolicyStates to
                restore the states of the policies being built.
        """

        # If our policy_map does not exist yet, create it here.
        self.policy_map = self.policy_map or PolicyMap(
            capacity=self.config.policy_map_capacity,
            policy_states_are_swappable=self.config.policy_states_are_swappable,
        )

        # Loop through given policy-dict and add each entry to our map.
        for name, policy_spec in sorted(policy_dict.items()):
            # Create the actual policy object.
            if policy is None:
                new_policy = create_policy_for_framework(
                    policy_id=name,
                    policy_class=get_tf_eager_cls_if_necessary(
                        policy_spec.policy_class, policy_spec.config
                    ),
                    merged_config=policy_spec.config,
                    observation_space=policy_spec.observation_space,
                    action_space=policy_spec.action_space,
                    worker_index=self.worker_index,
                    seed=self.seed,
                )
            else:
                new_policy = policy

            self.policy_map[name] = new_policy

            restore_states = (policy_states or {}).get(name, None)
            # Set the state of the newly created policy before syncing filters, etc.
            if restore_states:
                new_policy.set_state(restore_states)

    def _update_filter_dict(self, policy_dict: MultiAgentPolicyConfigDict) -> None:
        """Updates the filter dict for the given policy_dict."""

        for name, policy_spec in sorted(policy_dict.items()):
            new_policy = self.policy_map[name]
            # Note(jungong) : We should only create new connectors for the
            # policy iff we are creating a new policy from scratch. i.e,
            # we should NOT create new connectors when we already have the
            # policy object created before this function call or have the
            # restoring states from the caller.
            # Also note that we cannot just check the existence of connectors
            # to decide whether we should create connectors because we may be
            # restoring a policy that has 0 connectors configured.
            if (
                new_policy.agent_connectors is None
                or new_policy.action_connectors is None
            ):
                # TODO(jungong) : revisit this. It will be nicer to create
                # connectors as the last step of Policy.__init__().
                create_connectors_for_policy(new_policy, policy_spec.config)
            maybe_get_filters_for_syncing(self, name)

    def _call_callbacks_on_create_policy(self):
        """Calls the on_create_policy callback for each policy in the policy map."""
        for name, policy in self.policy_map.items():
            self.callbacks.on_create_policy(policy_id=name, policy=policy)

    def _get_input_creator_from_config(self):
        def valid_module(class_path):
            if (
                isinstance(class_path, str)
                and not os.path.isfile(class_path)
                and "." in class_path
            ):
                module_path, class_name = class_path.rsplit(".", 1)
                try:
                    spec = importlib.util.find_spec(module_path)
                    if spec is not None:
                        return True
                except (ModuleNotFoundError, ValueError):
                    print(
                        f"module {module_path} not found while trying to get "
                        f"input {class_path}"
                    )
            return False

        # A callable returning an InputReader object to use.
        if isinstance(self.config.input_, FunctionType):
            return self.config.input_
        # Use RLlib's Sampler classes (SyncSampler).
        elif self.config.input_ == "sampler":
            return lambda ioctx: ioctx.default_sampler_input()
        # Ray Dataset input -> Use `config.input_config` to construct DatasetReader.
        elif self.config.input_ == "dataset":
            assert self._ds_shards is not None
            # Input dataset shards should have already been prepared.
            # We just need to take the proper shard here.
            return lambda ioctx: DatasetReader(
                self._ds_shards[self.worker_index], ioctx
            )
        # Dict: Mix of different input methods with different ratios.
        elif isinstance(self.config.input_, dict):
            return lambda ioctx: ShuffledInput(
                MixedInput(self.config.input_, ioctx), self.config.shuffle_buffer_size
            )
        # A pre-registered input descriptor (str).
        elif isinstance(self.config.input_, str) and registry_contains_input(
            self.config.input_
        ):
            return registry_get_input(self.config.input_)
        # D4RL input.
        elif "d4rl" in self.config.input_:
            env_name = self.config.input_.split(".")[-1]
            return lambda ioctx: D4RLReader(env_name, ioctx)
        # Valid python module (class path) -> Create using `from_config`.
        elif valid_module(self.config.input_):
            return lambda ioctx: ShuffledInput(
                from_config(self.config.input_, ioctx=ioctx)
            )
        # JSON file or list of JSON files -> Use JsonReader (shuffled).
        else:
            return lambda ioctx: ShuffledInput(
                JsonReader(self.config.input_, ioctx), self.config.shuffle_buffer_size
            )

    def _get_output_creator_from_config(self):
        if isinstance(self.config.output, FunctionType):
            return self.config.output
        elif self.config.output is None:
            return lambda ioctx: NoopOutput()
        elif self.config.output == "dataset":
            return lambda ioctx: DatasetWriter(
                ioctx, compress_columns=self.config.output_compress_columns
            )
        elif self.config.output == "logdir":
            return lambda ioctx: JsonWriter(
                ioctx.log_dir,
                ioctx,
                max_file_size=self.config.output_max_file_size,
                compress_columns=self.config.output_compress_columns,
            )
        else:
            return lambda ioctx: JsonWriter(
                self.config.output,
                ioctx,
                max_file_size=self.config.output_max_file_size,
                compress_columns=self.config.output_compress_columns,
            )

    def _get_make_sub_env_fn(
        self, env_creator, env_context, validate_env, env_wrapper, seed
    ):
        def _make_sub_env_local(vector_index):
            # Used to created additional environments during environment
            # vectorization.

            # Create the env context (config dict + meta-data) for
            # this particular sub-env within the vectorized one.
            env_ctx = env_context.copy_with_overrides(vector_index=vector_index)
            # Create the sub-env.
            env = env_creator(env_ctx)
            # Custom validation function given by user.
            if validate_env is not None:
                validate_env(env, env_ctx)
            # Use our wrapper, defined above.
            env = env_wrapper(env)

            # Make sure a deterministic random seed is set on
            # all the sub-environments if specified.
            _update_env_seed_if_necessary(
                env, seed, env_context.worker_index, vector_index
            )
            return env

        if not env_context.remote:

            def _make_sub_env_remote(vector_index):
                sub_env = _make_sub_env_local(vector_index)
                self.callbacks.on_sub_environment_created(
                    worker=self,
                    sub_environment=sub_env,
                    env_context=env_context.copy_with_overrides(
                        worker_index=env_context.worker_index,
                        vector_index=vector_index,
                        remote=False,
                    ),
                )
                return sub_env

            return _make_sub_env_remote

        else:
            return _make_sub_env_local
