import logging
import queue
import threading
import time
from abc import ABCMeta, abstractmethod
from collections import defaultdict, namedtuple
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
)

import numpy as np
import tree  # pip install dm_tree

from ray.rllib.env.base_env import ASYNC_RESET_RETURN, BaseEnv, convert_to_base_env
from ray.rllib.evaluation.collectors.sample_collector import SampleCollector
from ray.rllib.evaluation.collectors.simple_list_collector import SimpleListCollector
from ray.rllib.evaluation.env_runner_v2 import (
    EnvRunnerV2,
    _fetch_atari_metrics,
    _get_or_raise,
    _PerfStats,
)
from ray.rllib.evaluation.episode import Episode
from ray.rllib.evaluation.metrics import RolloutMetrics
from ray.rllib.offline import InputReader
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.policy_map import PolicyMap
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import DeveloperAPI, override
from ray.rllib.utils.debug import summarize
from ray.rllib.utils.deprecation import deprecation_warning
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.numpy import convert_to_numpy, make_action_immutable
from ray.rllib.utils.spaces.space_utils import clip_action, unbatch, unsquash_action
from ray.rllib.utils.typing import (
    AgentID,
    EnvActionType,
    EnvID,
    EnvInfoDict,
    EnvObsType,
    MultiEnvDict,
    PolicyID,
    SampleBatchType,
    TensorStructType,
)
from ray.util.debug import log_once

if TYPE_CHECKING:
    from gym.envs.classic_control.rendering import SimpleImageViewer

    from ray.rllib.algorithms.callbacks import DefaultCallbacks
    from ray.rllib.evaluation.observation_function import ObservationFunction
    from ray.rllib.evaluation.rollout_worker import RolloutWorker

tf1, tf, _ = try_import_tf()
logger = logging.getLogger(__name__)

_PolicyEvalData = namedtuple(
    "_PolicyEvalData",
    ["env_id", "agent_id", "obs", "info", "rnn_state", "prev_action", "prev_reward"],
)

# A batch of RNN states with dimensions [state_index, batch, state_object].
StateBatch = List[List[Any]]


class _NewEpisodeDefaultDict(defaultdict):
    def __missing__(self, env_id):
        if self.default_factory is None:
            raise KeyError(env_id)
        else:
            ret = self[env_id] = self.default_factory(env_id)
            return ret


@DeveloperAPI
class SamplerInput(InputReader, metaclass=ABCMeta):
    """Reads input experiences from an existing sampler."""

    @override(InputReader)
    def next(self) -> SampleBatchType:
        batches = [self.get_data()]
        batches.extend(self.get_extra_batches())
        if len(batches) > 1:
            return batches[0].concat_samples(batches)
        else:
            return batches[0]

    @abstractmethod
    @DeveloperAPI
    def get_data(self) -> SampleBatchType:
        """Called by `self.next()` to return the next batch of data.

        Override this in child classes.

        Returns:
            The next batch of data.
        """
        raise NotImplementedError

    @abstractmethod
    @DeveloperAPI
    def get_metrics(self) -> List[RolloutMetrics]:
        """Returns list of episode metrics since the last call to this method.

        The list will contain one RolloutMetrics object per completed episode.

        Returns:
            List of RolloutMetrics objects, one per completed episode since
            the last call to this method.
        """
        raise NotImplementedError

    @abstractmethod
    @DeveloperAPI
    def get_extra_batches(self) -> List[SampleBatchType]:
        """Returns list of extra batches since the last call to this method.

        The list will contain all SampleBatches or
        MultiAgentBatches that the user has provided thus-far. Users can
        add these "extra batches" to an episode by calling the episode's
        `add_extra_batch([SampleBatchType])` method. This can be done from
        inside an overridden `Policy.compute_actions_from_input_dict(...,
        episodes)` or from a custom callback's `on_episode_[start|step|end]()`
        methods.

        Returns:
            List of SamplesBatches or MultiAgentBatches provided thus-far by
            the user since the last call to this method.
        """
        raise NotImplementedError


@DeveloperAPI
class SyncSampler(SamplerInput):
    """Sync SamplerInput that collects experiences when `get_data()` is called."""

    def __init__(
        self,
        *,
        worker: "RolloutWorker",
        env: BaseEnv,
        clip_rewards: Union[bool, float],
        rollout_fragment_length: int,
        count_steps_by: str = "env_steps",
        callbacks: "DefaultCallbacks",
        horizon: int = None,
        multiple_episodes_in_batch: bool = False,
        normalize_actions: bool = True,
        clip_actions: bool = False,
        soft_horizon: bool = False,
        no_done_at_end: bool = False,
        observation_fn: Optional["ObservationFunction"] = None,
        sample_collector_class: Optional[Type[SampleCollector]] = None,
        render: bool = False,
        # Obsolete.
        policies=None,
        policy_mapping_fn=None,
        preprocessors=None,
        obs_filters=None,
        tf_sess=None,
    ):
        """Initializes a SyncSampler instance.

        Args:
            worker: The RolloutWorker that will use this Sampler for sampling.
            env: Any Env object. Will be converted into an RLlib BaseEnv.
            clip_rewards: True for +/-1.0 clipping,
                actual float value for +/- value clipping. False for no
                clipping.
            rollout_fragment_length: The length of a fragment to collect
                before building a SampleBatch from the data and resetting
                the SampleBatchBuilder object.
            count_steps_by: One of "env_steps" (default) or "agent_steps".
                Use "agent_steps", if you want rollout lengths to be counted
                by individual agent steps. In a multi-agent env,
                a single env_step contains one or more agent_steps, depending
                on how many agents are present at any given time in the
                ongoing episode.
            callbacks: The Callbacks object to use when episode
                events happen during rollout.
            horizon: Hard-reset the Env after this many timesteps.
            multiple_episodes_in_batch: Whether to pack multiple
                episodes into each batch. This guarantees batches will be
                exactly `rollout_fragment_length` in size.
            normalize_actions: Whether to normalize actions to the
                action space's bounds.
            clip_actions: Whether to clip actions according to the
                given action_space's bounds.
            soft_horizon: If True, calculate bootstrapped values as if
                episode had ended, but don't physically reset the environment
                when the horizon is hit.
            no_done_at_end: Ignore the done=True at the end of the
                episode and instead record done=False.
            observation_fn: Optional multi-agent observation func to use for
                preprocessing observations.
            sample_collector_class: An optional Samplecollector sub-class to
                use to collect, store, and retrieve environment-, model-,
                and sampler data.
            render: Whether to try to render the environment after each step.
        """
        # All of the following arguments are deprecated. They will instead be
        # provided via the passed in `worker` arg, e.g. `worker.policy_map`.
        if log_once("deprecated_sync_sampler_args"):
            if policies is not None:
                deprecation_warning(old="policies")
            if policy_mapping_fn is not None:
                deprecation_warning(old="policy_mapping_fn")
            if preprocessors is not None:
                deprecation_warning(old="preprocessors")
            if obs_filters is not None:
                deprecation_warning(old="obs_filters")
            if tf_sess is not None:
                deprecation_warning(old="tf_sess")

        self.base_env = convert_to_base_env(env)
        self.rollout_fragment_length = rollout_fragment_length
        self.horizon = horizon
        self.extra_batches = queue.Queue()
        self.perf_stats = _PerfStats(
            ema_coef=worker.policy_config.get("sampler_perf_stats_ema_coef"),
        )
        if not sample_collector_class:
            sample_collector_class = SimpleListCollector
        self.sample_collector = sample_collector_class(
            worker.policy_map,
            clip_rewards,
            callbacks,
            multiple_episodes_in_batch,
            rollout_fragment_length,
            count_steps_by=count_steps_by,
        )
        self.render = render

        if worker.policy_config.get("enable_connectors", False):
            # Keep a reference to the underlying EnvRunnerV2 instance for
            # unit testing purpose.
            self._env_runner_obj = EnvRunnerV2(
                worker=worker,
                base_env=self.base_env,
                horizon=self.horizon,
                multiple_episodes_in_batch=multiple_episodes_in_batch,
                callbacks=callbacks,
                perf_stats=self.perf_stats,
                soft_horizon=soft_horizon,
                no_done_at_end=no_done_at_end,
                rollout_fragment_length=rollout_fragment_length,
                count_steps_by=count_steps_by,
                render=self.render,
            )
            self._env_runner = self._env_runner_obj.run()
        else:
            # Create the rollout generator to use for calls to `get_data()`.
            self._env_runner = _env_runner(
                worker,
                self.base_env,
                self.extra_batches.put,
                self.horizon,
                normalize_actions,
                clip_actions,
                multiple_episodes_in_batch,
                callbacks,
                self.perf_stats,
                soft_horizon,
                no_done_at_end,
                observation_fn,
                self.sample_collector,
                self.render,
            )
        self.metrics_queue = queue.Queue()

    @override(SamplerInput)
    def get_data(self) -> SampleBatchType:
        while True:
            item = next(self._env_runner)
            if isinstance(item, RolloutMetrics):
                self.metrics_queue.put(item)
            else:
                return item

    @override(SamplerInput)
    def get_metrics(self) -> List[RolloutMetrics]:
        completed = []
        while True:
            try:
                completed.append(
                    self.metrics_queue.get_nowait()._replace(
                        perf_stats=self.perf_stats.get()
                    )
                )
            except queue.Empty:
                break
        return completed

    @override(SamplerInput)
    def get_extra_batches(self) -> List[SampleBatchType]:
        extra = []
        while True:
            try:
                extra.append(self.extra_batches.get_nowait())
            except queue.Empty:
                break
        return extra


@DeveloperAPI
class AsyncSampler(threading.Thread, SamplerInput):
    """Async SamplerInput that collects experiences in thread and queues them.

    Once started, experiences are continuously collected in the background
    and put into a Queue, from where they can be unqueued by the caller
    of `get_data()`.
    """

    def __init__(
        self,
        *,
        worker: "RolloutWorker",
        env: BaseEnv,
        clip_rewards: Union[bool, float],
        rollout_fragment_length: int,
        count_steps_by: str = "env_steps",
        callbacks: "DefaultCallbacks",
        horizon: Optional[int] = None,
        multiple_episodes_in_batch: bool = False,
        normalize_actions: bool = True,
        clip_actions: bool = False,
        soft_horizon: bool = False,
        no_done_at_end: bool = False,
        observation_fn: Optional["ObservationFunction"] = None,
        sample_collector_class: Optional[Type[SampleCollector]] = None,
        render: bool = False,
        blackhole_outputs: bool = False,
        # Obsolete.
        policies=None,
        policy_mapping_fn=None,
        preprocessors=None,
        obs_filters=None,
        tf_sess=None,
    ):
        """Initializes an AsyncSampler instance.

        Args:
            worker: The RolloutWorker that will use this Sampler for sampling.
            env: Any Env object. Will be converted into an RLlib BaseEnv.
            clip_rewards: True for +/-1.0 clipping,
                actual float value for +/- value clipping. False for no
                clipping.
            rollout_fragment_length: The length of a fragment to collect
                before building a SampleBatch from the data and resetting
                the SampleBatchBuilder object.
            count_steps_by: One of "env_steps" (default) or "agent_steps".
                Use "agent_steps", if you want rollout lengths to be counted
                by individual agent steps. In a multi-agent env,
                a single env_step contains one or more agent_steps, depending
                on how many agents are present at any given time in the
                ongoing episode.
            horizon: Hard-reset the Env after this many timesteps.
            multiple_episodes_in_batch: Whether to pack multiple
                episodes into each batch. This guarantees batches will be
                exactly `rollout_fragment_length` in size.
            normalize_actions: Whether to normalize actions to the
                action space's bounds.
            clip_actions: Whether to clip actions according to the
                given action_space's bounds.
            blackhole_outputs: Whether to collect samples, but then
                not further process or store them (throw away all samples).
            soft_horizon: If True, calculate bootstrapped values as if
                episode had ended, but don't physically reset the environment
                when the horizon is hit.
            no_done_at_end: Ignore the done=True at the end of the
                episode and instead record done=False.
            observation_fn: Optional multi-agent observation func to use for
                preprocessing observations.
            sample_collector_class: An optional SampleCollector sub-class to
                use to collect, store, and retrieve environment-, model-,
                and sampler data.
            render: Whether to try to render the environment after each step.
        """
        # All of the following arguments are deprecated. They will instead be
        # provided via the passed in `worker` arg, e.g. `worker.policy_map`.
        if log_once("deprecated_async_sampler_args"):
            if policies is not None:
                deprecation_warning(old="policies")
            if policy_mapping_fn is not None:
                deprecation_warning(old="policy_mapping_fn")
            if preprocessors is not None:
                deprecation_warning(old="preprocessors")
            if obs_filters is not None:
                deprecation_warning(old="obs_filters")
            if tf_sess is not None:
                deprecation_warning(old="tf_sess")

        self.worker = worker

        for _, f in worker.filters.items():
            assert getattr(
                f, "is_concurrent", False
            ), "Observation Filter must support concurrent updates."

        self.base_env = convert_to_base_env(env)
        threading.Thread.__init__(self)
        self.queue = queue.Queue(5)
        self.extra_batches = queue.Queue()
        self.metrics_queue = queue.Queue()
        self.rollout_fragment_length = rollout_fragment_length
        self.horizon = horizon
        self.clip_rewards = clip_rewards
        self.daemon = True
        self.multiple_episodes_in_batch = multiple_episodes_in_batch
        self.callbacks = callbacks
        self.normalize_actions = normalize_actions
        self.clip_actions = clip_actions
        self.blackhole_outputs = blackhole_outputs
        self.soft_horizon = soft_horizon
        self.no_done_at_end = no_done_at_end
        self.perf_stats = _PerfStats(
            ema_coef=worker.policy_config.get("sampler_perf_stats_ema_coef"),
        )
        self.shutdown = False
        self.observation_fn = observation_fn
        self.render = render
        if not sample_collector_class:
            sample_collector_class = SimpleListCollector
        self.sample_collector = sample_collector_class(
            self.worker.policy_map,
            self.clip_rewards,
            self.callbacks,
            self.multiple_episodes_in_batch,
            self.rollout_fragment_length,
            count_steps_by=count_steps_by,
        )
        self.count_steps_by = count_steps_by

    @override(threading.Thread)
    def run(self):
        try:
            self._run()
        except BaseException as e:
            self.queue.put(e)
            raise e

    def _run(self):
        # We are in a thread: Switch on eager execution mode, iff framework==tf2|tfe.
        if (
            tf1
            and self.worker.policy_config.get("framework", "tf") in ["tf2", "tfe"]
            and not tf1.executing_eagerly()
        ):
            tf1.enable_eager_execution()

        if self.blackhole_outputs:
            queue_putter = lambda x: None
            extra_batches_putter = lambda x: None
        else:
            queue_putter = self.queue.put
            extra_batches_putter = lambda x: self.extra_batches.put(x, timeout=600.0)
        if self.worker.policy_config.get("enable_connectors", False):
            env_runner = EnvRunnerV2(
                worker=self.worker,
                base_env=self.base_env,
                horizon=self.horizon,
                multiple_episodes_in_batch=self.multiple_episodes_in_batch,
                callbacks=self.callbacks,
                perf_stats=self.perf_stats,
                soft_horizon=self.soft_horizon,
                no_done_at_end=self.no_done_at_end,
                rollout_fragment_length=self.rollout_fragment_length,
                count_steps_by=self.count_steps_by,
                render=self.render,
            ).run()
        else:
            env_runner = _env_runner(
                self.worker,
                self.base_env,
                extra_batches_putter,
                self.horizon,
                self.normalize_actions,
                self.clip_actions,
                self.multiple_episodes_in_batch,
                self.callbacks,
                self.perf_stats,
                self.soft_horizon,
                self.no_done_at_end,
                self.observation_fn,
                self.sample_collector,
                self.render,
            )
        while not self.shutdown:
            # The timeout variable exists because apparently, if one worker
            # dies, the other workers won't die with it, unless the timeout is
            # set to some large number. This is an empirical observation.
            item = next(env_runner)
            if isinstance(item, RolloutMetrics):
                self.metrics_queue.put(item)
            else:
                queue_putter(item)

    @override(SamplerInput)
    def get_data(self) -> SampleBatchType:
        if not self.is_alive():
            raise RuntimeError("Sampling thread has died")
        rollout = self.queue.get(timeout=600.0)

        # Propagate errors.
        if isinstance(rollout, BaseException):
            raise rollout

        return rollout

    @override(SamplerInput)
    def get_metrics(self) -> List[RolloutMetrics]:
        completed = []
        while True:
            try:
                completed.append(
                    self.metrics_queue.get_nowait()._replace(
                        perf_stats=self.perf_stats.get()
                    )
                )
            except queue.Empty:
                break
        return completed

    @override(SamplerInput)
    def get_extra_batches(self) -> List[SampleBatchType]:
        extra = []
        while True:
            try:
                extra.append(self.extra_batches.get_nowait())
            except queue.Empty:
                break
        return extra


def _env_runner(
    worker: "RolloutWorker",
    base_env: BaseEnv,
    extra_batch_callback: Callable[[SampleBatchType], None],
    horizon: Optional[int],
    normalize_actions: bool,
    clip_actions: bool,
    multiple_episodes_in_batch: bool,
    callbacks: "DefaultCallbacks",
    perf_stats: _PerfStats,
    soft_horizon: bool,
    no_done_at_end: bool,
    observation_fn: "ObservationFunction",
    sample_collector: Optional[SampleCollector] = None,
    render: bool = None,
) -> Iterator[SampleBatchType]:
    """This implements the common experience collection logic.

    Args:
        worker: Reference to the current rollout worker.
        base_env: Env implementing BaseEnv.
        extra_batch_callback: function to send extra batch data to.
        horizon: Horizon of the episode.
        multiple_episodes_in_batch: Whether to pack multiple
            episodes into each batch. This guarantees batches will be exactly
            `rollout_fragment_length` in size.
        normalize_actions: Whether to normalize actions to the action
            space's bounds.
        clip_actions: Whether to clip actions to the space range.
        callbacks: User callbacks to run on episode events.
        perf_stats: Record perf stats into this object.
        soft_horizon: Calculate rewards but don't reset the
            environment when the horizon is hit.
        no_done_at_end: Ignore the done=True at the end of the episode
            and instead record done=False.
        observation_fn: Optional multi-agent
            observation func to use for preprocessing observations.
        sample_collector: An optional
            SampleCollector object to use.
        render: Whether to try to render the environment after each
            step.

    Yields:
        Object containing state, action, reward, terminal condition,
        and other fields as dictated by `policy`.
    """

    # May be populated with used for image rendering
    simple_image_viewer: Optional["SimpleImageViewer"] = None

    # Try to get Env's `max_episode_steps` prop. If it doesn't exist, ignore
    # error and continue with max_episode_steps=None.
    max_episode_steps = None
    try:
        max_episode_steps = base_env.get_sub_environments()[0].spec.max_episode_steps
    except Exception:
        pass

    # Trainer has a given `horizon` setting.
    if horizon:
        # `horizon` is larger than env's limit.
        if max_episode_steps and horizon > max_episode_steps:
            # Try to override the env's own max-step setting with our horizon.
            # If this won't work, throw an error.
            try:
                base_env.get_sub_environments()[0].spec.max_episode_steps = horizon
                base_env.get_sub_environments()[0]._max_episode_steps = horizon
            except Exception:
                raise ValueError(
                    "Your `horizon` setting ({}) is larger than the Env's own "
                    "timestep limit ({}), which seems to be unsettable! Try "
                    "to increase the Env's built-in limit to be at least as "
                    "large as your wanted `horizon`.".format(horizon, max_episode_steps)
                )
    # Otherwise, set Trainer's horizon to env's max-steps.
    elif max_episode_steps:
        horizon = max_episode_steps
        logger.debug(
            "No episode horizon specified, setting it to Env's limit ({}).".format(
                max_episode_steps
            )
        )
    # No horizon/max_episode_steps -> Episodes may be infinitely long.
    else:
        horizon = float("inf")
        logger.debug("No episode horizon specified, assuming inf.")

    def _new_episode(env_id):
        episode = Episode(
            worker.policy_map,
            worker.policy_mapping_fn,
            # SimpleListCollector will find or create a
            # simple_list_collector._PolicyCollector as batch_builder
            # for this episode later. Here we simply provide a None factory.
            lambda: None,  # batch_builder_factory
            extra_batch_callback,
            env_id=env_id,
            worker=worker,
        )
        return episode

    active_episodes: Dict[EnvID, Episode] = _NewEpisodeDefaultDict(_new_episode)

    # Before the very first poll (this will reset all vector sub-environments):
    # Call custom `before_sub_environment_reset` callbacks for all sub-environments.
    for env_id, sub_env in base_env.get_sub_environments(as_dict=True).items():
        _create_episode(active_episodes, env_id, callbacks, worker, base_env)

    while True:
        perf_stats.incr("iters", 1)

        t0 = time.time()
        # Get observations from all ready agents.
        # types: MultiEnvDict, MultiEnvDict, MultiEnvDict, MultiEnvDict, ...
        unfiltered_obs, rewards, dones, infos, off_policy_actions = base_env.poll()
        env_poll_time = time.time() - t0

        if log_once("env_returns"):
            logger.info("Raw obs from env: {}".format(summarize(unfiltered_obs)))
            logger.info("Info return from env: {}".format(summarize(infos)))

        # Process observations and prepare for policy evaluation.
        t1 = time.time()
        # types: Set[EnvID], Dict[PolicyID, List[_PolicyEvalData]],
        #       List[Union[RolloutMetrics, SampleBatchType]]
        active_envs, to_eval, outputs = _process_observations(
            worker=worker,
            base_env=base_env,
            active_episodes=active_episodes,
            unfiltered_obs=unfiltered_obs,
            rewards=rewards,
            dones=dones,
            infos=infos,
            horizon=horizon,
            multiple_episodes_in_batch=multiple_episodes_in_batch,
            callbacks=callbacks,
            soft_horizon=soft_horizon,
            no_done_at_end=no_done_at_end,
            observation_fn=observation_fn,
            sample_collector=sample_collector,
        )
        perf_stats.incr("raw_obs_processing_time", time.time() - t1)
        for o in outputs:
            yield o

        # Do batched policy eval (accross vectorized envs).
        t2 = time.time()
        # types: Dict[PolicyID, Tuple[TensorStructType, StateBatch, dict]]
        eval_results = _do_policy_eval(
            to_eval=to_eval,
            policies=worker.policy_map,
            sample_collector=sample_collector,
            active_episodes=active_episodes,
        )
        perf_stats.incr("inference_time", time.time() - t2)

        # Process results and update episode state.
        t3 = time.time()
        actions_to_send: Dict[
            EnvID, Dict[AgentID, EnvActionType]
        ] = _process_policy_eval_results(
            to_eval=to_eval,
            eval_results=eval_results,
            active_episodes=active_episodes,
            active_envs=active_envs,
            off_policy_actions=off_policy_actions,
            policies=worker.policy_map,
            normalize_actions=normalize_actions,
            clip_actions=clip_actions,
        )
        perf_stats.incr("action_processing_time", time.time() - t3)

        # Return computed actions to ready envs. We also send to envs that have
        # taken off-policy actions; those envs are free to ignore the action.
        t4 = time.time()
        base_env.send_actions(actions_to_send)
        perf_stats.incr("env_wait_time", env_poll_time + time.time() - t4)

        # Try to render the env, if required.
        if render:
            t5 = time.time()
            # Render can either return an RGB image (uint8 [w x h x 3] numpy
            # array) or take care of rendering itself (returning True).
            rendered = base_env.try_render()
            # Rendering returned an image -> Display it in a SimpleImageViewer.
            if isinstance(rendered, np.ndarray) and len(rendered.shape) == 3:
                # ImageViewer not defined yet, try to create one.
                if simple_image_viewer is None:
                    try:
                        from gym.envs.classic_control.rendering import SimpleImageViewer

                        simple_image_viewer = SimpleImageViewer()
                    except (ImportError, ModuleNotFoundError):
                        render = False  # disable rendering
                        logger.warning(
                            "Could not import gym.envs.classic_control."
                            "rendering! Try `pip install gym[all]`."
                        )
                if simple_image_viewer:
                    simple_image_viewer.imshow(rendered)
            elif rendered not in [True, False, None]:
                raise ValueError(
                    f"The env's ({base_env}) `try_render()` method returned an"
                    " unsupported value! Make sure you either return a "
                    "uint8/w x h x 3 (RGB) image or handle rendering in a "
                    "window and then return `True`."
                )
            perf_stats.incr("env_render_time", time.time() - t5)


def _process_observations(
    *,
    worker: "RolloutWorker",
    base_env: BaseEnv,
    active_episodes: Dict[EnvID, Episode],
    unfiltered_obs: Dict[EnvID, Dict[AgentID, EnvObsType]],
    rewards: Dict[EnvID, Dict[AgentID, float]],
    dones: Dict[EnvID, Dict[AgentID, bool]],
    infos: Dict[EnvID, Dict[AgentID, EnvInfoDict]],
    horizon: int,
    multiple_episodes_in_batch: bool,
    callbacks: "DefaultCallbacks",
    soft_horizon: bool,
    no_done_at_end: bool,
    observation_fn: "ObservationFunction",
    sample_collector: SampleCollector,
) -> Tuple[
    Set[EnvID],
    Dict[PolicyID, List[_PolicyEvalData]],
    List[Union[RolloutMetrics, SampleBatchType]],
]:
    """Record new data from the environment and prepare for policy evaluation.

    Args:
        worker: Reference to the current rollout worker.
        base_env: Env implementing BaseEnv.
        active_episodes: Mapping from
            episode ID to currently ongoing Episode object.
        unfiltered_obs: Doubly keyed dict of env-ids -> agent ids
            -> unfiltered observation tensor, returned by a `BaseEnv.poll()`
            call.
        rewards: Doubly keyed dict of env-ids -> agent ids ->
            rewards tensor, returned by a `BaseEnv.poll()` call.
        dones: Doubly keyed dict of env-ids -> agent ids ->
            boolean done flags, returned by a `BaseEnv.poll()` call.
        infos: Doubly keyed dict of env-ids -> agent ids ->
            info dicts, returned by a `BaseEnv.poll()` call.
        horizon: Horizon of the episode.
        multiple_episodes_in_batch: Whether to pack multiple
            episodes into each batch. This guarantees batches will be exactly
            `rollout_fragment_length` in size.
        callbacks: User callbacks to run on episode events.
        soft_horizon: Calculate rewards but don't reset the
            environment when the horizon is hit.
        no_done_at_end: Ignore the done=True at the end of the episode
            and instead record done=False.
        observation_fn: Optional multi-agent
            observation func to use for preprocessing observations.
        sample_collector: The SampleCollector object
            used to store and retrieve environment samples.

    Returns:
        Tuple consisting of 1) active_envs: Set of non-terminated env ids.
        2) to_eval: Map of policy_id to list of agent _PolicyEvalData.
        3) outputs: List of metrics and samples to return from the sampler.
    """

    # Output objects.
    active_envs: Set[EnvID] = set()
    to_eval: Dict[PolicyID, List[_PolicyEvalData]] = defaultdict(list)
    outputs: List[Union[RolloutMetrics, SampleBatchType]] = []

    # For each (vectorized) sub-environment.
    # types: EnvID, Dict[AgentID, EnvObsType]
    for env_id, all_agents_obs in unfiltered_obs.items():
        episode: Episode = active_episodes[env_id]

        # Check for env_id having returned an error instead of a multi-agent obs dict.
        # This is how our BaseEnv can tell the caller to `poll()` that one of its
        # sub-environments is faulty and should be restarted (and the ongoing episode
        # should not be used for training).
        if isinstance(all_agents_obs, Exception):
            episode.is_faulty = True
            assert dones[env_id]["__all__"] is True, (
                f"ERROR: When a sub-environment (env-id {env_id}) returns an error "
                "as observation, the dones[__all__] flag must also be set to True!"
            )
            # This will be filled with dummy observations below.
            all_agents_obs = {}

        # If this episode is brand-new, call the episode start callback(s).
        if episode.started is False:
            _call_on_episode_start(episode, env_id, callbacks, worker, base_env)
        else:
            sample_collector.episode_step(episode)
            episode._add_agent_rewards(rewards[env_id])

        # Check episode termination conditions.
        if dones[env_id]["__all__"] or episode.length >= horizon:
            hit_horizon = episode.length >= horizon and not dones[env_id]["__all__"]
            all_agents_done = True
            atari_metrics: List[RolloutMetrics] = _fetch_atari_metrics(base_env)
            if not episode.is_faulty:
                if atari_metrics is not None:
                    for m in atari_metrics:
                        outputs.append(
                            m._replace(
                                custom_metrics=episode.custom_metrics,
                                hist_data=episode.hist_data,
                            )
                        )
                else:
                    outputs.append(
                        RolloutMetrics(
                            episode.length,
                            episode.total_reward,
                            dict(episode.agent_rewards),
                            episode.custom_metrics,
                            {},
                            episode.hist_data,
                            episode.media,
                        )
                    )
            else:
                # Add metrics about a faulty episode.
                outputs.append(RolloutMetrics(episode_faulty=True))
            # Check whether we have to create a fake-last observation
            # for some agents (the environment is not required to do so if
            # dones[__all__]=True).
            for ag_id in episode.get_agents():
                if not episode.last_done_for(ag_id) and ag_id not in all_agents_obs:
                    # Create a fake (all-0s) observation.
                    obs_sp = worker.policy_map[
                        episode.policy_for(ag_id)
                    ].observation_space
                    obs_sp = getattr(obs_sp, "original_space", obs_sp)
                    all_agents_obs[ag_id] = tree.map_structure(
                        np.zeros_like, obs_sp.sample()
                    )
        else:
            hit_horizon = False
            all_agents_done = False
            active_envs.add(env_id)

        # Custom observation function is applied before preprocessing.
        if observation_fn:
            all_agents_obs: Dict[AgentID, EnvObsType] = observation_fn(
                agent_obs=all_agents_obs,
                worker=worker,
                base_env=base_env,
                policies=worker.policy_map,
                episode=episode,
            )
            if not isinstance(all_agents_obs, dict):
                raise ValueError("observe() must return a dict of agent observations")

        common_infos = infos[env_id].get("__common__", {})
        episode._set_last_info("__common__", common_infos)

        # For each agent in the environment.
        # types: AgentID, EnvObsType
        for agent_id, raw_obs in all_agents_obs.items():
            assert agent_id != "__all__"

            last_observation: EnvObsType = episode.last_observation_for(agent_id)
            agent_done = bool(all_agents_done or dones[env_id].get(agent_id))

            # A new agent (initial obs) is already done -> Skip entirely.
            if last_observation is None and agent_done:
                continue

            policy_id: PolicyID = episode.policy_for(agent_id)

            preprocessor = _get_or_raise(worker.preprocessors, policy_id)
            prep_obs: EnvObsType = raw_obs
            if preprocessor is not None:
                prep_obs = preprocessor.transform(raw_obs)
                if log_once("prep_obs"):
                    logger.info("Preprocessed obs: {}".format(summarize(prep_obs)))
            filtered_obs: EnvObsType = _get_or_raise(worker.filters, policy_id)(
                prep_obs
            )
            if log_once("filtered_obs"):
                logger.info("Filtered obs: {}".format(summarize(filtered_obs)))

            episode._set_last_observation(agent_id, filtered_obs)
            episode._set_last_raw_obs(agent_id, raw_obs)
            episode._set_last_done(agent_id, agent_done)
            # Infos from the environment.
            agent_infos = infos[env_id].get(agent_id, {})
            episode._set_last_info(agent_id, agent_infos)

            # Record transition info if applicable.
            if last_observation is None:
                sample_collector.add_init_obs(
                    episode,
                    agent_id,
                    env_id,
                    policy_id,
                    episode.length - 1,
                    filtered_obs,
                )
            else:
                # Add actions, rewards, next-obs to collectors.
                values_dict = {
                    SampleBatch.T: episode.length - 1,
                    SampleBatch.ENV_ID: env_id,
                    SampleBatch.AGENT_INDEX: episode._agent_index(agent_id),
                    # Action (slot 0) taken at timestep t.
                    SampleBatch.ACTIONS: episode.last_action_for(agent_id),
                    # Reward received after taking a at timestep t.
                    SampleBatch.REWARDS: rewards[env_id].get(agent_id, 0.0),
                    # After taking action=a, did we reach terminal?
                    SampleBatch.DONES: (
                        False
                        if (no_done_at_end or (hit_horizon and soft_horizon))
                        else agent_done
                    ),
                    # Next observation.
                    SampleBatch.NEXT_OBS: filtered_obs,
                }
                # Add extra-action-fetches (policy-inference infos) to
                # collectors.
                pol = worker.policy_map[policy_id]
                for key, value in episode.last_extra_action_outs_for(agent_id).items():
                    if key in pol.view_requirements:
                        values_dict[key] = value
                # Env infos for this agent.
                if "infos" in pol.view_requirements:
                    values_dict["infos"] = agent_infos
                sample_collector.add_action_reward_next_obs(
                    episode.episode_id,
                    agent_id,
                    env_id,
                    policy_id,
                    agent_done,
                    values_dict,
                )

            if not agent_done:
                item = _PolicyEvalData(
                    env_id,
                    agent_id,
                    filtered_obs,
                    agent_infos,
                    None
                    if last_observation is None
                    else episode.rnn_state_for(agent_id),
                    None
                    if last_observation is None
                    else episode.last_action_for(agent_id),
                    rewards[env_id].get(agent_id, 0.0),
                )
                to_eval[policy_id].append(item)

        # Invoke the `on_episode_step` callback after the step is logged
        # to the episode.
        # Exception: The very first env.poll() call causes the env to get reset
        # (no step taken yet, just a single starting observation logged).
        # We need to skip this callback in this case.
        if not episode.is_faulty and episode.length > 0:
            callbacks.on_episode_step(
                worker=worker,
                base_env=base_env,
                policies=worker.policy_map,
                episode=episode,
                env_index=env_id,
            )

        # Episode is done for all agents (dones[__all__] == True)
        # or we hit the horizon.
        if all_agents_done:
            is_done = dones[env_id]["__all__"]
            check_dones = is_done and not no_done_at_end

            # If, we are not allowed to pack the next episode into the same
            # SampleBatch (batch_mode=complete_episodes) -> Build the
            # MultiAgentBatch from a single episode and add it to "outputs".
            # Otherwise, just postprocess and continue collecting across
            # episodes.
            # If an episode was marked faulty, perform regular postprocessing
            # (to e.g. properly flush and clean up the SampleCollector's buffers),
            # but then discard the entire batch and don't return it.
            if not episode.is_faulty or episode.length > 0:
                ma_sample_batch = sample_collector.postprocess_episode(
                    episode,
                    is_done=is_done or (hit_horizon and not soft_horizon),
                    check_dones=check_dones,
                    build=episode.is_faulty or not multiple_episodes_in_batch,
                )
            if not episode.is_faulty:
                if ma_sample_batch:
                    outputs.append(ma_sample_batch)

                # Call each (in-memory) policy's Exploration.on_episode_end
                # method.
                # Note: This may break the exploration (e.g. ParameterNoise) of
                # policies in the `policy_map` that have not been recently used
                # (and are therefore stashed to disk). However, we certainly do not
                # want to loop through all (even stashed) policies here as that
                # would counter the purpose of the LRU policy caching.
                for p in worker.policy_map.cache.values():
                    if getattr(p, "exploration", None) is not None:
                        p.exploration.on_episode_end(
                            policy=p,
                            environment=base_env,
                            episode=episode,
                            tf_sess=p.get_session(),
                        )
                # Call custom on_episode_end callback.
                callbacks.on_episode_end(
                    worker=worker,
                    base_env=base_env,
                    policies=worker.policy_map,
                    episode=episode,
                    env_index=env_id,
                )

            # Horizon hit and we have a soft horizon (no hard env reset).
            if not episode.is_faulty and hit_horizon and soft_horizon:
                episode.soft_reset()
                resetted_obs: Dict[EnvID, Dict[AgentID, EnvObsType]] = {
                    env_id: all_agents_obs
                }
            else:
                # Clean up old finished episode.
                del active_episodes[env_id]

                # Create a new episode and call `on_episode_created` callback(s).
                episode = _create_episode(
                    active_episodes, env_id, callbacks, worker, base_env
                )

                # TODO(jungong) : This will allow a single faulty env to
                # take out the entire RolloutWorker indefinitely. Revisit.
                while True:
                    resetted_obs: Optional[
                        Dict[EnvID, Dict[AgentID, EnvObsType]]
                    ] = base_env.try_reset(env_id)
                    if resetted_obs is None or not isinstance(
                        resetted_obs[env_id], Exception
                    ):
                        break
                    else:
                        # Failed to reset, add metrics about a faulty episode.
                        outputs.append(RolloutMetrics(episode_faulty=True))

            # Reset not supported, drop this env from the ready list.
            if resetted_obs is None:
                if horizon != float("inf"):
                    raise ValueError(
                        "Setting episode horizon requires reset() support "
                        "from the environment."
                    )

            # Creates a new episode if this is not async return.
            # If reset is async, we will get its result in some future poll.
            elif resetted_obs != ASYNC_RESET_RETURN:
                new_episode: Episode = active_episodes[env_id]
                _call_on_episode_start(new_episode, env_id, callbacks, worker, base_env)

                _assert_episode_not_faulty(new_episode)
                resetted_obs = resetted_obs[env_id]
                if observation_fn:
                    resetted_obs: Dict[AgentID, EnvObsType] = observation_fn(
                        agent_obs=resetted_obs,
                        worker=worker,
                        base_env=base_env,
                        policies=worker.policy_map,
                        episode=new_episode,
                    )
                # types: AgentID, EnvObsType
                for agent_id, raw_obs in resetted_obs.items():
                    policy_id: PolicyID = new_episode.policy_for(agent_id)
                    preproccessor = _get_or_raise(worker.preprocessors, policy_id)

                    prep_obs: EnvObsType = raw_obs
                    if preproccessor is not None:
                        prep_obs = preproccessor.transform(raw_obs)
                    filtered_obs: EnvObsType = _get_or_raise(worker.filters, policy_id)(
                        prep_obs
                    )
                    new_episode._set_last_raw_obs(agent_id, raw_obs)
                    new_episode._set_last_observation(agent_id, filtered_obs)

                    # Add initial obs to buffer.
                    sample_collector.add_init_obs(
                        new_episode,
                        agent_id,
                        env_id,
                        policy_id,
                        new_episode.length - 1,
                        filtered_obs,
                    )

                    item = _PolicyEvalData(
                        env_id,
                        agent_id,
                        filtered_obs,
                        new_episode.last_info_for(agent_id) or {},
                        new_episode.rnn_state_for(agent_id),
                        None,
                        0.0,
                    )
                    to_eval[policy_id].append(item)

    # Try to build something.
    if multiple_episodes_in_batch:
        sample_batches = (
            sample_collector.try_build_truncated_episode_multi_agent_batch()
        )
        if sample_batches:
            outputs.extend(sample_batches)

    return active_envs, to_eval, outputs


def _do_policy_eval(
    *,
    to_eval: Dict[PolicyID, List[_PolicyEvalData]],
    policies: PolicyMap,
    sample_collector: SampleCollector,
    active_episodes: Dict[EnvID, Episode],
) -> Dict[PolicyID, Tuple[TensorStructType, StateBatch, dict]]:
    """Call compute_actions on collected episode/model data to get next action.

    Args:
        to_eval: Mapping of policy IDs to lists of _PolicyEvalData objects
            (items in these lists will be the batch's items for the model
            forward pass).
        policies: Mapping from policy ID to Policy obj.
        sample_collector: The SampleCollector object to use.
        active_episodes: Mapping of EnvID to its currently active episode.

    Returns:
        Dict mapping PolicyIDs to compute_actions_from_input_dict() outputs.
    """

    eval_results: Dict[PolicyID, TensorStructType] = {}

    if log_once("compute_actions_input"):
        logger.info("Inputs to compute_actions():\n\n{}\n".format(summarize(to_eval)))

    for policy_id, eval_data in to_eval.items():
        # In case the policyID has been removed from this worker, we need to
        # re-assign policy_id and re-lookup the Policy object to use.
        try:
            policy: Policy = _get_or_raise(policies, policy_id)
        except ValueError:
            # Important: Get the policy_mapping_fn from the active
            # Episode as the policy_mapping_fn from the worker may
            # have already been changed (mapping fn stay constant
            # within one episode).
            episode = active_episodes[eval_data[0].env_id]
            _assert_episode_not_faulty(episode)
            policy_id = episode.policy_mapping_fn(
                eval_data[0].agent_id, episode, worker=episode.worker
            )
            policy: Policy = _get_or_raise(policies, policy_id)

        input_dict = sample_collector.get_inference_input_dict(policy_id)
        eval_results[policy_id] = policy.compute_actions_from_input_dict(
            input_dict,
            timestep=policy.global_timestep,
            episodes=[active_episodes[t.env_id] for t in eval_data],
        )

    if log_once("compute_actions_result"):
        logger.info(
            "Outputs of compute_actions():\n\n{}\n".format(summarize(eval_results))
        )

    return eval_results


def _process_policy_eval_results(
    *,
    to_eval: Dict[PolicyID, List[_PolicyEvalData]],
    eval_results: Dict[PolicyID, Tuple[TensorStructType, StateBatch, dict]],
    active_episodes: Dict[EnvID, Episode],
    active_envs: Set[int],
    off_policy_actions: MultiEnvDict,
    policies: Dict[PolicyID, Policy],
    normalize_actions: bool,
    clip_actions: bool,
) -> Dict[EnvID, Dict[AgentID, EnvActionType]]:
    """Process the output of policy neural network evaluation.

    Records policy evaluation results into the given episode objects and
    returns replies to send back to agents in the env.

    Args:
        to_eval: Mapping of policy IDs to lists of _PolicyEvalData objects.
        eval_results: Mapping of policy IDs to list of
            actions, rnn-out states, extra-action-fetches dicts.
        active_episodes: Mapping from episode ID to currently ongoing
            Episode object.
        active_envs: Set of non-terminated env ids.
        off_policy_actions: Doubly keyed dict of env-ids -> agent ids ->
            off-policy-action, returned by a `BaseEnv.poll()` call.
        policies: Mapping from policy ID to Policy.
        normalize_actions: Whether to normalize actions to the action
            space's bounds.
        clip_actions: Whether to clip actions to the action space's bounds.

    Returns:
        Nested dict of env id -> agent id -> actions to be sent to
        Env (np.ndarrays).
    """

    actions_to_send: Dict[EnvID, Dict[AgentID, EnvActionType]] = defaultdict(dict)

    # types: int
    for env_id in active_envs:
        actions_to_send[env_id] = {}  # at minimum send empty dict

    # types: PolicyID, List[_PolicyEvalData]
    for policy_id, eval_data in to_eval.items():
        actions: TensorStructType = eval_results[policy_id][0]
        actions = convert_to_numpy(actions)

        rnn_out_cols: StateBatch = eval_results[policy_id][1]
        extra_action_out_cols: dict = eval_results[policy_id][2]

        # In case actions is a list (representing the 0th dim of a batch of
        # primitive actions), try converting it first.
        if isinstance(actions, list):
            actions = np.array(actions)

        # Store RNN state ins/outs and extra-action fetches to episode.
        for f_i, column in enumerate(rnn_out_cols):
            extra_action_out_cols["state_out_{}".format(f_i)] = column

        policy: Policy = _get_or_raise(policies, policy_id)
        # Split action-component batches into single action rows.
        actions: List[EnvActionType] = unbatch(actions)
        # types: int, EnvActionType
        for i, action in enumerate(actions):
            # Normalize, if necessary.
            if normalize_actions:
                action_to_send = unsquash_action(action, policy.action_space_struct)
            # Clip, if necessary.
            elif clip_actions:
                action_to_send = clip_action(action, policy.action_space_struct)
            else:
                action_to_send = action

            env_id: int = eval_data[i].env_id
            agent_id: AgentID = eval_data[i].agent_id
            episode: Episode = active_episodes[env_id]
            _assert_episode_not_faulty(episode)
            episode._set_rnn_state(agent_id, [c[i] for c in rnn_out_cols])
            episode._set_last_extra_action_outs(
                agent_id, {k: v[i] for k, v in extra_action_out_cols.items()}
            )
            if env_id in off_policy_actions and agent_id in off_policy_actions[env_id]:
                episode._set_last_action(agent_id, off_policy_actions[env_id][agent_id])
            else:
                episode._set_last_action(agent_id, action)

            assert agent_id not in actions_to_send[env_id]
            # Flag actions as immutable to notify the user when trying to change it
            # and to avoid hardly traceable errors.
            tree.traverse(make_action_immutable, action_to_send, top_down=False)
            actions_to_send[env_id][agent_id] = action_to_send

    return actions_to_send


def _create_episode(active_episodes, env_id, callbacks, worker, base_env):
    # Make sure we are really creating a new episode here.
    assert env_id not in active_episodes

    # Create a new episode under the given `env_id` and call the
    # `on_episode_created` callbacks.
    new_episode = active_episodes[env_id]
    # Call `on_episode_created()` callback.
    callbacks.on_episode_created(
        worker=worker,
        base_env=base_env,
        policies=worker.policy_map,
        env_index=env_id,
        episode=new_episode,
    )
    return new_episode


def _call_on_episode_start(episode, env_id, callbacks, worker, base_env):
    # Call each policy's Exploration.on_episode_start method.
    # Note: This may break the exploration (e.g. ParameterNoise) of
    # policies in the `policy_map` that have not been recently used
    # (and are therefore stashed to disk). However, we certainly do not
    # want to loop through all (even stashed) policies here as that
    # would counter the purpose of the LRU policy caching.
    for p in worker.policy_map.cache.values():
        if getattr(p, "exploration", None) is not None:
            p.exploration.on_episode_start(
                policy=p,
                environment=base_env,
                episode=episode,
                tf_sess=p.get_session(),
            )
    callbacks.on_episode_start(
        worker=worker,
        base_env=base_env,
        policies=worker.policy_map,
        episode=episode,
        env_index=env_id,
    )
    episode.started = True


def _to_column_format(rnn_state_rows: List[List[Any]]) -> StateBatch:
    num_cols = len(rnn_state_rows[0])
    return [[row[i] for row in rnn_state_rows] for i in range(num_cols)]


def _assert_episode_not_faulty(episode):
    if episode.is_faulty:
        raise AssertionError(
            "Episodes marked as `faulty` should not be kept in the "
            f"`active_episodes` map! Episode ID={episode.episode_id}."
        )
