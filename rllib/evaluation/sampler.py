import logging
import queue
from abc import ABCMeta, abstractmethod
from collections import defaultdict, namedtuple
from typing import (
    TYPE_CHECKING,
    Any,
    List,
    Optional,
    Type,
    Union,
)

from ray.rllib.env.base_env import BaseEnv, convert_to_base_env
from ray.rllib.evaluation.collectors.sample_collector import SampleCollector
from ray.rllib.evaluation.collectors.simple_list_collector import SimpleListCollector
from ray.rllib.evaluation.env_runner_v2 import EnvRunnerV2, _PerfStats
from ray.rllib.evaluation.metrics import RolloutMetrics
from ray.rllib.offline import InputReader
from ray.rllib.policy.sample_batch import concat_samples
from ray.rllib.utils.annotations import OldAPIStack, override
from ray.rllib.utils.deprecation import deprecation_warning, DEPRECATED_VALUE
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.typing import SampleBatchType
from ray.util.debug import log_once

if TYPE_CHECKING:
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


@OldAPIStack
class SamplerInput(InputReader, metaclass=ABCMeta):
    """Reads input experiences from an existing sampler."""

    @override(InputReader)
    def next(self) -> SampleBatchType:
        batches = [self.get_data()]
        batches.extend(self.get_extra_batches())
        if len(batches) == 0:
            raise RuntimeError("No data available from sampler.")
        return concat_samples(batches)

    @abstractmethod
    def get_data(self) -> SampleBatchType:
        """Called by `self.next()` to return the next batch of data.

        Override this in child classes.

        Returns:
            The next batch of data.
        """
        raise NotImplementedError

    @abstractmethod
    def get_metrics(self) -> List[RolloutMetrics]:
        """Returns list of episode metrics since the last call to this method.

        The list will contain one RolloutMetrics object per completed episode.

        Returns:
            List of RolloutMetrics objects, one per completed episode since
            the last call to this method.
        """
        raise NotImplementedError

    @abstractmethod
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


@OldAPIStack
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
        multiple_episodes_in_batch: bool = False,
        normalize_actions: bool = True,
        clip_actions: bool = False,
        observation_fn: Optional["ObservationFunction"] = None,
        sample_collector_class: Optional[Type[SampleCollector]] = None,
        render: bool = False,
        # Obsolete.
        policies=None,
        policy_mapping_fn=None,
        preprocessors=None,
        obs_filters=None,
        tf_sess=None,
        horizon=DEPRECATED_VALUE,
        soft_horizon=DEPRECATED_VALUE,
        no_done_at_end=DEPRECATED_VALUE,
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
            multiple_episodes_in_batch: Whether to pack multiple
                episodes into each batch. This guarantees batches will be
                exactly `rollout_fragment_length` in size.
            normalize_actions: Whether to normalize actions to the
                action space's bounds.
            clip_actions: Whether to clip actions according to the
                given action_space's bounds.
            observation_fn: Optional multi-agent observation func to use for
                preprocessing observations.
            sample_collector_class: An optional SampleCollector sub-class to
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
            if horizon != DEPRECATED_VALUE:
                deprecation_warning(old="horizon", error=True)
            if soft_horizon != DEPRECATED_VALUE:
                deprecation_warning(old="soft_horizon", error=True)
            if no_done_at_end != DEPRECATED_VALUE:
                deprecation_warning(old="no_done_at_end", error=True)

        self.base_env = convert_to_base_env(env)
        self.rollout_fragment_length = rollout_fragment_length
        self.extra_batches = queue.Queue()
        self.perf_stats = _PerfStats(
            ema_coef=worker.config.sampler_perf_stats_ema_coef,
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

        # Keep a reference to the underlying EnvRunnerV2 instance for
        # unit testing purpose.
        self._env_runner_obj = EnvRunnerV2(
            worker=worker,
            base_env=self.base_env,
            multiple_episodes_in_batch=multiple_episodes_in_batch,
            callbacks=callbacks,
            perf_stats=self.perf_stats,
            rollout_fragment_length=rollout_fragment_length,
            count_steps_by=count_steps_by,
            render=self.render,
        )
        self._env_runner = self._env_runner_obj.run()
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
