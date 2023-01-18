from collections import defaultdict
import logging
import time
import tree  # pip install dm_tree
from typing import TYPE_CHECKING, Dict, Iterator, List, Optional, Set, Tuple, Union
import numpy as np

from ray.rllib.env.base_env import ASYNC_RESET_RETURN, BaseEnv
from ray.rllib.env.external_env import ExternalEnvWrapper
from ray.rllib.env.wrappers.atari_wrappers import MonitorEnv, get_wrapper_by_cls
from ray.rllib.evaluation.collectors.simple_list_collector import _PolicyCollectorGroup
from ray.rllib.evaluation.episode_v2 import EpisodeV2
from ray.rllib.evaluation.metrics import RolloutMetrics
from ray.rllib.models.preprocessors import Preprocessor
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import MultiAgentBatch, SampleBatch, concat_samples
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.filter import Filter
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.spaces.space_utils import unbatch, get_original_space
from ray.rllib.utils.typing import (
    ActionConnectorDataType,
    AgentConnectorDataType,
    AgentID,
    EnvActionType,
    EnvID,
    EnvInfoDict,
    EnvObsType,
    MultiAgentDict,
    MultiEnvDict,
    PolicyID,
    PolicyOutputType,
    SampleBatchType,
    StateBatches,
    TensorStructType,
)
from ray.util.debug import log_once

if TYPE_CHECKING:
    from gymnasium.envs.classic_control.rendering import SimpleImageViewer

    from ray.rllib.algorithms.callbacks import DefaultCallbacks
    from ray.rllib.evaluation.rollout_worker import RolloutWorker


logger = logging.getLogger(__name__)


MIN_LARGE_BATCH_THRESHOLD = 1000
DEFAULT_LARGE_BATCH_THRESHOLD = 5000
MS_TO_SEC = 1000.0


class _PerfStats:
    """Sampler perf stats that will be included in rollout metrics."""

    def __init__(self, ema_coef: Optional[float] = None):
        # If not None, enable Exponential Moving Average mode.
        # The way we update stats is by:
        #     updated = (1 - ema_coef) * old + ema_coef * new
        # In general provides more responsive stats about sampler performance.
        # TODO(jungong) : make ema the default (only) mode if it works well.
        self.ema_coef = ema_coef

        self.iters = 0
        self.raw_obs_processing_time = 0.0
        self.inference_time = 0.0
        self.action_processing_time = 0.0
        self.env_wait_time = 0.0
        self.env_render_time = 0.0

    def incr(self, field: str, value: Union[int, float]):
        if field == "iters":
            self.iters += value
            return

        # All the other fields support either global average or ema mode.
        if self.ema_coef is None:
            # Global average.
            self.__dict__[field] += value
        else:
            self.__dict__[field] = (1.0 - self.ema_coef) * self.__dict__[
                field
            ] + self.ema_coef * value

    def _get_avg(self):
        # Mean multiplicator (1000 = sec -> ms).
        factor = MS_TO_SEC / self.iters
        return {
            # Raw observation preprocessing.
            "mean_raw_obs_processing_ms": self.raw_obs_processing_time * factor,
            # Computing actions through policy.
            "mean_inference_ms": self.inference_time * factor,
            # Processing actions (to be sent to env, e.g. clipping).
            "mean_action_processing_ms": self.action_processing_time * factor,
            # Waiting for environment (during poll).
            "mean_env_wait_ms": self.env_wait_time * factor,
            # Environment rendering (False by default).
            "mean_env_render_ms": self.env_render_time * factor,
        }

    def _get_ema(self):
        # In EMA mode, stats are already (exponentially) averaged,
        # hence we only need to do the sec -> ms conversion here.
        return {
            # Raw observation preprocessing.
            "mean_raw_obs_processing_ms": self.raw_obs_processing_time * MS_TO_SEC,
            # Computing actions through policy.
            "mean_inference_ms": self.inference_time * MS_TO_SEC,
            # Processing actions (to be sent to env, e.g. clipping).
            "mean_action_processing_ms": self.action_processing_time * MS_TO_SEC,
            # Waiting for environment (during poll).
            "mean_env_wait_ms": self.env_wait_time * MS_TO_SEC,
            # Environment rendering (False by default).
            "mean_env_render_ms": self.env_render_time * MS_TO_SEC,
        }

    def get(self):
        if self.ema_coef is None:
            return self._get_avg()
        else:
            return self._get_ema()


class _NewDefaultDict(defaultdict):
    def __missing__(self, env_id):
        ret = self[env_id] = self.default_factory(env_id)
        return ret


def _build_multi_agent_batch(
    episode_id: int,
    batch_builder: _PolicyCollectorGroup,
    large_batch_threshold: int,
    multiple_episodes_in_batch: bool,
) -> MultiAgentBatch:
    """Build MultiAgentBatch from a dict of _PolicyCollectors.

    Args:
        env_steps: total env steps.
        policy_collectors: collected training SampleBatchs by policy.

    Returns:
        Always returns a sample batch in MultiAgentBatch format.
    """
    ma_batch = {}
    for pid, collector in batch_builder.policy_collectors.items():
        if collector.agent_steps <= 0:
            continue

        if batch_builder.agent_steps > large_batch_threshold and log_once(
            "large_batch_warning"
        ):
            logger.warning(
                "More than {} observations in {} env steps for "
                "episode {} ".format(
                    batch_builder.agent_steps, batch_builder.env_steps, episode_id
                )
                + "are buffered in the sampler. If this is more than you "
                "expected, check that that you set a horizon on your "
                "environment correctly and that it terminates at some "
                "point. Note: In multi-agent environments, "
                "`rollout_fragment_length` sets the batch size based on "
                "(across-agents) environment steps, not the steps of "
                "individual agents, which can result in unexpectedly "
                "large batches."
                + (
                    "Also, you may be waiting for your Env to "
                    "terminate (batch_mode=`complete_episodes`). Make sure "
                    "it does at some point."
                    if not multiple_episodes_in_batch
                    else ""
                )
            )

        ma_batch[pid] = collector.build()

    # Create the multi agent batch.
    return MultiAgentBatch(policy_batches=ma_batch, env_steps=batch_builder.env_steps)


def _batch_inference_sample_batches(eval_data: List[SampleBatch]) -> SampleBatch:
    """Batch a list of input SampleBatches into a single SampleBatch.

    Args:
        eval_data: list of SampleBatches.

    Returns:
        single batched SampleBatch.
    """
    inference_batch = concat_samples(eval_data)
    if "state_in_0" in inference_batch:
        batch_size = len(eval_data)
        inference_batch[SampleBatch.SEQ_LENS] = np.ones(batch_size, dtype=np.int32)
    return inference_batch


@DeveloperAPI
class EnvRunnerV2:
    """Collect experiences from user environment using Connectors."""

    def __init__(
        self,
        worker: "RolloutWorker",
        base_env: BaseEnv,
        multiple_episodes_in_batch: bool,
        callbacks: "DefaultCallbacks",
        perf_stats: _PerfStats,
        rollout_fragment_length: int = 200,
        count_steps_by: str = "env_steps",
        render: bool = None,
    ):
        """
        Args:
            worker: Reference to the current rollout worker.
            base_env: Env implementing BaseEnv.
            multiple_episodes_in_batch: Whether to pack multiple
                episodes into each batch. This guarantees batches will be exactly
                `rollout_fragment_length` in size.
            callbacks: User callbacks to run on episode events.
            perf_stats: Record perf stats into this object.
            rollout_fragment_length: The length of a fragment to collect
                before building a SampleBatch from the data and resetting
                the SampleBatchBuilder object.
            count_steps_by: One of "env_steps" (default) or "agent_steps".
                Use "agent_steps", if you want rollout lengths to be counted
                by individual agent steps. In a multi-agent env,
                a single env_step contains one or more agent_steps, depending
                on how many agents are present at any given time in the
                ongoing episode.
            render: Whether to try to render the environment after each
                step.
        """
        self._worker = worker
        if isinstance(base_env, ExternalEnvWrapper):
            raise ValueError(
                "Policies using the new Connector API do not support ExternalEnv."
            )
        self._base_env = base_env
        self._multiple_episodes_in_batch = multiple_episodes_in_batch
        self._callbacks = callbacks
        self._perf_stats = perf_stats
        self._rollout_fragment_length = rollout_fragment_length
        self._count_steps_by = count_steps_by
        self._render = render

        # May be populated for image rendering.
        self._simple_image_viewer: Optional[
            "SimpleImageViewer"
        ] = self._get_simple_image_viewer()

        # Keeps track of active episodes.
        self._active_episodes: Dict[EnvID, EpisodeV2] = {}
        self._batch_builders: Dict[EnvID, _PolicyCollectorGroup] = _NewDefaultDict(
            self._new_batch_builder
        )

        self._large_batch_threshold: int = (
            max(MIN_LARGE_BATCH_THRESHOLD, self._rollout_fragment_length * 10)
            if self._rollout_fragment_length != float("inf")
            else DEFAULT_LARGE_BATCH_THRESHOLD
        )

    def _get_simple_image_viewer(self):
        """Maybe construct a SimpleImageViewer instance for episode rendering."""
        # Try to render the env, if required.
        if not self._render:
            return None

        try:
            from gymnasium.envs.classic_control.rendering import SimpleImageViewer

            return SimpleImageViewer()
        except (ImportError, ModuleNotFoundError):
            self._render = False  # disable rendering
            logger.warning(
                "Could not import gymnasium.envs.classic_control."
                "rendering! Try `pip install gymnasium[all]`."
            )

        return None

    def _call_on_episode_start(self, episode, env_id):
        # Call each policy's Exploration.on_episode_start method.
        # Note: This may break the exploration (e.g. ParameterNoise) of
        # policies in the `policy_map` that have not been recently used
        # (and are therefore stashed to disk). However, we certainly do not
        # want to loop through all (even stashed) policies here as that
        # would counter the purpose of the LRU policy caching.
        for p in self._worker.policy_map.cache.values():
            if getattr(p, "exploration", None) is not None:
                p.exploration.on_episode_start(
                    policy=p,
                    environment=self._base_env,
                    episode=episode,
                    tf_sess=p.get_session(),
                )
        # Call `on_episode_start()` callback.
        self._callbacks.on_episode_start(
            worker=self._worker,
            base_env=self._base_env,
            policies=self._worker.policy_map,
            env_index=env_id,
            episode=episode,
        )

    def _new_batch_builder(self, _) -> _PolicyCollectorGroup:
        """Create a new batch builder.

        We create a _PolicyCollectorGroup based on the full policy_map
        as the batch builder.
        """
        return _PolicyCollectorGroup(self._worker.policy_map)

    def run(self) -> Iterator[SampleBatchType]:
        """Samples and yields training episodes continuously.

        Yields:
            Object containing state, action, reward, terminal condition,
            and other fields as dictated by `policy`.
        """
        while True:
            outputs = self.step()
            for o in outputs:
                yield o

    def step(self) -> List[SampleBatchType]:
        """Samples training episodes by stepping through environments."""

        self._perf_stats.incr("iters", 1)

        t0 = time.time()
        # Get observations from all ready agents.
        # types: MultiEnvDict, MultiEnvDict, MultiEnvDict, MultiEnvDict, ...
        (
            unfiltered_obs,
            rewards,
            terminateds,
            truncateds,
            infos,
            off_policy_actions,
        ) = self._base_env.poll()
        env_poll_time = time.time() - t0

        # Process observations and prepare for policy evaluation.
        t1 = time.time()
        # types: Set[EnvID], Dict[PolicyID, List[AgentConnectorDataType]],
        #       List[Union[RolloutMetrics, SampleBatchType]]
        active_envs, to_eval, outputs = self._process_observations(
            unfiltered_obs=unfiltered_obs,
            rewards=rewards,
            terminateds=terminateds,
            truncateds=truncateds,
            infos=infos,
        )
        self._perf_stats.incr("raw_obs_processing_time", time.time() - t1)

        # Do batched policy eval (accross vectorized envs).
        t2 = time.time()
        # types: Dict[PolicyID, Tuple[TensorStructType, StateBatch, dict]]
        eval_results = self._do_policy_eval(to_eval=to_eval)
        self._perf_stats.incr("inference_time", time.time() - t2)

        # Process results and update episode state.
        t3 = time.time()
        actions_to_send: Dict[
            EnvID, Dict[AgentID, EnvActionType]
        ] = self._process_policy_eval_results(
            active_envs=active_envs,
            to_eval=to_eval,
            eval_results=eval_results,
            off_policy_actions=off_policy_actions,
        )
        self._perf_stats.incr("action_processing_time", time.time() - t3)

        # Return computed actions to ready envs. We also send to envs that have
        # taken off-policy actions; those envs are free to ignore the action.
        t4 = time.time()
        self._base_env.send_actions(actions_to_send)
        self._perf_stats.incr("env_wait_time", env_poll_time + time.time() - t4)

        self._maybe_render()

        return outputs

    def _get_rollout_metrics(
        self, episode: EpisodeV2, policy_map: Dict[str, Policy]
    ) -> List[RolloutMetrics]:
        """Get rollout metrics from completed episode."""
        # TODO(jungong) : why do we need to handle atari metrics differently?
        # Can we unify atari and normal env metrics?
        atari_metrics: List[RolloutMetrics] = _fetch_atari_metrics(self._base_env)
        if atari_metrics is not None:
            for m in atari_metrics:
                m._replace(custom_metrics=episode.custom_metrics)
            return atari_metrics
        # Create connector metrics
        connector_metrics = {}
        active_agents = episode.get_agents()
        for agent in active_agents:
            policy_id = episode.policy_for(agent)
            policy = episode.policy_map[policy_id]
            connector_metrics[policy_id] = policy.get_connector_throughput_metrics()
        # Otherwise, return RolloutMetrics for the episode.
        return [
            RolloutMetrics(
                episode_length=episode.length,
                episode_reward=episode.total_reward,
                agent_rewards=dict(episode.agent_rewards),
                custom_metrics=episode.custom_metrics,
                perf_stats={},
                hist_data=episode.hist_data,
                media=episode.media,
                connector_metrics=connector_metrics,
            )
        ]

    def _process_observations(
        self,
        unfiltered_obs: MultiEnvDict,
        rewards: MultiEnvDict,
        terminateds: MultiEnvDict,
        truncateds: MultiEnvDict,
        infos: MultiEnvDict,
    ) -> Tuple[
        Set[EnvID],
        Dict[PolicyID, List[AgentConnectorDataType]],
        List[Union[RolloutMetrics, SampleBatchType]],
    ]:
        """Process raw obs from env.

        Group data for active agents by policy. Reset environments that are done.

        Args:
            unfiltered_obs: The unfiltered, raw observations from the BaseEnv
                (vectorized, possibly multi-agent). Dict of dict: By env index,
                then agent ID, then mapped to actual obs.
            rewards: The rewards MultiEnvDict of the BaseEnv.
            terminateds: The `terminated` flags MultiEnvDict of the BaseEnv.
            truncateds: The `truncated` flags MultiEnvDict of the BaseEnv.
            infos: The MultiEnvDict of infos dicts of the BaseEnv.

        Returns:
            A tuple of:
                A list of envs that were active during this step.
                AgentConnectorDataType for active agents for policy evaluation.
                SampleBatches and RolloutMetrics for completed agents for output.
        """
        # Output objects.
        # Note that we need to track envs that are active during this round explicitly,
        # just to be confident which envs require us to send at least an empty action
        # dict to.
        # We can not get this from the _active_episode or to_eval lists because
        # 1. All envs are not required to step during every single step. And
        # 2. to_eval only contains data for the agents that are still active. An env may
        # be active but all agents are done during the step.
        active_envs: Set[EnvID] = set()
        to_eval: Dict[PolicyID, List[AgentConnectorDataType]] = defaultdict(list)
        outputs: List[Union[RolloutMetrics, SampleBatchType]] = []

        # For each (vectorized) sub-environment.
        # types: EnvID, Dict[AgentID, EnvObsType]
        for env_id, env_obs in unfiltered_obs.items():
            # Check for env_id having returned an error instead of a multi-agent
            # obs dict. This is how our BaseEnv can tell the caller to `poll()` that
            # one of its sub-environments is faulty and should be restarted (and the
            # ongoing episode should not be used for training).
            if isinstance(env_obs, Exception):
                assert terminateds[env_id]["__all__"] is True, (
                    f"ERROR: When a sub-environment (env-id {env_id}) returns an error "
                    "as observation, the terminateds[__all__] flag must also be set to "
                    "True!"
                )
                # all_agents_obs is an Exception here.
                # Drop this episode and skip to next.
                self._handle_done_episode(
                    env_id=env_id,
                    env_obs_or_exception=env_obs,
                    is_done=True,
                    active_envs=active_envs,
                    to_eval=to_eval,
                    outputs=outputs,
                )
                continue

            if env_id not in self._active_episodes:
                episode: EpisodeV2 = self.create_episode(env_id)
                self._active_episodes[env_id] = episode
            else:
                episode: EpisodeV2 = self._active_episodes[env_id]
            # If this episode is brand-new, call the episode start callback(s).
            # Note: EpisodeV2s are initialized with length=-1 (before the reset).
            if not episode.has_init_obs():
                self._call_on_episode_start(episode, env_id)

            # Check episode termination conditions.
            if terminateds[env_id]["__all__"] or truncateds[env_id]["__all__"]:
                all_agents_done = True
            else:
                all_agents_done = False
                active_envs.add(env_id)

            # Special handling of common info dict.
            episode.set_last_info("__common__", infos[env_id].get("__common__", {}))

            # Agent sample batches grouped by policy. Each set of sample batches will
            # go through agent connectors together.
            sample_batches_by_policy = defaultdict(list)
            # Whether an agent is terminated or truncated.
            agent_terminateds = {}
            agent_truncateds = {}
            for agent_id, obs in env_obs.items():
                assert agent_id != "__all__"

                policy_id: PolicyID = episode.policy_for(agent_id)

                agent_terminated = bool(
                    terminateds[env_id]["__all__"] or terminateds[env_id].get(agent_id)
                )
                agent_terminateds[agent_id] = agent_terminated
                agent_truncated = bool(
                    truncateds[env_id]["__all__"]
                    or truncateds[env_id].get(agent_id, False)
                )
                agent_truncateds[agent_id] = agent_truncated

                # A completely new agent is already done -> Skip entirely.
                if not episode.has_init_obs(agent_id) and (
                    agent_terminated or agent_truncated
                ):
                    continue

                values_dict = {
                    SampleBatch.T: episode.length,  # Episodes start at -1 before we
                    # add the initial obs. After that, we infer from initial obs at
                    # t=0 since that will be our new episode.length.
                    SampleBatch.ENV_ID: env_id,
                    SampleBatch.AGENT_INDEX: episode.agent_index(agent_id),
                    # Last action (SampleBatch.ACTIONS) column will be populated by
                    # StateBufferConnector.
                    # Reward received after taking action at timestep t.
                    SampleBatch.REWARDS: rewards[env_id].get(agent_id, 0.0),
                    # After taking action=a, did we reach terminal?
                    SampleBatch.TERMINATEDS: agent_terminated,
                    # Was the episode truncated artificially
                    # (e.g. b/c of some time limit)?
                    SampleBatch.TRUNCATEDS: agent_truncated,
                    SampleBatch.INFOS: infos[env_id].get(agent_id, {}),
                    SampleBatch.NEXT_OBS: obs,
                }

                # Queue this obs sample for connector preprocessing.
                sample_batches_by_policy[policy_id].append((agent_id, values_dict))

            # The entire episode is done.
            if all_agents_done:
                # Let's check to see if there are any agents that haven't got the
                # last obs yet. If there are, we have to create fake-last
                # observations for them. (the environment is not required to do so if
                # terminateds[__all__]==True or truncateds[__all__]==True).
                for agent_id in episode.get_agents():
                    # If the latest obs we got for this agent is done, or if its
                    # episode state is already done, nothing to do.
                    if (
                        agent_terminateds.get(agent_id, False)
                        or agent_truncateds.get(agent_id, False)
                        or episode.is_done(agent_id)
                    ):
                        continue

                    policy_id: PolicyID = episode.policy_for(agent_id)
                    policy = self._worker.policy_map[policy_id]

                    # Create a fake observation by sampling the original env
                    # observation space.
                    obs_space = get_original_space(policy.observation_space)
                    # Although there is no obs for this agent, there may be
                    # good rewards and info dicts for it.
                    # This is the case for e.g. OpenSpiel games, where a reward
                    # is only earned with the last step, but the obs for that
                    # step is {}.
                    reward = rewards[env_id].get(agent_id, 0.0)
                    info = infos[env_id].get(agent_id, {})
                    values_dict = {
                        SampleBatch.T: episode.length,
                        SampleBatch.ENV_ID: env_id,
                        SampleBatch.AGENT_INDEX: episode.agent_index(agent_id),
                        # TODO(sven): These should be the summed-up(!) rewards since the
                        #  last observation received for this agent.
                        SampleBatch.REWARDS: reward,
                        SampleBatch.TERMINATEDS: True,
                        SampleBatch.TRUNCATEDS: truncateds[env_id].get(agent_id, False),
                        SampleBatch.INFOS: info,
                        SampleBatch.NEXT_OBS: obs_space.sample(),
                    }

                    # Queue these fake obs for connector preprocessing too.
                    sample_batches_by_policy[policy_id].append((agent_id, values_dict))

            # Run agent connectors.
            for policy_id, batches in sample_batches_by_policy.items():
                policy: Policy = self._worker.policy_map[policy_id]
                # Collected full MultiAgentDicts for this environment.
                # Run agent connectors.
                assert (
                    policy.agent_connectors
                ), "EnvRunnerV2 requires agent connectors to work."

                acd_list: List[AgentConnectorDataType] = [
                    AgentConnectorDataType(env_id, agent_id, data)
                    for agent_id, data in batches
                ]

                # For all agents mapped to policy_id, run their data
                # through agent_connectors.
                processed = policy.agent_connectors(acd_list)

                for d in processed:
                    # Record transition info if applicable.
                    if not episode.has_init_obs(d.agent_id):
                        episode.add_init_obs(
                            agent_id=d.agent_id,
                            init_obs=d.data.raw_dict[SampleBatch.NEXT_OBS],
                            init_infos=d.data.raw_dict[SampleBatch.INFOS],
                            t=d.data.raw_dict[SampleBatch.T],
                        )
                    else:
                        episode.add_action_reward_done_next_obs(
                            d.agent_id, d.data.raw_dict
                        )

                    # Need to evaluate next actions.
                    if not (
                        all_agents_done
                        or agent_terminateds.get(d.agent_id, False)
                        or agent_truncateds.get(d.agent_id, False)
                        or episode.is_done(d.agent_id)
                    ):
                        # Add to eval set if env is not done and this particular agent
                        # is also not done.
                        item = AgentConnectorDataType(d.env_id, d.agent_id, d.data)
                        to_eval[policy_id].append(item)

            # Finished advancing episode by 1 step, mark it so.
            episode.step()

            # Exception: The very first env.poll() call causes the env to get reset
            # (no step taken yet, just a single starting observation logged).
            # We need to skip this callback in this case.
            if episode.length > 0:
                # Invoke the `on_episode_step` callback after the step is logged
                # to the episode.
                self._callbacks.on_episode_step(
                    worker=self._worker,
                    base_env=self._base_env,
                    policies=self._worker.policy_map,
                    episode=episode,
                    env_index=env_id,
                )

            # Episode is terminated/truncated for all agents
            # (terminateds[__all__] == True or truncateds[__all__] == True).
            if all_agents_done:
                # _handle_done_episode will build a MultiAgentBatch for all
                # the agents that are done during this step of rollout in
                # the case of _multiple_episodes_in_batch=False.
                self._handle_done_episode(
                    env_id,
                    env_obs,
                    terminateds[env_id]["__all__"] or truncateds[env_id]["__all__"],
                    active_envs,
                    to_eval,
                    outputs,
                )

            # Try to build something.
            if self._multiple_episodes_in_batch:
                sample_batch = self._try_build_truncated_episode_multi_agent_batch(
                    self._batch_builders[env_id], episode
                )
                if sample_batch:
                    outputs.append(sample_batch)

                    # SampleBatch built from data collected by batch_builder.
                    # Clean up and delete the batch_builder.
                    del self._batch_builders[env_id]

        return active_envs, to_eval, outputs

    def _build_done_episode(
        self,
        env_id: EnvID,
        is_done: bool,
        outputs: List[SampleBatchType],
    ):
        """Builds a MultiAgentSampleBatch from the episode and adds it to outputs.

        Args:
            env_id: The env id.
            is_done: Whether the env is done.
            outputs: The list of outputs to add the
        """
        episode: EpisodeV2 = self._active_episodes[env_id]
        batch_builder = self._batch_builders[env_id]

        episode.postprocess_episode(
            batch_builder=batch_builder,
            is_done=is_done,
            check_dones=is_done,
        )

        # If, we are not allowed to pack the next episode into the same
        # SampleBatch (batch_mode=complete_episodes) -> Build the
        # MultiAgentBatch from a single episode and add it to "outputs".
        # Otherwise, just postprocess and continue collecting across
        # episodes.
        if not self._multiple_episodes_in_batch:
            ma_sample_batch = _build_multi_agent_batch(
                episode.episode_id,
                batch_builder,
                self._large_batch_threshold,
                self._multiple_episodes_in_batch,
            )
            if ma_sample_batch:
                outputs.append(ma_sample_batch)

            # SampleBatch built from data collected by batch_builder.
            # Clean up and delete the batch_builder.
            del self._batch_builders[env_id]

    def __process_resetted_obs_for_eval(
        self,
        env_id: EnvID,
        obs: Dict[EnvID, Dict[AgentID, EnvObsType]],
        infos: Dict[EnvID, Dict[AgentID, EnvInfoDict]],
        episode: EpisodeV2,
        to_eval: Dict[PolicyID, List[AgentConnectorDataType]],
    ):
        """Process resetted obs through agent connectors for policy eval.

        Args:
            env_id: The env id.
            obs: The Resetted obs.
            episode: New episode.
            to_eval: List of agent connector data for policy eval.
        """
        per_policy_resetted_obs: Dict[PolicyID, List] = defaultdict(list)
        # types: AgentID, EnvObsType
        for agent_id, raw_obs in obs[env_id].items():
            policy_id: PolicyID = episode.policy_for(agent_id)
            per_policy_resetted_obs[policy_id].append((agent_id, raw_obs))

        for policy_id, agents_obs in per_policy_resetted_obs.items():
            policy = self._worker.policy_map[policy_id]
            acd_list: List[AgentConnectorDataType] = [
                AgentConnectorDataType(
                    env_id,
                    agent_id,
                    {
                        SampleBatch.NEXT_OBS: obs,
                        SampleBatch.INFOS: infos,
                        SampleBatch.T: episode.length,
                    },
                )
                for agent_id, obs in agents_obs
            ]
            # Call agent connectors on these initial obs.
            processed = policy.agent_connectors(acd_list)

            for d in processed:
                episode.add_init_obs(
                    agent_id=d.agent_id,
                    init_obs=d.data.raw_dict[SampleBatch.NEXT_OBS],
                    init_infos=d.data.raw_dict[SampleBatch.INFOS],
                    t=d.data.raw_dict[SampleBatch.T],
                )
                to_eval[policy_id].append(d)

    def _handle_done_episode(
        self,
        env_id: EnvID,
        env_obs_or_exception: MultiAgentDict,
        is_done: bool,
        active_envs: Set[EnvID],
        to_eval: Dict[PolicyID, List[AgentConnectorDataType]],
        outputs: List[SampleBatchType],
    ) -> None:
        """Handle an all-finished episode.

        Add collected SampleBatch to batch builder. Reset corresponding env, etc.

        Args:
            env_id: Environment ID.
            env_obs_or_exception: Last per-environment observation or Exception.
            env_infos: Last per-environment infos.
            is_done: If all agents are done.
            active_envs: Set of active env ids.
            to_eval: Output container for policy eval data.
            outputs: Output container for collected sample batches.
        """
        if isinstance(env_obs_or_exception, Exception):
            episode_or_exception: Exception = env_obs_or_exception
            # Tell the sampler we have got a faulty episode.
            outputs.append(RolloutMetrics(episode_faulty=True))
        else:
            episode_or_exception: EpisodeV2 = self._active_episodes[env_id]
            # Add rollout metrics.
            outputs.extend(
                self._get_rollout_metrics(
                    episode_or_exception, policy_map=self._worker.policy_map
                )
            )
            # Output the collected episode after adding rollout metrics so that we
            # always fetch metrics with RolloutWorker before we fetch samples.
            # This is because we need to behave like env_runner() for now.
            self._build_done_episode(env_id, is_done, outputs)

        # Clean up and deleted the post-processed episode now that we have collected
        # its data.
        self.end_episode(env_id, episode_or_exception)
        # Create a new episode instance (before we reset the sub-environment).
        new_episode: EpisodeV2 = self.create_episode(env_id)

        # The sub environment at index `env_id` might throw an exception
        # during the following `try_reset()` attempt. If configured with
        # `restart_failed_sub_environments=True`, the BaseEnv will restart
        # the affected sub environment (create a new one using its c'tor) and
        # must reset the recreated sub env right after that.
        # Should the sub environment fail indefinitely during these
        # repeated reset attempts, the entire worker will be blocked.
        # This would be ok, b/c the alternative would be the worker crashing
        # entirely.
        while True:
            resetted_obs, resetted_infos = self._base_env.try_reset(env_id)

            if resetted_obs is None or not isinstance(resetted_obs[env_id], Exception):
                break
            else:
                # Report a faulty episode.
                outputs.append(RolloutMetrics(episode_faulty=True))

        # Reset connector state if this is a hard reset.
        for p in self._worker.policy_map.cache.values():
            p.agent_connectors.reset(env_id)

        # Creates a new episode if this is not async return.
        # If reset is async, we will get its result in some future poll.
        if resetted_obs is not None and resetted_obs != ASYNC_RESET_RETURN:
            self._active_episodes[env_id] = new_episode
            self._call_on_episode_start(new_episode, env_id)

            self.__process_resetted_obs_for_eval(
                env_id,
                resetted_obs,
                resetted_infos,
                new_episode,
                to_eval,
            )

            # Step after adding initial obs. This will give us 0 env and agent step.
            new_episode.step()
            active_envs.add(env_id)

    def create_episode(self, env_id: EnvID) -> EpisodeV2:
        """Creates a new EpisodeV2 instance and returns it.

        Calls `on_episode_created` callbacks, but does NOT reset the respective
        sub-environment yet.

        Args:
            env_id: Env ID.

        Returns:
            The newly created EpisodeV2 instance.
        """
        # Make sure we currently don't have an active episode under this env ID.
        assert env_id not in self._active_episodes

        # Create a new episode under the same `env_id` and call the
        # `on_episode_created` callbacks.
        new_episode = EpisodeV2(
            env_id,
            self._worker.policy_map,
            self._worker.policy_mapping_fn,
            worker=self._worker,
            callbacks=self._callbacks,
        )

        # Call `on_episode_created()` callback.
        self._callbacks.on_episode_created(
            worker=self._worker,
            base_env=self._base_env,
            policies=self._worker.policy_map,
            env_index=env_id,
            episode=new_episode,
        )
        return new_episode

    def end_episode(
        self, env_id: EnvID, episode_or_exception: Union[EpisodeV2, Exception]
    ):
        """Cleans up an episode that has finished.

        Args:
            env_id: Env ID.
            episode_or_exception: Instance of an episode if it finished successfully.
                Otherwise, the exception that was thrown,
        """
        # Signal the end of an episode, either successfully with an Episode or
        # unsuccessfully with an Exception.
        self._callbacks.on_episode_end(
            worker=self._worker,
            base_env=self._base_env,
            policies=self._worker.policy_map,
            episode=episode_or_exception,
            env_index=env_id,
        )

        # Call each (in-memory) policy's Exploration.on_episode_end
        # method.
        # Note: This may break the exploration (e.g. ParameterNoise) of
        # policies in the `policy_map` that have not been recently used
        # (and are therefore stashed to disk). However, we certainly do not
        # want to loop through all (even stashed) policies here as that
        # would counter the purpose of the LRU policy caching.
        for p in self._worker.policy_map.cache.values():
            if getattr(p, "exploration", None) is not None:
                p.exploration.on_episode_end(
                    policy=p,
                    environment=self._base_env,
                    episode=episode_or_exception,
                    tf_sess=p.get_session(),
                )

        if isinstance(episode_or_exception, EpisodeV2):
            episode = episode_or_exception
            if episode.total_agent_steps == 0:
                # if the key does not exist it means that throughout the episode all
                # observations were empty (i.e. there was no agent in the env)
                msg = (
                    f"Data from episode {episode.episode_id} does not show any agent "
                    f"interactions. Hint: Make sure for at least one timestep in the "
                    f"episode, env.step() returns non-empty values."
                )
                raise ValueError(msg)

        # Clean up the episode and batch_builder for this env id.
        if env_id in self._active_episodes:
            del self._active_episodes[env_id]

    def _try_build_truncated_episode_multi_agent_batch(
        self, batch_builder: _PolicyCollectorGroup, episode: EpisodeV2
    ) -> Union[None, SampleBatch, MultiAgentBatch]:
        # Measure batch size in env-steps.
        if self._count_steps_by == "env_steps":
            built_steps = batch_builder.env_steps
            ongoing_steps = episode.active_env_steps
        # Measure batch-size in agent-steps.
        else:
            built_steps = batch_builder.agent_steps
            ongoing_steps = episode.active_agent_steps

        # Reached the fragment-len -> We should build an MA-Batch.
        if built_steps + ongoing_steps >= self._rollout_fragment_length:
            if self._count_steps_by != "agent_steps":
                assert built_steps + ongoing_steps == self._rollout_fragment_length, (
                    f"built_steps ({built_steps}) + ongoing_steps ({ongoing_steps}) != "
                    f"rollout_fragment_length ({self._rollout_fragment_length})."
                )

            # If we reached the fragment-len only because of `episode_id`
            # (still ongoing) -> postprocess `episode_id` first.
            if built_steps < self._rollout_fragment_length:
                episode.postprocess_episode(batch_builder=batch_builder, is_done=False)

            # If builder has collected some data,
            # build the MA-batch and add to return values.
            if batch_builder.agent_steps > 0:
                return _build_multi_agent_batch(
                    episode.episode_id,
                    batch_builder,
                    self._large_batch_threshold,
                    self._multiple_episodes_in_batch,
                )
            # No batch-builder:
            # We have reached the rollout-fragment length w/o any agent
            # steps! Warn that the environment may never request any
            # actions from any agents.
            elif log_once("no_agent_steps"):
                logger.warning(
                    "Your environment seems to be stepping w/o ever "
                    "emitting agent observations (agents are never "
                    "requested to act)!"
                )

        return None

    def _do_policy_eval(
        self,
        to_eval: Dict[PolicyID, List[AgentConnectorDataType]],
    ) -> Dict[PolicyID, PolicyOutputType]:
        """Call compute_actions on collected episode data to get next action.

        Args:
            to_eval: Mapping of policy IDs to lists of AgentConnectorDataType objects
                (items in these lists will be the batch's items for the model
                forward pass).

        Returns:
            Dict mapping PolicyIDs to compute_actions_from_input_dict() outputs.
        """
        policies = self._worker.policy_map

        # In case policy map has changed, try to find the new policy that
        # should handle all these per-agent eval data.
        # Throws exception if these agents are mapped to multiple different
        # policies now.
        def _try_find_policy_again(eval_data: AgentConnectorDataType):
            policy_id = None
            for d in eval_data:
                episode = self._active_episodes[d.env_id]
                # Force refresh policy mapping on the episode.
                pid = episode.policy_for(d.agent_id, refresh=True)
                if policy_id is not None and pid != policy_id:
                    raise ValueError(
                        "Policy map changed. The list of eval data that was handled "
                        f"by a same policy is now handled by policy {pid} "
                        "and {policy_id}. "
                        "Please don't do this in the middle of an episode."
                    )
                policy_id = pid
            return _get_or_raise(self._worker.policy_map, policy_id)

        eval_results: Dict[PolicyID, TensorStructType] = {}
        for policy_id, eval_data in to_eval.items():
            # In case the policyID has been removed from this worker, we need to
            # re-assign policy_id and re-lookup the Policy object to use.
            try:
                policy: Policy = _get_or_raise(policies, policy_id)
            except ValueError:
                # policy_mapping_fn from the worker may have already been
                # changed (mapping fn not staying constant within one episode).
                policy: Policy = _try_find_policy_again(eval_data)

            input_dict = _batch_inference_sample_batches(
                [d.data.sample_batch for d in eval_data]
            )

            eval_results[policy_id] = policy.compute_actions_from_input_dict(
                input_dict,
                timestep=policy.global_timestep,
                episodes=[self._active_episodes[t.env_id] for t in eval_data],
            )

        return eval_results

    def _process_policy_eval_results(
        self,
        active_envs: Set[EnvID],
        to_eval: Dict[PolicyID, List[AgentConnectorDataType]],
        eval_results: Dict[PolicyID, PolicyOutputType],
        off_policy_actions: MultiEnvDict,
    ):
        """Process the output of policy neural network evaluation.

        Records policy evaluation results into agent connectors and
        returns replies to send back to agents in the env.

        Args:
            active_envs: Set of env IDs that are still active.
            to_eval: Mapping of policy IDs to lists of AgentConnectorDataType objects.
            eval_results: Mapping of policy IDs to list of
                actions, rnn-out states, extra-action-fetches dicts.
            off_policy_actions: Doubly keyed dict of env-ids -> agent ids ->
                off-policy-action, returned by a `BaseEnv.poll()` call.

        Returns:
            Nested dict of env id -> agent id -> actions to be sent to
            Env (np.ndarrays).
        """
        actions_to_send: Dict[EnvID, Dict[AgentID, EnvActionType]] = defaultdict(dict)

        for env_id in active_envs:
            actions_to_send[env_id] = {}  # at minimum send empty dict

        # types: PolicyID, List[AgentConnectorDataType]
        for policy_id, eval_data in to_eval.items():
            actions: TensorStructType = eval_results[policy_id][0]
            actions = convert_to_numpy(actions)

            rnn_out: StateBatches = eval_results[policy_id][1]
            extra_action_out: dict = eval_results[policy_id][2]

            # In case actions is a list (representing the 0th dim of a batch of
            # primitive actions), try converting it first.
            if isinstance(actions, list):
                actions = np.array(actions)
            # Split action-component batches into single action rows.
            actions: List[EnvActionType] = unbatch(actions)

            policy: Policy = _get_or_raise(self._worker.policy_map, policy_id)
            assert (
                policy.agent_connectors and policy.action_connectors
            ), "EnvRunnerV2 requires action connectors to work."

            # types: int, EnvActionType
            for i, action in enumerate(actions):
                env_id: int = eval_data[i].env_id
                agent_id: AgentID = eval_data[i].agent_id
                input_dict: TensorStructType = eval_data[i].data.raw_dict

                rnn_states: List[StateBatches] = tree.map_structure(
                    lambda x: x[i], rnn_out
                )

                # extra_action_out could be a nested dict
                fetches: Dict = tree.map_structure(lambda x: x[i], extra_action_out)

                # Post-process policy output by running them through action connectors.
                ac_data = ActionConnectorDataType(
                    env_id, agent_id, input_dict, (action, rnn_states, fetches)
                )

                action_to_send, rnn_states, fetches = policy.action_connectors(
                    ac_data
                ).output

                # The action we want to buffer is the direct output of
                # compute_actions_from_input_dict() here. This is because we want to
                # send the unsqushed actions to the environment while learning and
                # possibly basing subsequent actions on the squashed actions.
                action_to_buffer = (
                    action
                    if env_id not in off_policy_actions
                    or agent_id not in off_policy_actions[env_id]
                    else off_policy_actions[env_id][agent_id]
                )

                # Notify agent connectors with this new policy output.
                # Necessary for state buffering agent connectors, for example.
                ac_data: ActionConnectorDataType = ActionConnectorDataType(
                    env_id,
                    agent_id,
                    input_dict,
                    (action_to_buffer, rnn_states, fetches),
                )
                policy.agent_connectors.on_policy_output(ac_data)

                assert agent_id not in actions_to_send[env_id]
                actions_to_send[env_id][agent_id] = action_to_send

        return actions_to_send

    def _maybe_render(self):
        """Visualize environment."""
        # Check if we should render.
        if not self._render or not self._simple_image_viewer:
            return

        t5 = time.time()

        # Render can either return an RGB image (uint8 [w x h x 3] numpy
        # array) or take care of rendering itself (returning True).
        rendered = self._base_env.try_render()
        # Rendering returned an image -> Display it in a SimpleImageViewer.
        if isinstance(rendered, np.ndarray) and len(rendered.shape) == 3:
            self._simple_image_viewer.imshow(rendered)
        elif rendered not in [True, False, None]:
            raise ValueError(
                f"The env's ({self._base_env}) `try_render()` method returned an"
                " unsupported value! Make sure you either return a "
                "uint8/w x h x 3 (RGB) image or handle rendering in a "
                "window and then return `True`."
            )

        self._perf_stats.incr("env_render_time", time.time() - t5)


def _fetch_atari_metrics(base_env: BaseEnv) -> List[RolloutMetrics]:
    """Atari games have multiple logical episodes, one per life.

    However, for metrics reporting we count full episodes, all lives included.
    """
    sub_environments = base_env.get_sub_environments()
    if not sub_environments:
        return None
    atari_out = []
    for sub_env in sub_environments:
        monitor = get_wrapper_by_cls(sub_env, MonitorEnv)
        if not monitor:
            return None
        for eps_rew, eps_len in monitor.next_episode_results():
            atari_out.append(RolloutMetrics(eps_len, eps_rew))
    return atari_out


def _get_or_raise(
    mapping: Dict[PolicyID, Union[Policy, Preprocessor, Filter]], policy_id: PolicyID
) -> Union[Policy, Preprocessor, Filter]:
    """Returns an object under key `policy_id` in `mapping`.

    Args:
        mapping (Dict[PolicyID, Union[Policy, Preprocessor, Filter]]): The
            mapping dict from policy id (str) to actual object (Policy,
            Preprocessor, etc.).
        policy_id: The policy ID to lookup.

    Returns:
        Union[Policy, Preprocessor, Filter]: The found object.

    Raises:
        ValueError: If `policy_id` cannot be found in `mapping`.
    """
    if policy_id not in mapping:
        raise ValueError(
            "Could not find policy for agent: PolicyID `{}` not found "
            "in policy map, whose keys are `{}`.".format(policy_id, mapping.keys())
        )
    return mapping[policy_id]
