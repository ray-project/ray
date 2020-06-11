from abc import abstractmethod, ABCMeta
from collections import defaultdict, namedtuple
import logging
import numpy as np
import queue
import threading
import time

from ray.util.debug import log_once
from ray.rllib.evaluation.episode import MultiAgentEpisode
from ray.rllib.evaluation.rollout_metrics import RolloutMetrics
from ray.rllib.evaluation.sample_batch_builder import \
    MultiAgentSampleBatchBuilder
from ray.rllib.policy.policy import clip_action
from ray.rllib.policy.tf_policy import TFPolicy
from ray.rllib.env.base_env import BaseEnv, ASYNC_RESET_RETURN
from ray.rllib.env.atari_wrappers import get_wrapper_by_cls, MonitorEnv
from ray.rllib.offline import InputReader
from ray.rllib.utils import try_import_tree
from ray.rllib.utils.annotations import override, DeveloperAPI
from ray.rllib.utils.debug import summarize
from ray.rllib.utils.spaces.space_utils import flatten_to_single_ndarray, \
    unbatch
from ray.rllib.utils.tf_run_builder import TFRunBuilder

tree = try_import_tree()

logger = logging.getLogger(__name__)

PolicyEvalData = namedtuple("PolicyEvalData", [
    "env_id", "agent_id", "obs", "info", "rnn_state", "prev_action",
    "prev_reward"
])


class PerfStats:
    """Sampler perf stats that will be included in rollout metrics."""

    def __init__(self):
        self.iters = 0
        self.env_wait_time = 0.0
        self.processing_time = 0.0
        self.inference_time = 0.0

    def get(self):
        return {
            "mean_env_wait_ms": self.env_wait_time * 1000 / self.iters,
            "mean_processing_ms": self.processing_time * 1000 / self.iters,
            "mean_inference_ms": self.inference_time * 1000 / self.iters
        }


@DeveloperAPI
class SamplerInput(InputReader, metaclass=ABCMeta):
    """Reads input experiences from an existing sampler."""

    @override(InputReader)
    def next(self):
        batches = [self.get_data()]
        batches.extend(self.get_extra_batches())
        if len(batches) > 1:
            return batches[0].concat_samples(batches)
        else:
            return batches[0]

    @abstractmethod
    @DeveloperAPI
    def get_data(self):
        raise NotImplementedError

    @abstractmethod
    @DeveloperAPI
    def get_metrics(self):
        raise NotImplementedError

    @abstractmethod
    @DeveloperAPI
    def get_extra_batches(self):
        raise NotImplementedError


@DeveloperAPI
class SyncSampler(SamplerInput):
    """Sync SamplerInput that collects experiences when `get_data()` is called.
    """

    def __init__(self,
                 *,
                 worker,
                 env,
                 policies,
                 policy_mapping_fn,
                 preprocessors,
                 obs_filters,
                 clip_rewards,
                 rollout_fragment_length,
                 callbacks,
                 horizon=None,
                 pack_multiple_episodes_in_batch=False,
                 tf_sess=None,
                 clip_actions=True,
                 soft_horizon=False,
                 no_done_at_end=False,
                 observation_fn=None):
        """Initializes a SyncSampler object.

        Args:
            worker (RolloutWorker): The RolloutWorker that will use this
                Sampler for sampling.
            env (Env): Any Env object. Will be converted into an RLlib BaseEnv.
            policies (Dict[str,Policy]): Mapping from policy ID to Policy obj.
            policy_mapping_fn (callable): Callable that takes an agent ID and
                returns a Policy object.
            preprocessors (Dict[str,Preprocessor]): Mapping from policy ID to
                Preprocessor object for the observations prior to filtering.
            obs_filters (Dict[str,Filter]): Mapping from policy ID to
                env Filter object.
            clip_rewards (Union[bool,float]): True for +/-1.0 clipping, actual
                float value for +/- value clipping. False for no clipping.
            rollout_fragment_length (int): The length of a fragment to collect
                before building a SampleBatch from the data and resetting
                the SampleBatchBuilder object.
            callbacks (Callbacks): The Callbacks object to use when episode
                events happen during rollout.
            horizon (Optional[int]): Hard-reset the Env
            pack_multiple_episodes_in_batch (bool): Whether to pack multiple
                episodes into each batch. This guarantees batches will be
                exactly `rollout_fragment_length` in size.
            tf_sess (Optional[tf.Session]): A tf.Session object to use (only if
                framework=tf).
            clip_actions (bool): Whether to clip actions according to the
                given action_space's bounds.
            soft_horizon (bool): If True, calculate bootstrapped values as if
                episode had ended, but don't physically reset the environment
                when the horizon is hit.
            no_done_at_end (bool): Ignore the done=True at the end of the
                episode and instead record done=False.
            observation_fn (Optional[ObservationFunction]): Optional
                multi-agent observation func to use for preprocessing
                observations.
        """

        self.base_env = BaseEnv.to_base_env(env)
        self.rollout_fragment_length = rollout_fragment_length
        self.horizon = horizon
        self.policies = policies
        self.policy_mapping_fn = policy_mapping_fn
        self.preprocessors = preprocessors
        self.obs_filters = obs_filters
        self.extra_batches = queue.Queue()
        self.perf_stats = PerfStats()
        # Create the rollout generator to use for calls to `get_data()`.
        self.rollout_provider = _env_runner(
            worker, self.base_env, self.extra_batches.put, self.policies,
            self.policy_mapping_fn, self.rollout_fragment_length, self.horizon,
            self.preprocessors, self.obs_filters, clip_rewards, clip_actions,
            pack_multiple_episodes_in_batch, callbacks, tf_sess,
            self.perf_stats, soft_horizon, no_done_at_end, observation_fn)
        self.metrics_queue = queue.Queue()

    @override(SamplerInput)
    def get_data(self):
        while True:
            item = next(self.rollout_provider)
            if isinstance(item, RolloutMetrics):
                self.metrics_queue.put(item)
            else:
                return item

    @override(SamplerInput)
    def get_metrics(self):
        completed = []
        while True:
            try:
                completed.append(self.metrics_queue.get_nowait()._replace(
                    perf_stats=self.perf_stats.get()))
            except queue.Empty:
                break
        return completed

    @override(SamplerInput)
    def get_extra_batches(self):
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

    Once started, experiences are continuously collected and put into a Queue,
    from where they can be unqueued by the caller of `get_data()`.
    """

    def __init__(self,
                 *,
                 worker,
                 env,
                 policies,
                 policy_mapping_fn,
                 preprocessors,
                 obs_filters,
                 clip_rewards,
                 rollout_fragment_length,
                 callbacks,
                 horizon=None,
                 pack_multiple_episodes_in_batch=False,
                 tf_sess=None,
                 clip_actions=True,
                 blackhole_outputs=False,
                 soft_horizon=False,
                 no_done_at_end=False,
                 observation_fn=None):
        """Initializes a AsyncSampler object.

        Args:
            worker (RolloutWorker): The RolloutWorker that will use this
                Sampler for sampling.
            env (Env): Any Env object. Will be converted into an RLlib BaseEnv.
            policies (Dict[str,Policy]): Mapping from policy ID to Policy obj.
            policy_mapping_fn (callable): Callable that takes an agent ID and
                returns a Policy object.
            preprocessors (Dict[str,Preprocessor]): Mapping from policy ID to
                Preprocessor object for the observations prior to filtering.
            obs_filters (Dict[str,Filter]): Mapping from policy ID to
                env Filter object.
            clip_rewards (Union[bool,float]): True for +/-1.0 clipping, actual
                float value for +/- value clipping. False for no clipping.
            rollout_fragment_length (int): The length of a fragment to collect
                before building a SampleBatch from the data and resetting
                the SampleBatchBuilder object.
            callbacks (Callbacks): The Callbacks object to use when episode
                events happen during rollout.
            horizon (Optional[int]): Hard-reset the Env
            pack_multiple_episodes_in_batch (bool): Whether to pack multiple
                episodes into each batch. This guarantees batches will be
                exactly `rollout_fragment_length` in size.
            tf_sess (Optional[tf.Session]): A tf.Session object to use (only if
                framework=tf).
            clip_actions (bool): Whether to clip actions according to the
                given action_space's bounds.
            blackhole_outputs (bool): Whether to collect samples, but then
                not further process or store them (throw away all samples).
            soft_horizon (bool): If True, calculate bootstrapped values as if
                episode had ended, but don't physically reset the environment
                when the horizon is hit.
            no_done_at_end (bool): Ignore the done=True at the end of the
                episode and instead record done=False.
            observation_fn (Optional[ObservationFunction]): Optional
                multi-agent observation func to use for preprocessing
                observations.
        """
        for _, f in obs_filters.items():
            assert getattr(f, "is_concurrent", False), \
                "Observation Filter must support concurrent updates."
        self.worker = worker
        self.base_env = BaseEnv.to_base_env(env)
        threading.Thread.__init__(self)
        self.queue = queue.Queue(5)
        self.extra_batches = queue.Queue()
        self.metrics_queue = queue.Queue()
        self.rollout_fragment_length = rollout_fragment_length
        self.horizon = horizon
        self.policies = policies
        self.policy_mapping_fn = policy_mapping_fn
        self.preprocessors = preprocessors
        self.obs_filters = obs_filters
        self.clip_rewards = clip_rewards
        self.daemon = True
        self.pack_multiple_episodes_in_batch = pack_multiple_episodes_in_batch
        self.tf_sess = tf_sess
        self.callbacks = callbacks
        self.clip_actions = clip_actions
        self.blackhole_outputs = blackhole_outputs
        self.soft_horizon = soft_horizon
        self.no_done_at_end = no_done_at_end
        self.perf_stats = PerfStats()
        self.shutdown = False
        self.observation_fn = observation_fn

    @override(threading.Thread)
    def run(self):
        try:
            self._run()
        except BaseException as e:
            self.queue.put(e)
            raise e

    def _run(self):
        if self.blackhole_outputs:
            queue_putter = (lambda x: None)
            extra_batches_putter = (lambda x: None)
        else:
            queue_putter = self.queue.put
            extra_batches_putter = (
                lambda x: self.extra_batches.put(x, timeout=600.0))
        rollout_provider = _env_runner(
            self.worker, self.base_env, extra_batches_putter, self.policies,
            self.policy_mapping_fn, self.rollout_fragment_length, self.horizon,
            self.preprocessors, self.obs_filters, self.clip_rewards,
            self.clip_actions, self.pack_multiple_episodes_in_batch,
            self.callbacks, self.tf_sess, self.perf_stats, self.soft_horizon,
            self.no_done_at_end, self.observation_fn)
        while not self.shutdown:
            # The timeout variable exists because apparently, if one worker
            # dies, the other workers won't die with it, unless the timeout is
            # set to some large number. This is an empirical observation.
            item = next(rollout_provider)
            if isinstance(item, RolloutMetrics):
                self.metrics_queue.put(item)
            else:
                queue_putter(item)

    @override(SamplerInput)
    def get_data(self):
        if not self.is_alive():
            raise RuntimeError("Sampling thread has died")
        rollout = self.queue.get(timeout=600.0)

        # Propagate errors
        if isinstance(rollout, BaseException):
            raise rollout

        return rollout

    @override(SamplerInput)
    def get_metrics(self):
        completed = []
        while True:
            try:
                completed.append(self.metrics_queue.get_nowait()._replace(
                    perf_stats=self.perf_stats.get()))
            except queue.Empty:
                break
        return completed

    @override(SamplerInput)
    def get_extra_batches(self):
        extra = []
        while True:
            try:
                extra.append(self.extra_batches.get_nowait())
            except queue.Empty:
                break
        return extra


def _env_runner(worker, base_env, extra_batch_callback, policies,
                policy_mapping_fn, rollout_fragment_length, horizon,
                preprocessors, obs_filters, clip_rewards, clip_actions,
                pack_multiple_episodes_in_batch, callbacks, tf_sess,
                perf_stats, soft_horizon, no_done_at_end, observation_fn):
    """This implements the common experience collection logic.

    Args:
        worker (RolloutWorker): Reference to the current rollout worker.
        base_env (BaseEnv): Env implementing BaseEnv.
        extra_batch_callback (fn): function to send extra batch data to.
        policies (dict): Map of policy ids to Policy instances.
        policy_mapping_fn (func): Function that maps agent ids to policy ids.
            This is called when an agent first enters the environment. The
            agent is then "bound" to the returned policy for the episode.
        rollout_fragment_length (int): Number of episode steps before
            `SampleBatch` is yielded. Set to infinity to yield complete
            episodes.
        horizon (int): Horizon of the episode.
        preprocessors (dict): Map of policy id to preprocessor for the
            observations prior to filtering.
        obs_filters (dict): Map of policy id to filter used to process
            observations for the policy.
        clip_rewards (bool): Whether to clip rewards before postprocessing.
        pack_multiple_episodes_in_batch (bool): Whether to pack multiple
            episodes into each batch. This guarantees batches will be exactly
            `rollout_fragment_length` in size.
        clip_actions (bool): Whether to clip actions to the space range.
        callbacks (DefaultCallbacks): User callbacks to run on episode events.
        tf_sess (Session|None): Optional tensorflow session to use for batching
            TF policy evaluations.
        perf_stats (PerfStats): Record perf stats into this object.
        soft_horizon (bool): Calculate rewards but don't reset the
            environment when the horizon is hit.
        no_done_at_end (bool): Ignore the done=True at the end of the episode
            and instead record done=False.
        observation_fn (ObservationFunction): Optional multi-agent
            observation func to use for preprocessing observations.

    Yields:
        rollout (SampleBatch): Object containing state, action, reward,
            terminal condition, and other fields as dictated by `policy`.
    """

    # Try to get Env's max_episode_steps prop. If it doesn't exist, catch
    # error and continue.
    max_episode_steps = None
    try:
        max_episode_steps = base_env.get_unwrapped()[0].spec.max_episode_steps
    except Exception:
        pass

    # Trainer has a given `horizon` setting.
    if horizon:
        # `horizon` is larger than env's limit -> Error and explain how
        # to increase Env's own episode limit.
        if max_episode_steps and horizon > max_episode_steps:
            raise ValueError(
                "Your `horizon` setting ({}) is larger than the Env's own "
                "timestep limit ({})! Try to increase the Env's limit via "
                "setting its `spec.max_episode_steps` property.".format(
                    horizon, max_episode_steps))
    # Otherwise, set Trainer's horizon to env's max-steps.
    elif max_episode_steps:
        horizon = max_episode_steps
        logger.debug(
            "No episode horizon specified, setting it to Env's limit ({}).".
            format(max_episode_steps))
    else:
        horizon = float("inf")
        logger.debug("No episode horizon specified, assuming inf.")

    # Pool of batch builders, which can be shared across episodes to pack
    # trajectory data.
    batch_builder_pool = []

    def get_batch_builder():
        if batch_builder_pool:
            return batch_builder_pool.pop()
        else:
            return MultiAgentSampleBatchBuilder(policies, clip_rewards,
                                                callbacks)

    def new_episode():
        episode = MultiAgentEpisode(policies, policy_mapping_fn,
                                    get_batch_builder, extra_batch_callback)
        # Call each policy's Exploration.on_episode_start method.
        for p in policies.values():
            if getattr(p, "exploration", None) is not None:
                p.exploration.on_episode_start(
                    policy=p,
                    environment=base_env,
                    episode=episode,
                    tf_sess=getattr(p, "_sess", None))
        callbacks.on_episode_start(
            worker=worker,
            base_env=base_env,
            policies=policies,
            episode=episode)
        return episode

    active_episodes = defaultdict(new_episode)

    while True:
        perf_stats.iters += 1
        t0 = time.time()
        # Get observations from all ready agents.
        unfiltered_obs, rewards, dones, infos, off_policy_actions = \
            base_env.poll()
        perf_stats.env_wait_time += time.time() - t0

        if log_once("env_returns"):
            logger.info("Raw obs from env: {}".format(
                summarize(unfiltered_obs)))
            logger.info("Info return from env: {}".format(summarize(infos)))

        # Process observations and prepare for policy evaluation.
        t1 = time.time()
        active_envs, to_eval, outputs = _process_observations(
            worker=worker,
            base_env=base_env,
            policies=policies,
            batch_builder_pool=batch_builder_pool,
            active_episodes=active_episodes,
            unfiltered_obs=unfiltered_obs,
            rewards=rewards,
            dones=dones,
            infos=infos,
            horizon=horizon,
            preprocessors=preprocessors,
            obs_filters=obs_filters,
            rollout_fragment_length=rollout_fragment_length,
            pack_multiple_episodes_in_batch=pack_multiple_episodes_in_batch,
            callbacks=callbacks,
            soft_horizon=soft_horizon,
            no_done_at_end=no_done_at_end,
            observation_fn=observation_fn)
        perf_stats.processing_time += time.time() - t1
        for o in outputs:
            yield o

        # Do batched policy eval (accross vectorized envs).
        t2 = time.time()
        eval_results = _do_policy_eval(
            to_eval=to_eval,
            policies=policies,
            active_episodes=active_episodes,
            tf_sess=tf_sess)
        perf_stats.inference_time += time.time() - t2

        # Process results and update episode state.
        t3 = time.time()
        actions_to_send = _process_policy_eval_results(
            to_eval=to_eval,
            eval_results=eval_results,
            active_episodes=active_episodes,
            active_envs=active_envs,
            off_policy_actions=off_policy_actions,
            policies=policies,
            clip_actions=clip_actions)
        perf_stats.processing_time += time.time() - t3

        # Return computed actions to ready envs. We also send to envs that have
        # taken off-policy actions; those envs are free to ignore the action.
        t4 = time.time()
        base_env.send_actions(actions_to_send)
        perf_stats.env_wait_time += time.time() - t4


def _process_observations(
        worker, base_env, policies, batch_builder_pool, active_episodes,
        unfiltered_obs, rewards, dones, infos, horizon, preprocessors,
        obs_filters, rollout_fragment_length, pack_multiple_episodes_in_batch,
        callbacks, soft_horizon, no_done_at_end, observation_fn):
    """Record new data from the environment and prepare for policy evaluation.

    Args:
        worker (RolloutWorker): Reference to the current rollout worker.
        base_env (BaseEnv): Env implementing BaseEnv.
        policies (dict): Map of policy ids to Policy instances.
        batch_builder_pool (List[SampleBatchBuilder]): List of pooled
            SampleBatchBuilder object for recycling.
        active_episodes (defaultdict[str,MultiAgentEpisode]): Mapping from
            episode ID to currently ongoing MultiAgentEpisode object.
        unfiltered_obs (dict): Doubly keyed dict of env-ids -> agent ids ->
            unfiltered observation tensor, returned by a `BaseEnv.poll()` call.
        rewards (dict): Doubly keyed dict of env-ids -> agent ids ->
            rewards tensor, returned by a `BaseEnv.poll()` call.
        dones (dict): Doubly keyed dict of env-ids -> agent ids ->
            boolean done flags, returned by a `BaseEnv.poll()` call.
        infos (dict): Doubly keyed dict of env-ids -> agent ids ->
            info dicts, returned by a `BaseEnv.poll()` call.
        horizon (int): Horizon of the episode.
        preprocessors (dict): Map of policy id to preprocessor for the
            observations prior to filtering.
        obs_filters (dict): Map of policy id to filter used to process
            observations for the policy.
        rollout_fragment_length (int): Number of episode steps before
            `SampleBatch` is yielded. Set to infinity to yield complete
            episodes.
        pack_multiple_episodes_in_batch (bool): Whether to pack multiple
            episodes into each batch. This guarantees batches will be exactly
            `rollout_fragment_length` in size.
        callbacks (DefaultCallbacks): User callbacks to run on episode events.
        soft_horizon (bool): Calculate rewards but don't reset the
            environment when the horizon is hit.
        no_done_at_end (bool): Ignore the done=True at the end of the episode
            and instead record done=False.
        observation_fn (ObservationFunction): Optional multi-agent
            observation func to use for preprocessing observations.

    Returns:
        Tuple:
            - active_envs: Set of non-terminated env ids.
            - to_eval: Map of policy_id to list of agent PolicyEvalData.
            - outputs: List of metrics and samples to return from the sampler.
    """

    active_envs = set()
    to_eval = defaultdict(list)
    outputs = []
    large_batch_threshold = max(1000, rollout_fragment_length * 10) if \
        rollout_fragment_length != float("inf") else 5000

    # For each environment.
    for env_id, agent_obs in unfiltered_obs.items():
        is_new_episode = env_id not in active_episodes
        episode = active_episodes[env_id]
        if not is_new_episode:
            episode.length += 1
            episode.batch_builder.count += 1
            episode._add_agent_rewards(rewards[env_id])

        if (episode.batch_builder.total() > large_batch_threshold
                and log_once("large_batch_warning")):
            logger.warning(
                "More than {} observations for {} env steps ".format(
                    episode.batch_builder.total(),
                    episode.batch_builder.count) + "are buffered in "
                "the sampler. If this is more than you expected, check that "
                "that you set a horizon on your environment correctly and that"
                " it terminates at some point. "
                "Note: In multi-agent environments, `rollout_fragment_length` "
                "sets the batch size based on environment steps, not the "
                "steps of "
                "individual agents, which can result in unexpectedly large "
                "batches. Also, you may be in evaluation waiting for your Env "
                "to terminate (batch_mode=`complete_episodes`). Make sure it "
                "does at some point.")

        # Check episode termination conditions.
        if dones[env_id]["__all__"] or episode.length >= horizon:
            hit_horizon = (episode.length >= horizon
                           and not dones[env_id]["__all__"])
            all_agents_done = True
            atari_metrics = _fetch_atari_metrics(base_env)
            if atari_metrics is not None:
                for m in atari_metrics:
                    outputs.append(
                        m._replace(custom_metrics=episode.custom_metrics))
            else:
                outputs.append(
                    RolloutMetrics(episode.length, episode.total_reward,
                                   dict(episode.agent_rewards),
                                   episode.custom_metrics, {},
                                   episode.hist_data))
        else:
            hit_horizon = False
            all_agents_done = False
            active_envs.add(env_id)

        # Custom observation function is applied before preprocessing.
        if observation_fn:
            agent_obs = observation_fn(
                agent_obs=agent_obs,
                worker=worker,
                base_env=base_env,
                policies=policies,
                episode=episode)
            if not isinstance(agent_obs, dict):
                raise ValueError(
                    "observe() must return a dict of agent observations")

        # For each agent in the environment.
        for agent_id, raw_obs in agent_obs.items():
            assert agent_id != "__all__"
            policy_id = episode.policy_for(agent_id)
            prep_obs = _get_or_raise(preprocessors,
                                     policy_id).transform(raw_obs)
            if log_once("prep_obs"):
                logger.info("Preprocessed obs: {}".format(summarize(prep_obs)))

            filtered_obs = _get_or_raise(obs_filters, policy_id)(prep_obs)
            if log_once("filtered_obs"):
                logger.info("Filtered obs: {}".format(summarize(filtered_obs)))

            agent_done = bool(all_agents_done or dones[env_id].get(agent_id))
            if not agent_done:
                to_eval[policy_id].append(
                    PolicyEvalData(env_id, agent_id, filtered_obs,
                                   infos[env_id].get(agent_id, {}),
                                   episode.rnn_state_for(agent_id),
                                   episode.last_action_for(agent_id),
                                   rewards[env_id][agent_id] or 0.0))

            last_observation = episode.last_observation_for(agent_id)
            episode._set_last_observation(agent_id, filtered_obs)
            episode._set_last_raw_obs(agent_id, raw_obs)
            episode._set_last_info(agent_id, infos[env_id].get(agent_id, {}))

            # Record transition info if applicable.
            if (last_observation is not None and infos[env_id].get(
                    agent_id, {}).get("training_enabled", True)):
                episode.batch_builder.add_values(
                    agent_id,
                    policy_id,
                    t=episode.length - 1,
                    eps_id=episode.episode_id,
                    agent_index=episode._agent_index(agent_id),
                    obs=last_observation,
                    actions=episode.last_action_for(agent_id),
                    rewards=rewards[env_id][agent_id],
                    prev_actions=episode.prev_action_for(agent_id),
                    prev_rewards=episode.prev_reward_for(agent_id),
                    dones=(False if (no_done_at_end
                                     or (hit_horizon and soft_horizon)) else
                           agent_done),
                    infos=infos[env_id].get(agent_id, {}),
                    new_obs=filtered_obs,
                    **episode.last_pi_info_for(agent_id))

        # Invoke the step callback after the step is logged to the episode
        callbacks.on_episode_step(
            worker=worker, base_env=base_env, episode=episode)

        # Cut the batch if we're not packing multiple episodes into one,
        # or if we've exceeded the requested batch size.
        if episode.batch_builder.has_pending_agent_data():
            # Sanity check, whether all agents have done=True, if done[__all__]
            # is True.
            if dones[env_id]["__all__"] and not no_done_at_end:
                episode.batch_builder.check_missing_dones()

            # Reached end of episode and we are not allowed to pack the
            # next episode into the same SampleBatch -> Build the SampleBatch
            # and add it to "outputs".
            if (all_agents_done and not pack_multiple_episodes_in_batch) or \
                    episode.batch_builder.count >= rollout_fragment_length:
                outputs.append(episode.batch_builder.build_and_reset(episode))
            # Make sure postprocessor stays within one episode.
            elif all_agents_done:
                episode.batch_builder.postprocess_batch_so_far(episode)

        if all_agents_done:
            # Handle episode termination.
            batch_builder_pool.append(episode.batch_builder)
            # Call each policy's Exploration.on_episode_end method.
            for p in policies.values():
                if getattr(p, "exploration", None) is not None:
                    p.exploration.on_episode_end(
                        policy=p,
                        environment=base_env,
                        episode=episode,
                        tf_sess=getattr(p, "_sess", None))
            # Call custom on_episode_end callback.
            callbacks.on_episode_end(
                worker=worker,
                base_env=base_env,
                policies=policies,
                episode=episode)
            if hit_horizon and soft_horizon:
                episode.soft_reset()
                resetted_obs = agent_obs
            else:
                del active_episodes[env_id]
                resetted_obs = base_env.try_reset(env_id)
            if resetted_obs is None:
                # Reset not supported, drop this env from the ready list.
                if horizon != float("inf"):
                    raise ValueError(
                        "Setting episode horizon requires reset() support "
                        "from the environment.")
            elif resetted_obs != ASYNC_RESET_RETURN:
                # Creates a new episode if this is not async return.
                # If reset is async, we will get its result in some future poll
                episode = active_episodes[env_id]
                if observation_fn:
                    resetted_obs = observation_fn(
                        agent_obs=resetted_obs,
                        worker=worker,
                        base_env=base_env,
                        policies=policies,
                        episode=episode)
                for agent_id, raw_obs in resetted_obs.items():
                    policy_id = episode.policy_for(agent_id)
                    policy = _get_or_raise(policies, policy_id)
                    prep_obs = _get_or_raise(preprocessors,
                                             policy_id).transform(raw_obs)
                    filtered_obs = _get_or_raise(obs_filters,
                                                 policy_id)(prep_obs)
                    episode._set_last_observation(agent_id, filtered_obs)
                    to_eval[policy_id].append(
                        PolicyEvalData(
                            env_id, agent_id, filtered_obs,
                            episode.last_info_for(agent_id) or {},
                            episode.rnn_state_for(agent_id),
                            np.zeros_like(
                                flatten_to_single_ndarray(
                                    policy.action_space.sample())), 0.0))

    return active_envs, to_eval, outputs


def _do_policy_eval(*, to_eval, policies, active_episodes, tf_sess=None):
    """Call compute_actions on collected episode/model data to get next action.

    Args:
        tf_sess (Optional[tf.Session]): Optional tensorflow session to use for
            batching TF policy evaluations.
        to_eval (Dict[str,List[PolicyEvalData]]): Mapping of policy IDs to
            lists of PolicyEvalData objects.
        policies (Dict[str,Policy]): Mapping from policy ID to Policy obj.
        active_episodes (defaultdict[str,MultiAgentEpisode]): Mapping from
            episode ID to currently ongoing MultiAgentEpisode object.

    Returns:
        eval_results: dict of policy to compute_action() outputs.
    """

    eval_results = {}

    if tf_sess:
        builder = TFRunBuilder(tf_sess, "policy_eval")
        pending_fetches = {}
    else:
        builder = None

    if log_once("compute_actions_input"):
        logger.info("Inputs to compute_actions():\n\n{}\n".format(
            summarize(to_eval)))

    for policy_id, eval_data in to_eval.items():
        rnn_in = [t.rnn_state for t in eval_data]
        policy = _get_or_raise(policies, policy_id)
        # If tf (non eager) AND TFPolicy's compute_action method has not been
        # overridden -> Use `policy._build_compute_actions()`.
        if builder and (policy.compute_actions.__code__ is
                        TFPolicy.compute_actions.__code__):

            obs_batch = [t.obs for t in eval_data]
            state_batches = _to_column_format(rnn_in)
            # TODO(ekl): how can we make info batch available to TF code?
            prev_action_batch = [t.prev_action for t in eval_data]
            prev_reward_batch = [t.prev_reward for t in eval_data]

            pending_fetches[policy_id] = policy._build_compute_actions(
                builder,
                obs_batch=obs_batch,
                state_batches=state_batches,
                prev_action_batch=prev_action_batch,
                prev_reward_batch=prev_reward_batch,
                timestep=policy.global_timestep)
        else:
            rnn_in_cols = [
                np.stack([row[i] for row in rnn_in])
                for i in range(len(rnn_in[0]))
            ]
            eval_results[policy_id] = policy.compute_actions(
                [t.obs for t in eval_data],
                state_batches=rnn_in_cols,
                prev_action_batch=[t.prev_action for t in eval_data],
                prev_reward_batch=[t.prev_reward for t in eval_data],
                info_batch=[t.info for t in eval_data],
                episodes=[active_episodes[t.env_id] for t in eval_data],
                timestep=policy.global_timestep)
    if builder:
        for pid, v in pending_fetches.items():
            eval_results[pid] = builder.get(v)

    if log_once("compute_actions_result"):
        logger.info("Outputs of compute_actions():\n\n{}\n".format(
            summarize(eval_results)))

    return eval_results


def _process_policy_eval_results(*, to_eval, eval_results, active_episodes,
                                 active_envs, off_policy_actions, policies,
                                 clip_actions):
    """Process the output of policy neural network evaluation.

    Records policy evaluation results into the given episode objects and
    returns replies to send back to agents in the env.

    Args:
        to_eval (Dict[str,List[PolicyEvalData]]): Mapping of policy IDs to
            lists of PolicyEvalData objects.
        eval_results (Dict[str,List]): Mapping of policy IDs to list of
            actions, rnn-out states, extra-action-fetches dicts.
        active_episodes (defaultdict[str,MultiAgentEpisode]): Mapping from
            episode ID to currently ongoing MultiAgentEpisode object.
        active_envs (Set[int]): Set of non-terminated env ids.
        off_policy_actions (dict): Doubly keyed dict of env-ids -> agent ids ->
            off-policy-action, returned by a `BaseEnv.poll()` call.
        policies (Dict[str,Policy]): Mapping from policy ID to Policy obj.
        clip_actions (bool): Whether to clip actions to the action space's
            bounds.

    Returns:
        actions_to_send: Nested dict of env id -> agent id -> agent replies.
    """

    actions_to_send = defaultdict(dict)
    for env_id in active_envs:
        actions_to_send[env_id] = {}  # at minimum send empty dict

    for policy_id, eval_data in to_eval.items():
        rnn_in_cols = _to_column_format([t.rnn_state for t in eval_data])

        actions = eval_results[policy_id][0]
        rnn_out_cols = eval_results[policy_id][1]
        pi_info_cols = eval_results[policy_id][2]

        # In case actions is a list (representing the 0th dim of a batch of
        # primitive actions), try to convert it first.
        if isinstance(actions, list):
            actions = np.array(actions)

        if len(rnn_in_cols) != len(rnn_out_cols):
            raise ValueError("Length of RNN in did not match RNN out, got: "
                             "{} vs {}".format(rnn_in_cols, rnn_out_cols))
        # Add RNN state info
        for f_i, column in enumerate(rnn_in_cols):
            pi_info_cols["state_in_{}".format(f_i)] = column
        for f_i, column in enumerate(rnn_out_cols):
            pi_info_cols["state_out_{}".format(f_i)] = column

        policy = _get_or_raise(policies, policy_id)
        # Split action-component batches into single action rows.
        actions = unbatch(actions)
        for i, action in enumerate(actions):
            env_id = eval_data[i].env_id
            agent_id = eval_data[i].agent_id
            # Clip if necessary.
            if clip_actions:
                clipped_action = clip_action(action,
                                             policy.action_space_struct)
            else:
                clipped_action = action
            actions_to_send[env_id][agent_id] = clipped_action
            episode = active_episodes[env_id]
            episode._set_rnn_state(agent_id, [c[i] for c in rnn_out_cols])
            episode._set_last_pi_info(
                agent_id, {k: v[i]
                           for k, v in pi_info_cols.items()})
            if env_id in off_policy_actions and \
                    agent_id in off_policy_actions[env_id]:
                episode._set_last_action(agent_id,
                                         off_policy_actions[env_id][agent_id])
            else:
                episode._set_last_action(agent_id, action)

    return actions_to_send


def _fetch_atari_metrics(base_env):
    """Atari games have multiple logical episodes, one per life.

    However, for metrics reporting we count full episodes, all lives included.
    """
    unwrapped = base_env.get_unwrapped()
    if not unwrapped:
        return None
    atari_out = []
    for u in unwrapped:
        monitor = get_wrapper_by_cls(u, MonitorEnv)
        if not monitor:
            return None
        for eps_rew, eps_len in monitor.next_episode_results():
            atari_out.append(RolloutMetrics(eps_len, eps_rew))
    return atari_out


def _to_column_format(rnn_state_rows):
    num_cols = len(rnn_state_rows[0])
    return [[row[i] for row in rnn_state_rows] for i in range(num_cols)]


def _get_or_raise(mapping, policy_id):
    """Returns a Policy object under key `policy_id` in `mapping`.

    Args:
        mapping (dict): The mapping dict from policy id (str) to
            actual Policy object.
        policy_id (str): The policy ID to lookup.

    Returns:
        Policy: The found Policy object.

    Throws:
        ValueError: If `policy_id` cannot be found.
    """
    if policy_id not in mapping:
        raise ValueError(
            "Could not find policy for agent: agent policy id `{}` not "
            "in policy map keys {}.".format(policy_id, mapping.keys()))
    return mapping[policy_id]
