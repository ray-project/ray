from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import defaultdict, namedtuple
import six.moves.queue as queue
import threading

from ray.rllib.evaluation.episode import MultiAgentEpisode
from ray.rllib.evaluation.sample_batch import MultiAgentSampleBatchBuilder, \
    MultiAgentBatch
from ray.rllib.evaluation.tf_policy_graph import TFPolicyGraph
from ray.rllib.env.async_vector_env import AsyncVectorEnv
from ray.rllib.env.atari_wrappers import get_wrapper_by_cls, MonitorEnv
from ray.rllib.utils.tf_run_builder import TFRunBuilder

RolloutMetrics = namedtuple(
    "RolloutMetrics", ["episode_length", "episode_reward", "agent_rewards"])

PolicyEvalData = namedtuple("PolicyEvalData",
                            ["env_id", "agent_id", "obs", "rnn_state"])


class SyncSampler(object):
    """This class interacts with the environment and tells it what to do.

    Note that batch_size is only a unit of measure here. Batches can
    accumulate and the gradient can be calculated on up to 5 batches.

    This class provides data on invocation, rather than on a separate
    thread."""

    def __init__(self,
                 env,
                 policies,
                 policy_mapping_fn,
                 obs_filters,
                 clip_rewards,
                 num_local_steps,
                 horizon=None,
                 pack=False,
                 tf_sess=None):
        self.async_vector_env = AsyncVectorEnv.wrap_async(env)
        self.num_local_steps = num_local_steps
        self.horizon = horizon
        self.policies = policies
        self.policy_mapping_fn = policy_mapping_fn
        self._obs_filters = obs_filters
        self.extra_batches = queue.Queue()
        self.rollout_provider = _env_runner(
            self.async_vector_env, self.extra_batches.put, self.policies,
            self.policy_mapping_fn, self.num_local_steps, self.horizon,
            self._obs_filters, clip_rewards, pack, tf_sess)
        self.metrics_queue = queue.Queue()

    def get_data(self):
        while True:
            item = next(self.rollout_provider)
            if isinstance(item, RolloutMetrics):
                self.metrics_queue.put(item)
            else:
                return item

    def get_metrics(self):
        completed = []
        while True:
            try:
                completed.append(self.metrics_queue.get_nowait())
            except queue.Empty:
                break
        return completed

    def get_extra_batches(self):
        extra = []
        while True:
            try:
                extra.append(self.extra_batches.get_nowait())
            except queue.Empty:
                break
        return extra


class AsyncSampler(threading.Thread):
    """This class interacts with the environment and tells it what to do.

    Note that batch_size is only a unit of measure here. Batches can
    accumulate and the gradient can be calculated on up to 5 batches."""

    def __init__(self,
                 env,
                 policies,
                 policy_mapping_fn,
                 obs_filters,
                 clip_rewards,
                 num_local_steps,
                 horizon=None,
                 pack=False,
                 tf_sess=None):
        for _, f in obs_filters.items():
            assert getattr(f, "is_concurrent", False), \
                "Observation Filter must support concurrent updates."
        self.async_vector_env = AsyncVectorEnv.wrap_async(env)
        threading.Thread.__init__(self)
        self.queue = queue.Queue(5)
        self.extra_batches = queue.Queue()
        self.metrics_queue = queue.Queue()
        self.num_local_steps = num_local_steps
        self.horizon = horizon
        self.policies = policies
        self.policy_mapping_fn = policy_mapping_fn
        self._obs_filters = obs_filters
        self.clip_rewards = clip_rewards
        self.daemon = True
        self.pack = pack
        self.tf_sess = tf_sess

    def run(self):
        try:
            self._run()
        except BaseException as e:
            self.queue.put(e)
            raise e

    def _run(self):
        rollout_provider = _env_runner(
            self.async_vector_env, self.extra_batches.put, self.policies,
            self.policy_mapping_fn, self.num_local_steps, self.horizon,
            self._obs_filters, self.clip_rewards, self.pack, self.tf_sess)
        while True:
            # The timeout variable exists because apparently, if one worker
            # dies, the other workers won't die with it, unless the timeout is
            # set to some large number. This is an empirical observation.
            item = next(rollout_provider)
            if isinstance(item, RolloutMetrics):
                self.metrics_queue.put(item)
            else:
                self.queue.put(item, timeout=600.0)

    def get_data(self):
        rollout = self.queue.get(timeout=600.0)

        # Propagate errors
        if isinstance(rollout, BaseException):
            raise rollout

        # We can't auto-concat rollouts in these modes
        if self.async_vector_env.num_envs > 1 or \
                isinstance(rollout, MultiAgentBatch):
            return rollout

        # Auto-concat rollouts; TODO(ekl) is this important for A3C perf?
        while not rollout["dones"][-1]:
            try:
                part = self.queue.get_nowait()
                if isinstance(part, BaseException):
                    raise rollout
                rollout = rollout.concat(part)
            except queue.Empty:
                break
        return rollout

    def get_metrics(self):
        completed = []
        while True:
            try:
                completed.append(self.metrics_queue.get_nowait())
            except queue.Empty:
                break
        return completed

    def get_extra_batches(self):
        extra = []
        while True:
            try:
                extra.append(self.extra_batches.get_nowait())
            except queue.Empty:
                break
        return extra


def _env_runner(async_vector_env,
                extra_batch_callback,
                policies,
                policy_mapping_fn,
                num_local_steps,
                horizon,
                obs_filters,
                clip_rewards,
                pack,
                tf_sess=None):
    """This implements the common experience collection logic.

    Args:
        async_vector_env (AsyncVectorEnv): env implementing AsyncVectorEnv.
        extra_batch_callback (fn): function to send extra batch data to.
        policies (dict): Map of policy ids to PolicyGraph instances.
        policy_mapping_fn (func): Function that maps agent ids to policy ids.
            This is called when an agent first enters the environment. The
            agent is then "bound" to the returned policy for the episode.
        num_local_steps (int): Number of episode steps before `SampleBatch` is
            yielded. Set to infinity to yield complete episodes.
        horizon (int): Horizon of the episode.
        obs_filters (dict): Map of policy id to filter used to process
            observations for the policy.
        clip_rewards (bool): Whether to clip rewards before postprocessing.
        pack (bool): Whether to pack multiple episodes into each batch. This
            guarantees batches will be exactly `num_local_steps` in size.
        tf_sess (Session|None): Optional tensorflow session to use for batching
            TF policy evaluations.

    Yields:
        rollout (SampleBatch): Object containing state, action, reward,
            terminal condition, and other fields as dictated by `policy`.
    """

    try:
        if not horizon:
            horizon = (
                async_vector_env.get_unwrapped()[0].spec.max_episode_steps)
    except Exception:
        print("Warning, no horizon specified, assuming infinite")
    if not horizon:
        horizon = float("inf")

    # Pool of batch builders, which can be shared across episodes to pack
    # trajectory data.
    batch_builder_pool = []

    def get_batch_builder():
        if batch_builder_pool:
            return batch_builder_pool.pop()
        else:
            return MultiAgentSampleBatchBuilder(policies, clip_rewards)

    def new_episode():
        return MultiAgentEpisode(policies, policy_mapping_fn,
                                 get_batch_builder, extra_batch_callback)

    active_episodes = defaultdict(new_episode)

    while True:
        # Get observations from all ready agents
        unfiltered_obs, rewards, dones, infos, off_policy_actions = \
            async_vector_env.poll()

        # Map of policy_id to list of PolicyEvalData
        to_eval = defaultdict(list)

        # Map of env_id -> agent_id -> action replies
        actions_to_send = defaultdict(dict)

        # For each environment
        for env_id, agent_obs in unfiltered_obs.items():
            new_episode = env_id not in active_episodes
            episode = active_episodes[env_id]
            if not new_episode:
                episode.length += 1
                episode.batch_builder.count += 1
                episode._add_agent_rewards(rewards[env_id])

            # Check episode termination conditions
            if dones[env_id]["__all__"] or episode.length >= horizon:
                all_done = True
                atari_metrics = _fetch_atari_metrics(async_vector_env)
                if atari_metrics is not None:
                    for m in atari_metrics:
                        yield m
                else:
                    yield RolloutMetrics(episode.length, episode.total_reward,
                                         dict(episode.agent_rewards))
            else:
                all_done = False
                # At least send an empty dict if not done
                actions_to_send[env_id] = {}

            # For each agent in the environment
            for agent_id, raw_obs in agent_obs.items():
                policy_id = episode.policy_for(agent_id)
                filtered_obs = _get_or_raise(obs_filters, policy_id)(raw_obs)
                agent_done = bool(all_done or dones[env_id].get(agent_id))
                if not agent_done:
                    to_eval[policy_id].append(
                        PolicyEvalData(env_id, agent_id, filtered_obs,
                                       episode.rnn_state_for(agent_id)))

                last_observation = episode.last_observation_for(agent_id)
                episode._set_last_observation(agent_id, filtered_obs)

                # Record transition info if applicable
                if last_observation is not None and \
                        infos[env_id][agent_id].get("training_enabled", True):
                    episode.batch_builder.add_values(
                        agent_id,
                        policy_id,
                        t=episode.length - 1,
                        eps_id=episode.episode_id,
                        obs=last_observation,
                        actions=episode.last_action_for(agent_id),
                        rewards=rewards[env_id][agent_id],
                        dones=agent_done,
                        infos=infos[env_id][agent_id],
                        new_obs=filtered_obs,
                        **episode.last_pi_info_for(agent_id))

            # Cut the batch if we're not packing multiple episodes into one,
            # or if we've exceeded the requested batch size.
            if episode.batch_builder.has_pending_data():
                if (all_done and not pack) or \
                        episode.batch_builder.count >= num_local_steps:
                    yield episode.batch_builder.build_and_reset()
                elif all_done:
                    # Make sure postprocessor stays within one episode
                    episode.batch_builder.postprocess_batch_so_far()

            if all_done:
                # Handle episode termination
                batch_builder_pool.append(episode.batch_builder)
                del active_episodes[env_id]
                resetted_obs = async_vector_env.try_reset(env_id)
                if resetted_obs is None:
                    # Reset not supported, drop this env from the ready list
                    assert horizon == float("inf"), \
                        "Setting episode horizon requires reset() support."
                else:
                    # Creates a new episode
                    episode = active_episodes[env_id]
                    for agent_id, raw_obs in resetted_obs.items():
                        policy_id = episode.policy_for(agent_id)
                        filtered_obs = _get_or_raise(obs_filters,
                                                     policy_id)(raw_obs)
                        episode._set_last_observation(agent_id, filtered_obs)
                        to_eval[policy_id].append(
                            PolicyEvalData(env_id, agent_id, filtered_obs,
                                           episode.rnn_state_for(agent_id)))

        # Batch eval policy actions if possible
        if tf_sess:
            builder = TFRunBuilder(tf_sess, "policy_eval")
            pending_fetches = {}
        else:
            builder = None
        eval_results = {}
        rnn_in_cols = {}
        for policy_id, eval_data in to_eval.items():
            rnn_in = _to_column_format([t.rnn_state for t in eval_data])
            rnn_in_cols[policy_id] = rnn_in
            policy = _get_or_raise(policies, policy_id)
            if builder and (policy.compute_actions.__code__ is
                            TFPolicyGraph.compute_actions.__code__):
                pending_fetches[policy_id] = policy.build_compute_actions(
                    builder, [t.obs for t in eval_data],
                    rnn_in,
                    is_training=True)
            else:
                eval_results[policy_id] = policy.compute_actions(
                    [t.obs for t in eval_data],
                    rnn_in,
                    is_training=True,
                    episodes=[active_episodes[t.env_id] for t in eval_data])
        if builder:
            for k, v in pending_fetches.items():
                eval_results[k] = builder.get(v)

        # Record the policy eval results
        for policy_id, eval_data in to_eval.items():
            actions, rnn_out_cols, pi_info_cols = eval_results[policy_id]
            # Add RNN state info
            for f_i, column in enumerate(rnn_in_cols[policy_id]):
                pi_info_cols["state_in_{}".format(f_i)] = column
            for f_i, column in enumerate(rnn_out_cols):
                pi_info_cols["state_out_{}".format(f_i)] = column
            # Save output rows
            for i, action in enumerate(actions):
                env_id = eval_data[i].env_id
                agent_id = eval_data[i].agent_id
                actions_to_send[env_id][agent_id] = action
                episode = active_episodes[env_id]
                episode._set_rnn_state(agent_id, [c[i] for c in rnn_out_cols])
                episode._set_last_pi_info(
                    agent_id, {k: v[i]
                               for k, v in pi_info_cols.items()})
                if env_id in off_policy_actions and \
                        agent_id in off_policy_actions[env_id]:
                    episode._set_last_action(
                        agent_id, off_policy_actions[env_id][agent_id])
                else:
                    episode._set_last_action(agent_id, action)

        # Return computed actions to ready envs. We also send to envs that have
        # taken off-policy actions; those envs are free to ignore the action.
        async_vector_env.send_actions(dict(actions_to_send))


def _fetch_atari_metrics(async_vector_env):
    """Atari games have multiple logical episodes, one per life.

    However for metrics reporting we count full episodes all lives included.
    """
    unwrapped = async_vector_env.get_unwrapped()
    if not unwrapped:
        return None
    atari_out = []
    for u in unwrapped:
        monitor = get_wrapper_by_cls(u, MonitorEnv)
        if not monitor:
            return None
        for eps_rew, eps_len in monitor.next_episode_results():
            atari_out.append(RolloutMetrics(eps_len, eps_rew, {}))
    return atari_out


def _to_column_format(rnn_state_rows):
    num_cols = len(rnn_state_rows[0])
    return [[row[i] for row in rnn_state_rows] for i in range(num_cols)]


def _get_or_raise(mapping, policy_id):
    if policy_id not in mapping:
        raise ValueError(
            "Could not find policy for agent: agent policy id `{}` not "
            "in policy map keys {}.".format(policy_id, mapping.keys()))
    return mapping[policy_id]
