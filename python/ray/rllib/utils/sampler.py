from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import defaultdict, namedtuple
import numpy as np
import six.moves.queue as queue
import threading

from ray.rllib.optimizers.sample_batch import MultiAgentSampleBatchBuilder
from ray.rllib.utils.vector_env import VectorEnv, _VectorEnvToAsync
from ray.rllib.utils.async_vector_env import AsyncVectorEnv


CompletedRollout = namedtuple("CompletedRollout",
                              ["episode_length", "episode_reward"])


def _to_async_env(env):
    if not isinstance(env, AsyncVectorEnv):
        if isinstance(env, MultiAgentEnv):
            env = _MultiAgentEnvToAsync(
                make_env=None, existing_envs=[env], num_envs=1)
        elif not isinstance(env, VectorEnv):
            env = VectorEnv.wrap(
                make_env=None, existing_envs=[env], num_envs=1)
            env = _VectorEnvToAsync(env)
    return env


class SyncSampler(object):
    """This class interacts with the environment and tells it what to do.

    Note that batch_size is only a unit of measure here. Batches can
    accumulate and the gradient can be calculated on up to 5 batches.

    This class provides data on invocation, rather than on a separate
    thread."""

    def __init__(
            self, env, policies, policy_mapping_fn, obs_filters,
            num_local_steps, horizon=None, pack=False):
        self.async_vector_env = _to_async_env(env)
        self.num_local_steps = num_local_steps
        self.horizon = horizon
        self.policies = policies
        self.policy_mapping_fn = policy_mapping_fn
        self._obs_filters = obs_filters
        self.rollout_provider = _env_runner(
            self.async_vector_env, self.policies, self.policy_mapping_fn,
            self.num_local_steps, self.horizon, self._obs_filters, pack)
        self.metrics_queue = queue.Queue()

    def get_data(self):
        while True:
            item = next(self.rollout_provider)
            if isinstance(item, CompletedRollout):
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


class AsyncSampler(threading.Thread):
    """This class interacts with the environment and tells it what to do.

    Note that batch_size is only a unit of measure here. Batches can
    accumulate and the gradient can be calculated on up to 5 batches."""

    def __init__(
            self, env, policies, policy_mapping_fn, obs_filters,
            num_local_steps, horizon=None, pack=False):
        for _, f in obs_filters.items():
            assert getattr(f, "is_concurrent", False), \
                "Observation Filter must support concurrent updates."
        self.async_vector_env = _to_async_env(env)
        threading.Thread.__init__(self)
        self.queue = queue.Queue(5)
        self.metrics_queue = queue.Queue()
        self.num_local_steps = num_local_steps
        self.horizon = horizon
        self.policies = policies
        self.policy_mapping_fn = policy_mapping_fn
        self._obs_filters = obs_filters
        self.daemon = True
        self.pack = pack

    def run(self):
        try:
            self._run()
        except BaseException as e:
            self.queue.put(e)
            raise e

    def _run(self):
        rollout_provider = _env_runner(
            self.async_vector_env, self.policies, self.policy_mapping_fn,
            self.num_local_steps, self.horizon, self._obs_filters, self.pack)
        while True:
            # The timeout variable exists because apparently, if one worker
            # dies, the other workers won't die with it, unless the timeout is
            # set to some large number. This is an empirical observation.
            item = next(rollout_provider)
            if isinstance(item, CompletedRollout):
                self.metrics_queue.put(item)
            else:
                self.queue.put(item, timeout=600.0)

    def get_data(self):
        rollout = self.queue.get(timeout=600.0)

        # Propagate errors
        if isinstance(rollout, BaseException):
            raise rollout

        # We can't auto-concat rollouts in vector mode
        if self.async_vector_env.num_envs > 1:
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


def _env_runner(
        async_vector_env, policies, policy_mapping_fn, num_local_steps,
        horizon, obs_filters, pack):
    """This implements the common experience collection logic.

    Args:
        async_vector_env (AsyncVectorEnv): env implementing AsyncVectorEnv.
        policies (dict): Map of policy ids to PolicyGraph instances.
        policy_mapping_fn (func): Function that maps agent ids to policy ids.
            This is called when an agent first enters the environment. The
            agent is then "bound" to the returned policy for the episode.
        num_local_steps (int): Number of episode steps before `SampleBatch` is
            yielded. Set to infinity to yield complete episodes.
        horizon (int): Horizon of the episode.
        obs_filters (dict): Map of policy id to filter used to process
            observations for the policy.
        pack (bool): Whether to pack multiple episodes into each batch. This
            guarantees batches will be exactly `num_local_steps` in size.

    Yields:
        rollout (SampleBatch): Object containing state, action, reward,
            terminal condition, and other fields as dictated by `policy`.
    """

    try:
        if not horizon:
            horizon = async_vector_env.get_unwrapped().spec.max_episode_steps
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
            return MultiAgentSampleBatchBuilder(policies)

    episodes = defaultdict(
        lambda: _Episode(policies, policy_mapping_fn, get_batch_builder))

    while True:
        # Get observations from all ready agents
        unfiltered_obs, rewards, dones, infos, off_policy_actions = \
            async_vector_env.poll()

        # Map of policy_id to list of (env_id, agent_id, obs, rnn_state)
        ready_agents = collections.defaultdict(list)

        # For each environment
        for env_id, agent_obs in unfiltered_obs.items():
            episode = episodes[env_id]
            episode.length += 1
            episode.add_agent_rewards(rewards[env_id])

            # Check episode termination conditions
            if dones[env_id]["__all__"] or episode.length >= horizon:
                all_done = True
                yield CompletedRollout(episode.length, episode.total_reward)
            else:
                all_done = False

            # For each agent in the environment
            for agent_id, raw_obs in agent_obs.items():
                policy_id = episode.policy_for(agent_id)
                filtered_obs = obs_filters[policy_id](raw_obs)
                agent_done = all_done or dones[env_id][agent_id]
                if not agent_done:
                    # Queue for policy evaluation
                    ready_agents[policy_id].append(
                        (env_id, agent_id, filtered_obs,
                         episode.rnn_state_for(agent_id)))

                last_observation = episode.last_observation_for(agent_id)
                episode.set_last_observation(agent_id, filtered_obs)
                if last_observation is None:
                    continue  # This is the initial observation for the agent

                # Record transition info
                episode.batch_builder.add_values(
                    agent_id,
                    policy_id,
                    t=episode.length - 1,
                    obs=last_observation,
                    actions=episode.last_action_for(agent_id),
                    rewards=rewards[env_id][agent_id],
                    dones=agent_done,
                    infos=infos[env_id][agent_id],
                    new_obs=filtered_obs,
                    **episode.last_pi_info_for(agent_id))

            # Cut the batch if we're not packing multiple episodes into one,
            # or if we've exceeded the requested batch size.
            if (all_done and not pack) or episode.length >= num_local_steps:
                yield episode.batch_builder.build_and_reset(
                    policy.postprocess_trajectory)
            elif all_done:
                # Make sure postprocessor never goes across episode boundaries
                episode.batch_builder.postprocess_batch_so_far(
                    policy.postprocess_trajectory)

            if all_done:
                # Handle episode termination
                batch_builder_pool.append(episode.batch_builder)
                del episodes[env_id]
                resetted_obs = async_vector_env.try_reset(env_id)
                if resetted_obs is None:
                    # Reset not supported, drop this env from the ready list
                    assert horizon == float("inf"), \
                        "Setting episode horizon requires reset() support."
                else:
                    # Reset successful, put in the new obs as ready
                    episode = episodes[eid]
                    for agent_id, raw_obs in resetted_obs.items():
                        policy_id = episode.policy_for(agent_id)
                        filtered_obs = obs_filters[policy_id](raw_obs)
                        episode.set_last_observation(agent_id, filtered_obs)
                        # Queue for policy evaluation
                        ready_agents[policy_id].append(
                            (env_id, agent_id, filtered_obs,
                             episode.rnn_state_for(agent_id)))

        if not ready_eids:
            continue  # No actions to take

        # Compute action for ready envs
        ready_rnn_state_cols = _to_column_format(ready_rnn_states)
        actions, new_rnn_state_cols, pi_info_cols = policy.compute_actions(
            ready_obs, ready_rnn_state_cols, is_training=True)

        # Add RNN state info
        for f_i, column in enumerate(ready_rnn_state_cols):
            pi_info_cols["state_in_{}".format(f_i)] = column
        for f_i, column in enumerate(new_rnn_state_cols):
            pi_info_cols["state_out_{}".format(f_i)] = column

        # Return computed actions to ready envs. We also send to envs that have
        # taken off-policy actions; those envs are free to ignore the action.
        async_vector_env.send_actions(dict(zip(ready_eids, actions)))

        # Store the computed action info
        for i, eid in enumerate(ready_eids):
            episode = episodes[eid]
            if eid in off_policy_actions:
                episode.last_action = off_policy_actions[eid]
            else:
                episode.last_action = actions[i]
            episode.rnn_state = [column[i] for column in new_rnn_state_cols]
            episode.last_pi_info = {
                k: column[i] for k, column in pi_info_cols.items()}


def _to_column_format(rnn_state_rows):
    num_cols = len(rnn_state_rows[0])
    return [
        [row[i] for row in rnn_state_rows] for i in range(num_cols)]


class _Episode(object):
    def __init__(self, init_rnn_state, batch_builder_factory):
        self.rnn_state = init_rnn_state
        self.batch_builder = batch_builder_factory()
        self.last_action = None
        self.last_observation = None
        self.last_pi_info = None
        self.total_reward = 0.0
        self.length = 0

    def last_action_flat(self):
        # Concatenate multiagent actions
        if isinstance(self.last_action, list):
            return np.concatenate(self.last_action, axis=0).flatten()
        return self.last_action
