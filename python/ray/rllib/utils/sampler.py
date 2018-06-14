from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import defaultdict, namedtuple
import numpy as np
import six.moves.queue as queue
import threading

from ray.rllib.optimizers.sample_batch import SampleBatchBuilder
from ray.rllib.utils.vector_env import VectorEnv
from ray.rllib.utils.async_vector_env import AsyncVectorEnv, _VectorEnvToAsync


CompletedRollout = namedtuple("CompletedRollout",
                              ["episode_length", "episode_reward"])


class SyncSampler(object):
    """This class interacts with the environment and tells it what to do.

    Note that batch_size is only a unit of measure here. Batches can
    accumulate and the gradient can be calculated on up to 5 batches.

    This class provides data on invocation, rather than on a separate
    thread."""

    def __init__(
            self, env, policy, obs_filter, num_local_steps,
            horizon=None, pack=False):
        if not isinstance(env, AsyncVectorEnv):
            if not isinstance(env, VectorEnv):
                env = VectorEnv.wrap(make_env=None, existing_envs=[env])
            env = _VectorEnvToAsync(env)
        self.async_vector_env = env
        self.num_local_steps = num_local_steps
        self.horizon = horizon
        self.policy = policy
        self._obs_filter = obs_filter
        self.rollout_provider = _env_runner(self.async_vector_env, self.policy,
                                            self.num_local_steps, self.horizon,
                                            self._obs_filter, pack)
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
            self, env, policy, obs_filter, num_local_steps,
            horizon=None, pack=False):
        assert getattr(
            obs_filter, "is_concurrent",
            False), ("Observation Filter must support concurrent updates.")
        if not isinstance(env, AsyncVectorEnv):
            if not isinstance(env, VectorEnv):
                env = VectorEnv.wrap(make_env=None, existing_envs=[env])
            env = _VectorEnvToAsync(env)
        self.async_vector_env = env
        threading.Thread.__init__(self)
        self.queue = queue.Queue(5)
        self.metrics_queue = queue.Queue()
        self.num_local_steps = num_local_steps
        self.horizon = horizon
        self.policy = policy
        self._obs_filter = obs_filter
        self.daemon = True
        self.pack = pack

    def run(self):
        try:
            self._run()
        except BaseException as e:
            self.queue.put(e)
            raise e

    def _run(self):
        rollout_provider = _env_runner(self.async_vector_env, self.policy,
                                       self.num_local_steps, self.horizon,
                                       self._obs_filter, self.pack)
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
        async_vector_env, policy, num_local_steps, horizon, obs_filter, pack):
    """This implements the logic of the thread runner.

    It continually runs the policy, and as long as the rollout exceeds a
    certain length, the thread runner appends the policy to the queue. Yields
    when `timestep_limit` is surpassed, environment terminates, or
    `num_local_steps` is reached.

    Args:
        async_vector_env: env implementing AsyncVectorEnv.
        policy: Policy used to interact with environment. Also sets fields
            to be included in `SampleBatch`.
        num_local_steps: Number of steps before `SampleBatch` is yielded. Set
            to infinity to yield complete episodes.
        horizon: Horizon of the episode.
        obs_filter: Filter used to process observations.
        pack: Whether to pack multiple episodes into each batch. This
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
            return SampleBatchBuilder()

    episodes = defaultdict(
        lambda: _Episode(policy.get_initial_state(), get_batch_builder))

    while True:
        # Get observations from ready envs
        unfiltered_obs, rewards, dones, _, off_policy_actions = \
            async_vector_env.poll()
        ready_eids = []
        ready_obs = []
        ready_rnn_states = []

        # Process and record the new observations
        for eid, raw_obs in unfiltered_obs.items():
            episode = episodes[eid]
            filtered_obs = obs_filter(raw_obs)
            ready_eids.append(eid)
            ready_obs.append(filtered_obs)
            ready_rnn_states.append(episode.rnn_state)

            if episode.last_observation is None:
                episode.last_observation = filtered_obs
                continue  # This is the initial observation after a reset

            episode.length += 1
            episode.total_reward += rewards[eid]

            # Handle episode terminations
            if dones[eid] or episode.length >= horizon:
                done = True
                yield CompletedRollout(episode.length, episode.total_reward)
            else:
                done = False

            episode.batch_builder.add_values(
                obs=episode.last_observation,
                actions=episode.last_action_flat(),
                rewards=rewards[eid],
                dones=done,
                new_obs=filtered_obs,
                **episode.last_pi_info)

            # Cut the batch if we're not packing multiple episodes into one,
            # or if we've exceeded the requested batch size.
            if (done and not pack) or \
                    episode.batch_builder.count >= num_local_steps:
                yield episode.batch_builder.build_and_reset(
                    policy.postprocess_trajectory)
            elif done:
                # Make sure postprocessor never goes across episode boundaries
                episode.batch_builder.postprocess_batch_so_far(
                    policy.postprocess_trajectory)

            if done:
                # Handle episode termination
                batch_builder_pool.append(episode.batch_builder)
                del episodes[eid]
                resetted_obs = async_vector_env.try_reset(eid)
                if resetted_obs is None:
                    # Reset not supported, drop this env from the ready list
                    assert horizon == float("inf"), \
                        "Setting episode horizon requires reset() support."
                    ready_eids.pop()
                    ready_obs.pop()
                    ready_rnn_states.pop()
                else:
                    # Reset successful, put in the new obs as ready
                    episode = episodes[eid]
                    episode.last_observation = obs_filter(resetted_obs)
                    ready_obs[-1] = episode.last_observation
                    ready_rnn_states[-1] = episode.rnn_state
            else:
                episode.last_observation = filtered_obs

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
