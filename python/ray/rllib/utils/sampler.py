from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six.moves.queue as queue
import threading
from collections import namedtuple
import numpy as np

from ray.rllib.optimizers.sample_batch import SampleBatchBuilder
from ray.rllib.utils.vector_env import VectorEnv


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
        if not hasattr(env, "vector_reset"):
            env = VectorEnv.wrap(make_env=None, existing_envs=[env])
        self.vector_env = env
        self.num_local_steps = num_local_steps
        self.horizon = horizon
        self.policy = policy
        self._obs_filter = obs_filter
        self.rollout_provider = _env_runner(self.vector_env, self.policy,
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
        if not hasattr(env, "vector_reset"):
            env = VectorEnv.wrap(make_env=None, existing_envs=[env])
        self.vector_env = env
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
        rollout_provider = _env_runner(self.vector_env, self.policy,
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
        if not hasattr(self.vector_env, "vector_width") or \
                self.vector_env.vector_width > 1:
            return rollout

        # Auto-concat rollouts; this is important for A3C perf
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
        vector_env, policy, num_local_steps, horizon, obs_filter, pack):
    """This implements the logic of the thread runner.

    It continually runs the policy, and as long as the rollout exceeds a
    certain length, the thread runner appends the policy to the queue. Yields
    when `timestep_limit` is surpassed, environment terminates, or
    `num_local_steps` is reached.

    Args:
        vector_env: env implementing vector_reset() and vector_step().
        policy: Policy used to interact with environment. Also sets fields
            to be included in `SampleBatch`
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
            horizon = vector_env.first_env().spec.max_episode_steps
    except Exception:
        print("Warning, no horizon specified, assuming infinite")
    if not horizon:
        horizon = 999999

    last_observations = [obs_filter(o) for o in vector_env.vector_reset()]
    vector_width = len(last_observations)

    def init_rnn():
        states = policy.get_initial_state()
        state_vector = [[s] for s in states]
        for _ in range(vector_width - 1):
            for i, s in enumerate(policy.get_initial_state()):
                state_vector[i] = s
        return state_vector

    last_rnn_states = init_rnn()

    rnn_states = last_rnn_states
    episode_lengths = [0] * vector_width
    episode_rewards = [0] * vector_width

    batch_builders = [
        SampleBatchBuilder() for _ in range(vector_width)]

    while True:
        actions, rnn_states, pi_infos = policy.compute_actions(
            last_observations, last_rnn_states, is_training=True)

        # Add RNN state info
        for f_i, column in enumerate(last_rnn_states):
            pi_infos["state_in_{}".format(f_i)] = column
        for f_i, column in enumerate(rnn_states):
            pi_infos["state_out_{}".format(f_i)] = column

        # Vectorized env step
        observations, rewards, dones, _ = vector_env.vector_step(actions)

        # Process transitions for each child environment
        for i in range(vector_width):
            episode_lengths[i] += 1
            episode_rewards[i] += rewards[i]
            observations[i] = obs_filter(observations[i])

            # Handle episode terminations
            if dones[i] or episode_lengths[i] >= horizon:
                done = True
                yield CompletedRollout(
                    episode_lengths[i], episode_rewards[i])
            else:
                done = False

            # Concatenate multiagent actions
            if isinstance(actions[i], list):
                actions[i] = np.concatenate(actions[i], axis=0).flatten()

            # Collect the experience.
            batch_builders[i].add_values(
                obs=last_observations[i],
                actions=actions[i],
                rewards=rewards[i],
                dones=done,
                new_obs=observations[i],
                **{k: v[i] for k, v in pi_infos.items()})

            if (done and not pack) or \
                    batch_builders[i].count >= num_local_steps:
                yield batch_builders[i].build_and_reset()

            if done:
                observations[i] = vector_env.reset_at(i)
                state = policy.get_initial_state()
                for f_i in enumerate(state):
                    last_rnn_states[f_i][i] = state[f_i]
                episode_lengths[i] = 0
                episode_rewards[i] = 0

        last_observations = observations
        last_rnn_states = rnn_states
