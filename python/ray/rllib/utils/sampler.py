from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six.moves.queue as queue
import threading
from collections import namedtuple


def lock_wrap(func, lock):
    def wrapper(*args, **kwargs):
        with lock:
            return func(*args, **kwargs)
    return wrapper


class PartialRollout(object):
    """A piece of a complete rollout.

    We run our agent, and process its experience once it has processed enough
    steps.
    """

    fields = ["observations", "actions", "rewards", "terminal", "features"]

    def __init__(self, extra_fields=None):
        """Initializers internals. Maintains a `last_r` field
        in support of partial rollouts, used in bootstrapping advantage
        estimation.

        Args:
            extra_fields: Optional field for object to keep track.
        """
        if extra_fields:
            self.fields.extend(extra_fields)
        self.data = {k: [] for k in self.fields}
        self.last_r = 0.0

    def add(self, **kwargs):
        for k, v in kwargs.items():
            self.data[k] += [v]

    def extend(self, other_rollout):
        """Extends internal data structure. Assumes other_rollout contains
        data that occured afterwards."""

        assert not self.is_terminal()
        assert all(k in other_rollout.fields for k in self.fields)
        for k, v in other_rollout.data.items():
            self.data[k].extend(v)
        self.last_r = other_rollout.last_r

    def is_terminal(self):
        """Check if terminal.

        Returns:
            terminal (bool): if rollout has terminated."""
        return self.data["terminal"][-1]


CompletedRollout = namedtuple(
    "CompletedRollout", ["episode_length", "episode_reward"])


class SyncSampler(object):
    """This class interacts with the environment and tells it what to do.

    Note that batch_size is only a unit of measure here. Batches can
    accumulate and the gradient can be calculated on up to 5 batches.

    This class provides data on invocation, rather than on a separate
    thread."""
    async = False

    def __init__(
        self, env, policy, obs_filter, num_local_steps, horizon=None):
        self.num_local_steps = num_local_steps
        self.horizon = horizon
        self.env = env
        self.policy = policy
        self.obs_filter = obs_filter
        self.rollout_provider = env_runner(
            self.env, self.policy, self.num_local_steps, self.horizon,
            self.obs_filter)
        self.metrics_queue = queue.Queue()

    def update_obs_filter(self, other_filter):
        """Method to update observation filter with copy from driver.
        Since this class is synchronous, updating the observation
        filter should be a straightforward replacement

        Args:
            other_filter: Another filter (of same type)."""
        self.obs_filter = other_filter.copy()

    def get_data(self):
        while True:
            item = next(self.rollout_provider)
            if isinstance(item, CompletedRollout):
                self.metrics_queue.put(item)
            else:
                obsf_snapshot = self.obs_filter.copy()
                if hasattr(self.obs_filter, "clear_buffer"):
                    self.obs_filter.clear_buffer()
                return item, obsf_snapshot

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
    async = True

    def __init__(
        self, env, policy, obs_filter, num_local_steps, horizon=None):
        threading.Thread.__init__(self)
        self.queue = queue.Queue(5)
        self.metrics_queue = queue.Queue()
        self.num_local_steps = num_local_steps
        self.horizon = horizon
        self.env = env
        self.policy = policy
        self.obs_filter = obs_filter
        self.obs_f_lock = threading.Lock()

    def start_runner(self):
        self.start()

    def run(self):
        try:
            self._run()
        except BaseException as e:
            self.queue.put(e)
            raise e

    def update_obs_filter(self, other_filter):
        """Method to update observation filter with copy from driver.
        Applies delta since last `clear_buffer` to given new filter,
        and syncs current filter to new filter. `self.obs_filter` is
        kept in place due to the `lock_wrap`.

        Args:
            other_filter: Another filter (of same type)."""
        with self.obs_f_lock:
            new_filter = other_filter.copy()
            # Applies delta to filter, including buffer
            new_filter.update(self.obs_filter, copy_buffer=True)
            # copies everything back into original filter - needed
            # due to `lock_wrap`
            self.obs_filter.sync(new_filter)

    def _run(self):
        """Sets observation filter into an atomic region and starts
        other thread for running."""
        safe_obs_filter = lock_wrap(self.obs_filter, self.obs_f_lock)
        rollout_provider = env_runner(
            self.env, self.policy, self.num_local_steps,
            self.horizon, safe_obs_filter)
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
        """Gets currently accumulated data and a snapshot of the current
        observation filter. The snapshot also clears the accumulated delta.
        Note that in between getting the rollout and acquiring the lock,
        the other thread can run, resulting in slight discrepamcies
        between data retrieved and filter statistics.

        Returns:
            rollout: trajectory data (unprocessed)
            obsf_snapshot: snapshot of observation filter.
        """

        rollout = self._pull_batch_from_queue()
        with self.obs_f_lock:
            obsf_snapshot = self.obs_filter.copy()
            if hasattr(self.obs_filter, "clear_buffer"):
                self.obs_filter.clear_buffer()
        return rollout, obsf_snapshot

    def _pull_batch_from_queue(self):
        """Take a rollout from the queue of the thread runner."""
        rollout = self.queue.get(timeout=600.0)
        if isinstance(rollout, BaseException):
            raise rollout
        while not rollout.is_terminal():
            try:
                part = self.queue.get_nowait()
                if isinstance(part, BaseException):
                    raise rollout
                rollout.extend(part)
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


def env_runner(env, policy, num_local_steps, horizon, obs_filter):
    """This implements the logic of the thread runner.

    It continually runs the policy, and as long as the rollout exceeds a
    certain length, the thread runner appends the policy to the queue.
    """
    last_observation = obs_filter(env.reset())
    horizon = horizon if horizon else env.spec.tags.get(
        "wrapper_config.TimeLimit.max_episode_steps")
    assert horizon > 0
    last_features = features = policy.get_initial_features()
    length = 0
    rewards = 0
    rollout_number = 0

    while True:
        terminal_end = False
        rollout = PartialRollout(extra_fields=policy.other_output)

        for _ in range(num_local_steps):
            action, pi_info = policy.compute_action(last_observation, *last_features)
            if policy.is_recurrent:
                features = pi_info["features"]
                del pi_info["features"]
            observation, reward, terminal, info = env.step(action)
            observation = obs_filter(observation)

            length += 1
            rewards += reward
            if length >= horizon:
                terminal = True

            # Collect the experience.
            rollout.add(observations=last_observation,
                        actions=action,
                        rewards=reward,
                        terminal=terminal,
                        features=last_features,
                        **pi_info)

            last_observation = observation
            last_features = features

            if terminal:
                terminal_end = True
                yield CompletedRollout(length, rewards)

                if (length >= horizon or
                        not env.metadata.get("semantics.autoreset")):
                    last_observation = obs_filter(env.reset())
                    last_features = policy.get_initial_features()
                    rollout_number += 1
                    length = 0
                    rewards = 0
                    break

        if not terminal_end:
            rollout.last_r = policy.value(last_observation, *last_features)

        # Once we have enough experience, yield it, and have the ThreadRunner
        # place it on a queue.
        yield rollout
