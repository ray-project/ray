from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six.moves.queue as queue
import threading
from ray.rllib.a3c.common import CompletedRollout, get_filter
import functools


def lock_wrap(func, lock):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        with lock:
            return func(*args, **kwargs)
    return wrapper


class PartialRollout(object):
    """A piece of a complete rollout.

    We run our agent, and process its experience once it has processed enough
    steps.
    """

    fields = ["state", "action", "reward", "terminal", "features"]

    def __init__(self, extra_fields=None):
        if extra_fields:
            self.fields.extend(extra_fields)
        self.data = {k: [] for k in self.fields}
        self.last_r = 0.0

    def add(self, **kwargs):
        for k, v in kwargs.items():
            self.data[k] += [v]

    def extend(self, other_rollout):
        assert not self.is_terminal()
        assert all(k in other_rollout.fields for k in self.fields)
        for k, v in other_rollout.data.items():
            self.data[k].extend(v)
        self.last_r = other_rollout.last_r

    def is_terminal(self):
        return self.data["terminal"][-1]


class AsyncSampler(threading.Thread):
    """This thread interacts with the environment and tells it what to do."""
    def __init__(self, env, policy, num_local_steps, obs_filter_config):
        threading.Thread.__init__(self)
        self.queue = queue.Queue(5)
        self.metrics_queue = queue.Queue()
        self.num_local_steps = num_local_steps
        self.env = env
        self.policy = policy
        self.obs_filter = get_filter(
            obs_filter_config, env.observation_space.shape)
        self.obs_f_lock = threading.Lock()

    def start_runner(self):
        self.start()

    def run(self):
        try:
            self._run()
        except BaseException as e:
            self.queue.put(e)
            raise e

    def replace_obs_filter(self, other_filter):
        with self.obs_f_lock:
            new_filter = other_filter.copy()
            new_filter.update(self.obs_filter)
            self.obs_filter.copy(new_filter)
            # TODO(rliaw): Make sure this is actually updated on separate thread

    def _run(self):
        safe_obs_filter = lock_wrap(self.obs_filter, self.obs_f_lock)
        rollout_provider = env_runner(
            self.env, self.policy, self.num_local_steps, safe_obs_filter)
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


def env_runner(env, policy, num_local_steps, obs_filter):
    """This implements the logic of the thread runner.

    It continually runs the policy, and as long as the rollout exceeds a
    certain length, the thread runner appends the policy to the queue.
    """
    last_state = obs_filter(env.reset())
    timestep_limit = env.spec.tags.get("wrapper_config.TimeLimit"
                                       ".max_episode_steps")
    last_features = features = policy.get_initial_features()
    length = 0
    rewards = 0
    rollout_number = 0

    while True:
        terminal_end = False
        # TODO(rliaw): Modify Policies to match
        rollout = PartialRollout(extra_fields=policy.other_output)

        for _ in range(num_local_steps):
            action, info = policy.compute_action(last_state, *last_features)
            if policy.is_recurrent:
                features = info["features"]
                del info["features"]
            # Argmax to convert from one-hot.
            state, reward, terminal, info = env.step(action)
            state = obs_filter(state)

            length += 1
            rewards += reward
            if length >= timestep_limit:
                terminal = True

            # Collect the experience.
            rollout.add(state=last_state,
                        action=action,
                        reward=reward,
                        terminal=terminal,
                        features=last_features,
                        **info)

            last_state = state
            last_features = features

            if terminal:
                terminal_end = True
                yield CompletedRollout(length, rewards)

                if (length >= timestep_limit or
                        not env.metadata.get("semantics.autoreset")):
                    last_state = env.reset()
                    last_features = policy.get_initial_features()
                    rollout_number += 1
                    length = 0
                    rewards = 0
                    break

        if not terminal_end:
            rollout.last_r = policy.value(last_state, *last_features)

        # Once we have enough experience, yield it, and have the ThreadRunner
        # place it on a queue.
        yield rollout
