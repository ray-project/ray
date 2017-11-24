from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import six.moves.queue as queue
import threading
from ray.rllib.a3c.common import CompletedRollout


class PartialRollout(object):
    """A piece of a complete rollout.

    We run our agent, and process its experience once it has processed enough
    steps.
    """

    fields = ["state", "action", "reward", "terminal"]

    def __init__(self, extra_fields=None):
        if extra_fields:
            self.fields.extend(extra_fields)
        self.data = {k: [] for k in self._fields}
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
    def __init__(self, env, policy, num_local_steps):
        threading.Thread.__init__(self)
        self.queue = queue.Queue(5)
        self.metrics_queue = queue.Queue()
        self.num_local_steps = num_local_steps
        self.env = env
        self.last_features = None
        self.policy = policy

    def start_runner(self):
        self.start()

    def run(self):
        try:
            self._run()
        except BaseException as e:
            self.queue.put(e)
            raise e

    def _run(self):
        rollout_provider = env_runner(
            self.env, self.policy, self.num_local_steps)
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
        return self._pull_batch_from_queue()

    def _pull_batch_from_queue(self):
        """Take a rollout from the queue of the thread runner."""
        rollout = self.queue.get(timeout=600.0)
        if isinstance(rollout, BaseException):
            raise rollout
        while not rollout.terminal:
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


def env_runner(env, policy, num_local_steps):
    """This implements the logic of the thread runner.

    It continually runs the policy, and as long as the rollout exceeds a
    certain length, the thread runner appends the policy to the queue.
    """
    last_state = env.reset()
    timestep_limit = env.spec.tags.get("wrapper_config.TimeLimit"
                                       ".max_episode_steps")
    last_features = policy.get_initial_features()
    length = 0
    rewards = 0
    rollout_number = 0

    while True:
        terminal_end = False
        rollout = PartialRollout(extra_fields=["value", "features"])

        for _ in range(num_local_steps):
            fetched = policy.compute_action(last_state, *last_features)
            action, value_, features = fetched[0], fetched[1], fetched[2:]
            # Argmax to convert from one-hot.
            state, reward, terminal, info = env.step(action)

            length += 1
            rewards += reward
            if length >= timestep_limit:
                terminal = True

            # Collect the experience.
            rollout.add(state=last_state,
                        action=action,
                        reward=reward,
                        terminal=terminal,
                        value=value_,
                        features=last_features)

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
            rollout.r = policy.value(last_state, *last_features)

        # Once we have enough experience, yield it, and have the ThreadRunner
        # place it on a queue.
        yield rollout
