from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import namedtuple
import numpy as np
import tensorflow as tf
import six.moves.queue as queue
import scipy.signal
import threading


def discount(x, gamma):
    return scipy.signal.lfilter([1], [1, -gamma], x[::-1], axis=0)[::-1]


def process_rollout(rollout, gamma, lambda_=1.0):
    """Given a rollout, compute its returns and the advantage."""
    batch_si = np.asarray(rollout.states)
    batch_a = np.asarray(rollout.actions)
    rewards = np.asarray(rollout.rewards)
    vpred_t = np.asarray(rollout.values + [rollout.r])

    rewards_plus_v = np.asarray(rollout.rewards + [rollout.r])
    batch_r = discount(rewards_plus_v, gamma)[:-1]
    delta_t = rewards + gamma * vpred_t[1:] - vpred_t[:-1]
    # This formula for the advantage comes "Generalized Advantage Estimation":
    # https://arxiv.org/abs/1506.02438
    batch_adv = discount(delta_t, gamma * lambda_)

    features = rollout.features[0]
    return Batch(batch_si, batch_a, batch_adv, batch_r, rollout.terminal,
                 features)


Batch = namedtuple(
    "Batch", ["si", "a", "adv", "r", "terminal", "features"])

CompletedRollout = namedtuple(
    "CompletedRollout", ["episode_length", "episode_reward"])


class PartialRollout(object):
    """A piece of a complete rollout.

    We run our agent, and process its experience once it has processed enough
    steps.
    """
    def __init__(self):
        self.states = []
        self.actions = []
        self.rewards = []
        self.values = []
        self.r = 0.0
        self.terminal = False
        self.features = []

    def add(self, state, action, reward, value, terminal, features):
        self.states += [state]
        self.actions += [action]
        self.rewards += [reward]
        self.values += [value]
        self.terminal = terminal
        self.features += [features]

    def extend(self, other):
        assert not self.terminal
        self.states.extend(other.states)
        self.actions.extend(other.actions)
        self.rewards.extend(other.rewards)
        self.values.extend(other.values)
        self.r = other.r
        self.terminal = other.terminal
        self.features.extend(other.features)


class RunnerThread(threading.Thread):
    """This thread interacts with the environment and tells it what to do."""
    def __init__(self, env, policy, num_local_steps, visualise=False):
        threading.Thread.__init__(self)
        self.queue = queue.Queue(5)
        self.metrics_queue = queue.Queue()
        self.num_local_steps = num_local_steps
        self.env = env
        self.last_features = None
        self.policy = policy
        self.daemon = True
        self.sess = None
        self.summary_writer = None
        self.visualise = visualise

    def start_runner(self, sess, summary_writer):
        self.sess = sess
        self.summary_writer = summary_writer
        self.start()

    def run(self):
        try:
            with self.sess.as_default():
                self._run()
        except BaseException as e:
            self.queue.put(e)
            raise e

    def _run(self):
        rollout_provider = env_runner(
            self.env, self.policy, self.num_local_steps,
            self.summary_writer, self.visualise)
        while True:
            # The timeout variable exists because apparently, if one worker
            # dies, the other workers won't die with it, unless the timeout is
            # set to some large number. This is an empirical observation.
            item = next(rollout_provider)
            if isinstance(item, CompletedRollout):
                self.metrics_queue.put(item)
            else:
                self.queue.put(item, timeout=600.0)


def env_runner(env, policy, num_local_steps, summary_writer, render):
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
        rollout = PartialRollout()

        for _ in range(num_local_steps):
            fetched = policy.compute_actions(last_state, *last_features)
            action, value_, features = fetched[0], fetched[1], fetched[2:]
            # Argmax to convert from one-hot.
            state, reward, terminal, info = env.step(action.argmax())
            if render:
                env.render()

            length += 1
            rewards += reward
            if length >= timestep_limit:
                terminal = True

            # Collect the experience.
            rollout.add(last_state, action, reward, value_, terminal,
                        last_features)

            last_state = state
            last_features = features

            if info:
                summary = tf.Summary()
                for k, v in info.items():
                    summary.value.add(tag=k, simple_value=float(v))
                summary_writer.add_summary(summary, rollout_number)
                summary_writer.flush()

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
