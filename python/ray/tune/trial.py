from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import multiprocessing
import os
import random
import sys
import traceback

import numpy as np
import ray
import time
import yaml

from ray.rllib.agents import get_agent_class


PENDING = 'PENDING'
RUNNING = 'RUNNING'
TERMINATED = 'TERMINATED'
ERROR = 'ERROR'


class Trial(object):
    def __init__(
            self, env, alg, config={}, local_dir='/tmp/ray', agent_id=None,
            resources={'cpu': 1}, stopping_criterion={},
            checkpoint_freq=sys.maxsize, restore_path=None):
        # Immutable config
        self.env = env
        self.alg = alg
        self.config = config
        self.local_dir = local_dir
        self.agent_id = agent_id
        self.resources = resources
        self.stopping_criterion = stopping_criterion
        self.checkpoint_freq = checkpoint_freq
        self.restore_path = restore_path

        # Local trial state that is updated during the run
        self.last_result = None
        self.checkpoint_path = None
        self.agent = None
        self.status = PENDING

    def checkpoint(self):
        path = ray.get(self.agent.save.remote())
        print("checkpointed at " + path)
        self.checkpoint_path = path
        return path

    def start(self):
        self.status = RUNNING
        agent_cls = get_agent_class(self.alg)
        cls = ray.remote(num_gpus=self.resources.get('gpu', 0))(agent_cls)
        self.agent = cls.remote(
            self.env, self.config, self.local_dir, agent_id=self.agent_id)
        if self.restore_path:
            ray.get(self.agent.restore.remote(self.restore_path))

    def stop(self, error=False):
        if error:
            self.status = ERROR
        else:
            self.status = TERMINATED
        try:
            self.agent.stop.remote()
            self.agent.__ray_terminate__.remote(self.agent._ray_actor_id.id())
        except:
            print("Error stopping agent:", traceback.format_exc())
            self.status = ERROR
        finally:
            self.agent = None

    def train_remote(self):
        return self.agent.train.remote()

    def should_stop(self, result):
        # should take an arbitrary (set) of key, value specified by config
        return any(getattr(result, criteria) >= stop_value
                    for criteria, stop_value in self.stopping_criterion.items())

    def should_checkpoint(self):
        if self.checkpoint_freq is None:
            return False
        if self.checkpoint_path is None:
            return True
        return (self.last_result.training_iteration) % self.checkpoint_freq == 0

    def progress_string(self):
        if self.last_result is None:
            return self.status
        return '{}, {} s, {} ts, {} itrs, {} rew'.format(
            self.status,
            int(self.last_result.time_total_s),
            int(self.last_result.timesteps_total),
            self.last_result.training_iteration,
            round(self.last_result.episode_reward_mean, 1))

    def update_progress(self, new_result):
        self.last_result = new_result

    def __str__(self):
        identifier = '{}_{}'.format(self.alg, self.env)
        if self.agent_id:
            identifier += '_' + self.agent_id
        return identifier

    def __eq__(self, other):
        return str(self) == str(other)

    def __hash__(self):
        return hash(str(self))
