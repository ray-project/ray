#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import multiprocessing
import os
import random
import sys

import ray
import yaml
import numpy as np
import ray.rllib.ppo as ppo
import ray.rllib.es as es
import ray.rllib.dqn as dqn
import ray.rllib.a3c as a3c


# TODO(rliaw): Change prints to logging


# TODO(rliaw): Catalog for Agents (AgentCatalog.)
# New dependency - pyyaml?
AGENTS = {
    'PPO': (ppo.PPOAgent, ppo.DEFAULT_CONFIG),
    'ES': (es.ESAgent, es.DEFAULT_CONFIG),
    'DQN': (dqn.DQNAgent, dqn.DEFAULT_CONFIG),
    'A3C': (a3c.A3CAgent, a3c.DEFAULT_CONFIG)
}

class Experiment(object):
    def __init__(
            self, env, alg, stopping_criterion,
            out_dir, i, config, was_resolved, resources):
        self.alg = alg
        self.env = env
        self.config = config
        self.was_resolved = was_resolved
        self.resources = resources
        # TODO(rliaw): Stopping criterion needs direction (min or max)
        self.stopping_criterion = stopping_criterion
        self.agent = None
        self.out_dir = out_dir
        self.i = i

    def resource_requirements(self):
        return self.resources

    def start(self):
        (agent_class, agent_config) = AGENTS[self.alg]
        config = agent_config.copy()
        for k in self.config.keys():
            if k not in config:
                raise Exception(
                    'Unknown agent config `{}`, all agent configs: {}'.format(
                        k, config.keys()))
        config.update(self.config)
        # TODO(rliaw): make sure agent takes in SEED parameter
        self.agent = ray.remote(agent_class).remote(
            self.env, config, self.out_dir, 'trial_{}_{}'.format(
                self.i, self.param_str()))

    def stop(self):
        ray.get(self.agent.stop.remote())
        self.agent = None

    def train_remote(self):
        return self.agent.train.remote()

    def should_stop(self, result):
        # should take an arbitrary (set) of key, value specified by config
        return any(getattr(result, criteria) >= stop_value
                    for criteria, stop_value in self.stopping_criterion.items())

    def param_str(self):
        return '_'.join(
            [k + '=' + str(v) for k, v in self.config.items()
                if self.was_resolved[k]])

    def __str__(self):
        identifier = '{}_{}_{}'.format(self.alg, self.env, self.i)
        params = self.param_str()
        if params:
            identifier += '_' + params
        return identifier

    def __eq__(self, other):
        return str(self) == str(other)

    def __hash__(self):
        return hash(str(self))


def parse_configuration(yaml_file):
    ''' Parses yaml_file for specifying experiment setup
        and return Experiment objects, one for each trial '''
    with open(yaml_file) as f:
        configuration = yaml.load(f)

    experiments = []

    def resolve(agent_cfg):
        ''' Resolves issues such as distributions and such '''
        assert type(agent_cfg) == dict
        cfg = agent_cfg.copy()
        was_resolved = {}
        for p, val in cfg.items():
            # TODO(rliaw): standardize 'distribution' keywords and processing
            if type(val) == str and val.startswith('Distribution'):
                sample_params = [int(x) for x in val[val.find('(')+1:val.find(')')].split(',')]
                cfg[p] = int(np.random.uniform(*sample_params))
                was_resolved[p] = True
            elif type(val) == dict and 'eval' in val:
                cfg[p] = eval(val['eval'], {
                    'random': random,
                    'np': np,
                }, {})
                was_resolved[p] = True
            else:
                was_resolved[p] = False
        return cfg, was_resolved

    for exp_name, exp_cfg in configuration.items():
        if 'search' in configuration:
            np.random.seed(exp_cfg['search']['search_seed'])
        env_name = exp_cfg['env']
        alg_name = exp_cfg['alg']
        stopping_criterion = exp_cfg['stop']
        out_dir = 'file:///tmp/rllib/' + exp_name
        os.makedirs(out_dir, exist_ok=True)
        for i in range(exp_cfg['max_trials']):
            resolved, was_resolved = resolve(exp_cfg['parameters'])
            experiments.append(Experiment(
                env_name, alg_name, stopping_criterion, out_dir, i,
                resolved, was_resolved, exp_cfg['resources']))

    return experiments


PENDING = 'PENDING'
RUNNING = 'RUNNING'
TERMINATED = 'TERMINATED'


class ExperimentState(object):
    def __init__(self):
        self.state = PENDING
        self.last_result = None

    def __repr__(self):
        if self.last_result is None:
            return self.state
        return '{}, {} s, {} ts, {} itrs, {} rew'.format(
            self.state,
            int(self.last_result.time_total_s),
            int(self.last_result.timesteps_total),
            self.last_result.training_iteration + 1,
            round(self.last_result.episode_reward_mean, 1))


class ExperimentRunner(object):

    def __init__(self, experiments):
        self._experiments = experiments
        self._status = {e: ExperimentState() for e in self._experiments}
        self._pending = {}
        self._avail_resources = {
            'cpu': multiprocessing.cpu_count()
        }
        self._committed_resources = {k: 0 for k in self._avail_resources}

    def is_finished(self):
        for (exp, status) in self._status.items():
            if status.state in [PENDING, RUNNING]:
                return False
        return True

    def can_launch_more(self):
        exp = self._get_runnable()
        return exp is not None

    def launch_experiment(self):
        exp = self._get_runnable()
        self._status[exp].state = RUNNING
        self._commit_resources(exp.resource_requirements())
        exp.start()
        self._pending[exp.train_remote()] = exp

    def process_events(self):
        [result_id], _ = ray.wait(self._pending.keys())
        exp = self._pending[result_id]
        del self._pending[result_id]
        result = ray.get(result_id)
        print("result", result)
        status = self._status[exp]
        status.last_result = result

        if exp.should_stop(result):
            status.state = TERMINATED
            self._return_resources(exp.resource_requirements())
            exp.stop()
        else:
            self._pending[exp.train_remote()] = exp

        # TODO(ekl) also switch to other experiments if the current one
        # doesn't look promising, i.e. bandits

        # TODO(ekl) checkpoint periodically

    def _get_runnable(self):
        for exp in self._experiments:
            status = self._status[exp]
            if (status.state == PENDING and
                    self._has_resources(exp.resource_requirements())):
                return exp
        return None

    def _has_resources(self, resources):
        for k, v in resources.items():
            if self._avail_resources[k] < v:
                return False
        return True

    def _commit_resources(self, resources):
        for k, v in resources.items():
            self._avail_resources[k] -= v
            self._committed_resources[k] += v
            assert self._avail_resources[k] >= 0

    def _return_resources(self, resources):
        for k, v in resources.items():
            self._avail_resources[k] += v
            self._committed_resources[k] -= v
            assert self._committed_resources[k] >= 0


    def debug_string(self):
        statuses = [
            ' - {}:\t{}'.format(e, self._status[e]) for e in self._experiments]
        return 'Available resources: {}'.format(self._avail_resources) + \
            '\nCommitted resources: {}'.format(self._committed_resources) + \
            '\nAll experiments:\n' + '\n'.join(statuses)


if __name__ == '__main__':
    experiments = parse_configuration(sys.argv[1])
    runner = ExperimentRunner(experiments)
    ray.init()

    # TODO(ekl) implement crash recovery from status files
    
    def debug_print(title='Status'):
        print('== {} ==\n{}'.format(title, runner.debug_string()))
        print('Tensorboard dir: {}'.format(experiments[0].out_dir))
        print()

    debug_print('Starting')
    while not runner.is_finished():
        while runner.can_launch_more():
            runner.launch_experiment()
            debug_print()
        runner.process_events()
        debug_print()
    debug_print('Completed')
