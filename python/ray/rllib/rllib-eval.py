#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import multiprocessing
import os
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
    def __init__(self, env, alg, stopping_criterion, out_dir, i, config):
        self.alg = alg
        self.env = env
        self.config = config
        # TODO(rliaw): Stopping criterion needs direction (min or max)
        self.stopping_criterion = stopping_criterion
        self.agent = None
        self.out_dir = out_dir
        self.i = i

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
        self.agent = None

    def train_remote(self):
        return self.agent.train.remote()

    def should_stop(self, result):
        # should take an arbitrary (set) of key, value specified by config
        return any(getattr(result, criteria) >= stop_value
                    for criteria, stop_value in self.stopping_criterion.items())

    def param_str(self):
        return '_'.join(
            [k + '=' + '%d' % v for k, v in self.config.items()])

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
        for p, val in cfg.items():
            # TODO(rliaw): standardize 'distribution' keywords and processing
            if type(val) == str and val.startswith('Distribution'):
                sample_params = [int(x) for x in val[val.find('(')+1:val.find(')')].split(',')]
                cfg[p] = int(np.random.uniform(*sample_params))
        return cfg

    for exp_name, exp_cfg in configuration.items():
        if 'search' in configuration:
            np.random.seed(exp_cfg['search']['search_seed'])
        env_name = exp_cfg['env']
        alg_name = exp_cfg['alg']
        stopping_criterion = exp_cfg['stop']
        out_dir = '/tmp/rllib/' + exp_name
        os.makedirs(out_dir, exist_ok=True)
        for i in range(exp_cfg['max_trials']):
            experiments.append(Experiment(
                env_name, alg_name, stopping_criterion, out_dir, i,
                resolve(exp_cfg['parameters'])))

    return experiments


STOPPED = 'STOPPED'
RUNNING = 'RUNNING'
TERMINATED = 'TERMINATED'


class ExperimentState(object):
    def __init__(self):
        self.state = STOPPED
        self.last_result = None

    def __repr__(self):
        if self.last_result is None:
            return self.state
        return '{}, {} seconds, {} iters, {} reward'.format(
            self.state,
            self.last_result.


class ExperimentRunner(object):

    def __init__(self, experiments):
        self._experiments = experiments
        self._status = {e: ExperimentState() for exp in self._experiments}
        self._pending = {}
        self._resources = {
            'cpu': multiprocessing.cpu_count()
        }

    def is_finished(self):
        for (exp, status) in self._status.items():
            if status.state in [STOPPED, RUNNING]:
                return False
        return True

    def can_launch_more(self):
        exp = self._get_runnable()
        return exp is not None

    def launch_experiment(self):
        exp = self._get_runnable()
        self._status[exp].state = RUNNING
        self._deduct_resources(exp.resource_requirements())
        exp.start()
        self._pending[exp.train_remote()] = exp

    def process_events(self):
        [result_id], _ = ray.wait(self._pending.keys())
        exp = self._pending[result_id]
        del self._pending[result_id]
        result = ray.get(result_id)
        if exp.should_stop(result):
            status = self._status[exp]
            status.state = TERMINATED
            self._status[exp] = TERMINATED
            self._status[exp
        else:

    def _get_runnable(self):
        for (exp, status) in self._status.items():
            if (status.state == STOPPED and
                    self._has_resources(exp.resource_requirements())):
                return exp
        return None

    def _has_resources(self, resources):
        for k, v in resources.items():
            if self.resources[k] < v:
                return False
        return True

    def _deduct_resources(self, resources):
        for k, v in resources.items():
            self.resources[k] -= v
            assert self.resouces[k] >= 0

    def launch_to_capacity(self):
        print('Launching experiments...')
        self.experiment_pool = {exp.train_remote(): exp for exp in experiments}

    def run(self):
        # launch enough within resource constraint
        self.launch()

        while self.experiment_pool:
            [next_agent], waiting_agents = ray.wait(list(self.experiment_pool.keys()))
            exp = self.experiment_pool.pop(next_agent)
            result = ray.get(next_agent)

            # self.write_results()

            if exp.should_stop(result):
                # TODO(rliaw): self.save_best_results
                print('{} *** FINISHED ***: {}'.format(exp, result))
            else:
                print('{} progress: {}'.format(exp, result))
                self.experiment_pool[exp.train_remote()] = exp

        return self.best_results

    def write_results(self):
        raise NotImplementedError


class Policy(object):
    def process_update(self, state):


if __name__ == '__main__':
    experiments = parse_configuration(sys.argv[1])
    runner = ExperimentRunner(experiments)
    ray.init()

    while not runner.is_finished():
        print('== Running ==\n', runner)
        while runner.can_launch_more():
            runner.launch_experiment()
        runner.process_events()

    print('== Completed ==\n', runner)
