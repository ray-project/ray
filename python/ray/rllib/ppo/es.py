from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

import ray
from ray.rllib.agent import Agent
from ray.rllib.ppo import ppo
from ray.tune.result import TrainingResult


OUTER_CONFIG = {
    "es_num_replicas": 2,
    "es_noise_stdev": 0.02,
    "es_iteration_time": 10,
}

DEFAULT_CONFIG = dict(ppo.DEFAULT_CONFIG, **OUTER_CONFIG)


class PPOESAgent(Agent):
    _agent_name = "PPO_ES"
    _default_config = DEFAULT_CONFIG

    def _init(self):
        inner_config = self.config.copy()
        for k in OUTER_CONFIG.keys():
            del inner_config[k]
        inner_config["verbose"] = False
        self.replicas = [
            ray.remote(ppo.PPOAgent).remote(self.env_creator, inner_config)
            for _ in range(self.config["es_num_replicas"])]

    def _train(self):
        scores = {}
        pending = {r.train.remote(): r for r in self.replicas}
        start = time.time()
        best_result = None
        while pending:
            [result], _ = ray.wait(list(pending.keys()))
            agent = pending[result]
            del pending[result]
            result = ray.get(result)
            if (not best_result or result.episode_reward_mean >
                    best_result.episode_reward_mean):
                best_result = result
            scores[agent] = result.episode_reward_mean
            if time.time() - start < self.config["es_iteration_time"]:
                pending[agent.train.remote()] = agent

        by_score = sorted([(score, agent) for (agent, score) in scores.items()])
        print("Agent rankings: {}".format(by_score))

        # Now clone the top half and add random perturbations.
        ranked = [agent for (_, agent) in by_score]
        mid = len(ranked) // 2
        top_half, bottom_half = ranked[mid:], ranked[:mid]
        for winner, loser in zip(top_half, bottom_half):
            loser.restore.remote(winner.save.remote())
            loser.perturb.remote(self.config["es_noise_stdev"])

        return best_result
