from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
"""A simple multi-agent env with two agents playing rock paper scissors.

This demonstrates running the following policies in competition:
    (1) heuristic policy of repeating the same move
    (2) heuristic policy of beating the last opponent move
    (3) LSTM/feedforward PG policies
    (4) LSTM policy with custom safety loss
"""

import random
from gym.spaces import Discrete

from ray import tune
from ray.rllib.agents.pg.pg import PGTrainer
from ray.rllib.agents.pg.pg_policy import PGTFPolicy
from ray.rllib.policy.policy import Policy
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.utils import try_import_tf

tf = try_import_tf()

ROCK = 0
PAPER = 1
SCISSORS = 2


class RockPaperScissorsEnv(MultiAgentEnv):
    """Two-player environment for rock paper scissors.

    The observation is simply the last opponent action."""

    def __init__(self, _):
        self.action_space = Discrete(3)
        self.observation_space = Discrete(3)
        self.player1 = "player1"
        self.player2 = "player2"
        self.last_move = None
        self.num_moves = 0

    def reset(self):
        self.last_move = (0, 0)
        self.num_moves = 0
        return {
            self.player1: self.last_move[1],
            self.player2: self.last_move[0],
        }

    def step(self, action_dict):
        move1 = action_dict[self.player1]
        move2 = action_dict[self.player2]
        self.last_move = (move1, move2)
        obs = {
            self.player1: self.last_move[1],
            self.player2: self.last_move[0],
        }
        r1, r2 = {
            (ROCK, ROCK): (0, 0),
            (ROCK, PAPER): (-1, 1),
            (ROCK, SCISSORS): (1, -1),
            (PAPER, ROCK): (1, -1),
            (PAPER, PAPER): (0, 0),
            (PAPER, SCISSORS): (-1, 1),
            (SCISSORS, ROCK): (-1, 1),
            (SCISSORS, PAPER): (1, -1),
            (SCISSORS, SCISSORS): (0, 0),
        }[move1, move2]
        rew = {
            self.player1: r1,
            self.player2: r2,
        }
        self.num_moves += 1
        done = {
            "__all__": self.num_moves >= 10,
        }
        return obs, rew, done, {}


class AlwaysSameHeuristic(Policy):
    """Pick a random move and stick with it for the entire episode."""

    def __init__(self, observation_space, action_space, config):
        Policy.__init__(self, observation_space, action_space, config)

    def get_initial_state(self):
        return [random.choice([ROCK, PAPER, SCISSORS])]

    def compute_actions(self,
                        obs_batch,
                        state_batches,
                        prev_action_batch=None,
                        prev_reward_batch=None,
                        info_batch=None,
                        episodes=None,
                        **kwargs):
        return [x for x in state_batches[0]], state_batches, {}

    def learn_on_batch(self, samples):
        pass

    def get_weights(self):
        pass

    def set_weights(self, weights):
        pass


class BeatLastHeuristic(Policy):
    """Play the move that would beat the last move of the opponent."""

    def __init__(self, observation_space, action_space, config):
        Policy.__init__(self, observation_space, action_space, config)

    def compute_actions(self,
                        obs_batch,
                        state_batches,
                        prev_action_batch=None,
                        prev_reward_batch=None,
                        info_batch=None,
                        episodes=None,
                        **kwargs):
        def successor(x):
            if x[ROCK] == 1:
                return PAPER
            elif x[PAPER] == 1:
                return SCISSORS
            elif x[SCISSORS] == 1:
                return ROCK

        return [successor(x) for x in obs_batch], [], {}

    def learn_on_batch(self, samples):
        pass

    def get_weights(self):
        pass

    def set_weights(self, weights):
        pass


def run_same_policy():
    """Use the same policy for both agents (trivial case)."""

    tune.run("PG", config={"env": RockPaperScissorsEnv})


def run_heuristic_vs_learned(use_lstm=False, trainer="PG"):
    """Run heuristic policies vs a learned agent.

    The learned agent should eventually reach a reward of ~5 with
    use_lstm=False, and ~7 with use_lstm=True. The reason the LSTM policy
    can perform better is since it can distinguish between the always_same vs
    beat_last heuristics.
    """

    def select_policy(agent_id):
        if agent_id == "player1":
            return "learned"
        else:
            return random.choice(["always_same", "beat_last"])

    tune.run(
        trainer,
        stop={"timesteps_total": 400000},
        config={
            "env": RockPaperScissorsEnv,
            "gamma": 0.9,
            "num_workers": 4,
            "num_envs_per_worker": 4,
            "sample_batch_size": 10,
            "train_batch_size": 200,
            "multiagent": {
                "policies_to_train": ["learned"],
                "policies": {
                    "always_same": (AlwaysSameHeuristic, Discrete(3),
                                    Discrete(3), {}),
                    "beat_last": (BeatLastHeuristic, Discrete(3), Discrete(3),
                                  {}),
                    "learned": (None, Discrete(3), Discrete(3), {
                        "model": {
                            "use_lstm": use_lstm
                        }
                    }),
                },
                "policy_mapping_fn": tune.function(select_policy),
            },
        })


def run_with_custom_entropy_loss():
    """Example of customizing the loss function of an existing policy.

    This performs about the same as the default loss does."""

    def entropy_policy_gradient_loss(policy, batch_tensors):
        actions = batch_tensors["actions"]
        advantages = batch_tensors["advantages"]
        return (-0.1 * policy.action_dist.entropy() - tf.reduce_mean(
            policy.action_dist.logp(actions) * advantages))

    EntropyPolicy = PGTFPolicy.with_updates(
        loss_fn=entropy_policy_gradient_loss)
    EntropyLossPG = PGTrainer.with_updates(
        name="EntropyPG", get_policy_class=lambda _: EntropyPolicy)
    run_heuristic_vs_learned(use_lstm=True, trainer=EntropyLossPG)


if __name__ == "__main__":
    # run_same_policy()
    # run_heuristic_vs_learned(use_lstm=False)
    run_heuristic_vs_learned(use_lstm=False)
    # run_with_custom_entropy_loss()
