import gym
import numpy as np
import random

from ray.rllib.examples.env.rock_paper_scissors import RockPaperScissors
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.view_requirement import ViewRequirement


class AlwaysSameHeuristic(Policy):
    """Pick a random move and stick with it for the entire episode."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.exploration = self._create_exploration()
        self.view_requirements.update({
            "state_in_0": ViewRequirement(
                "state_out_0",
                shift=-1,
                space=gym.spaces.Box(0, 100, shape=(), dtype=np.int32))
        })

    def get_initial_state(self):
        return [
            random.choice([
                RockPaperScissors.ROCK, RockPaperScissors.PAPER,
                RockPaperScissors.SCISSORS
            ])
        ]

    def compute_actions(self,
                        obs_batch,
                        state_batches=None,
                        prev_action_batch=None,
                        prev_reward_batch=None,
                        info_batch=None,
                        episodes=None,
                        **kwargs):
        return state_batches[0], state_batches, {}


class BeatLastHeuristic(Policy):
    """Play the move that would beat the last move of the opponent."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.exploration = self._create_exploration()

    def compute_actions(self,
                        obs_batch,
                        state_batches=None,
                        prev_action_batch=None,
                        prev_reward_batch=None,
                        info_batch=None,
                        episodes=None,
                        **kwargs):
        def successor(x):
            if x[RockPaperScissors.ROCK] == 1:
                return RockPaperScissors.PAPER
            elif x[RockPaperScissors.PAPER] == 1:
                return RockPaperScissors.SCISSORS
            elif x[RockPaperScissors.SCISSORS] == 1:
                return RockPaperScissors.ROCK

        return [successor(x) for x in obs_batch], [], {}

    def learn_on_batch(self, samples):
        pass

    def get_weights(self):
        pass

    def set_weights(self, weights):
        pass
