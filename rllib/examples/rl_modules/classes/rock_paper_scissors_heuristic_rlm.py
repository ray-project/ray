from collections import defaultdict

import numpy as np

from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override


class AlwaysSameHeuristicRLM(RLModule):
    """In rock-paper-scissors, always chooses the same action within an episode.

    The first move is random, all the following moves are the same as the first one.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._actions_per_vector_idx = defaultdict(int)

    @override(RLModule)
    def _forward_inference(self, batch, **kwargs):
        ret = []
        # Note that the obs is the previous move of the opponens (0-2). If it's 3, it
        # means that there was no previous move and thus, the episode just started.
        for i, obs in enumerate(batch[Columns.OBS]):
            if obs == 3:
                self._actions_per_vector_idx[i] = np.random.choice([0, 1, 2])
            ret.append(self._actions_per_vector_idx[i])
        return {Columns.ACTIONS: np.array(ret)}

    @override(RLModule)
    def _forward_exploration(self, batch, **kwargs):
        return self._forward_inference(batch, **kwargs)

    @override(RLModule)
    def _forward_train(self, batch, **kwargs):
        raise NotImplementedError(
            "AlwaysSameHeuristicRLM is not trainable! Make sure you do NOT include it "
            "in your `config.multi_agent(policies_to_train={...})` set."
        )


class BeatLastHeuristicRLM(RLModule):
    """In rock-paper-scissors, always acts such that it beats prev. move of opponent.

    The first move is random.

    For example, after opponent played `rock` (and this policy made a random
    move), the next move would be `paper`(to beat `rock`).
    """

    @override(RLModule)
    def _forward_inference(self, batch, **kwargs):
        """Returns the exact action that would beat the previous action of the opponent.

        The opponent's previous action is the current observation for this agent.

        Both action- and observation spaces are discrete. There are 3 actions available.
        (0-2) and 4 observations (0-2 plus 3, where 3 is the observation after the env
        reset, when no action has been taken yet). Thereby:
        0=Rock
        1=Paper
        2=Scissors
        3=[after reset] (observation space only)
        """
        return {
            Columns.ACTIONS: np.array(
                [self._pick_single_action(obs) for obs in batch[Columns.OBS]]
            ),
        }

    @override(RLModule)
    def _forward_exploration(self, batch, **kwargs):
        return self._forward_inference(batch, **kwargs)

    @override(RLModule)
    def _forward_train(self, batch, **kwargs):
        raise NotImplementedError(
            "BeatLastHeuristicRLM is not trainable! Make sure you do NOT include it in "
            "your `config.multi_agent(policies_to_train={...})` set."
        )

    @staticmethod
    def _pick_single_action(prev_opponent_obs):
        if prev_opponent_obs == 0:
            return 1
        elif prev_opponent_obs == 1:
            return 2
        elif prev_opponent_obs == 2:
            return 0
        else:
            return np.random.choice([0, 1, 2])
