import collections
import dataclasses
from typing import NewType

import numpy as np
from pettingzoo.utils import env

from ray.rllib.utils.typing import EpisodeType

AgentID = NewType("AgentID", env.AgentID)
ObsType = NewType("ObsType", env.ObsType)
ActionType = NewType("ActionType", env.ActionType)


@dataclasses.dataclass
class Matchup:
    p1: str
    p2: str
    prob: float


class Matchmaker:
    def __init__(self, matchups: list[Matchup]):
        self.matchups = matchups
        self.probs = [matchup.prob for matchup in matchups]
        self.current_matchups = collections.defaultdict(dict)

    def policy_mapping_fn(self, agent_id: str, episode: EpisodeType, **kwargs) -> str:
        """Policy mapping function that retrieves from the current matchup"""
        id_ = episode.env_id if hasattr(episode, "env_id") else episode.id_
        if self.current_matchups.get(id_) is None:
            # Sample a matchup
            sampled_matchup = np.random.choice(self.matchups, p=self.probs)

            # Randomize who is player 1 and player 2
            policies = [sampled_matchup.p1, sampled_matchup.p2]
            p1, p2 = np.random.choice(policies, size=2, replace=False)

            # Set this as the current episodes mapping
            self.current_matchups[id_]["p1"] = p1
            self.current_matchups[id_]["p2"] = p2

        pid = self.current_matchups[id_].pop(agent_id)

        if not self.current_matchups[id_]:
            del self.current_matchups[id_]

        return pid
