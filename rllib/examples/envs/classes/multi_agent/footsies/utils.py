import collections
import logging
from dataclasses import dataclass
from typing import Dict, Optional

import gymnasium as gym
import numpy as np
from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.callbacks import RLlibCallback
from ray.rllib.core.rl_module import RLModuleSpec
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.examples.envs.classes.multi_agent.footsies.fixed_rlmodules import (
    NoopFixedRLModule,
    BackFixedRLModule,
    AttackFixedRLModule,
)
from ray.rllib.examples.rl_modules.classes.random_rlm import RandomRLModule
from ray.rllib.utils.metrics import ENV_RUNNER_RESULTS
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
from ray.rllib.utils.typing import EpisodeType

logger = logging.getLogger("ray.rllib")


@dataclass
class Matchup:
    p1: str
    p2: str
    prob: float


class Matchmaker:
    def __init__(self, matchups: list[Matchup]):
        self.matchups = matchups
        self.probs = [matchup.prob for matchup in matchups]
        self.current_matchups = collections.defaultdict(dict)

    def agent_to_module_mapping_fn(
        self, agent_id: str, episode: EpisodeType, **kwargs
    ) -> str:
        """Mapping function that retrieves from the current matchup"""
        id_ = episode.env_id if hasattr(episode, "env_id") else episode.id_
        if self.current_matchups.get(id_) is None:
            # Sample a matchup
            sampled_matchup = np.random.choice(a=self.matchups, p=self.probs)

            # Randomize who is player 1 and player 2
            policies = [sampled_matchup.p1, sampled_matchup.p2]
            p1, p2 = np.random.choice(policies, size=2, replace=False)

            # Set this as the current episodes mapping
            self.current_matchups[id_]["p1"] = p1
            self.current_matchups[id_]["p2"] = p2

        policy_id = self.current_matchups[id_].pop(agent_id)

        # remove (an empty dict) for the current episode with id_
        if not self.current_matchups[id_]:
            del self.current_matchups[id_]

        return policy_id


class WinratesCallback(RLlibCallback):
    def __init__(
        self,
        win_rate_threshold: float,
        target_mix_size: int,
        main_policy: str,
        starting_modules=list[str],  # default is ["lstm", "noop"]
    ) -> None:
        """Callback to track win rates and manage mix of opponents for the main policy being trained."""
        super().__init__()
        self.win_rate_threshold = win_rate_threshold
        self.target_mix_size = target_mix_size
        self.main_policy = main_policy
        self.beginner_modules_progression_sequence = (
            "noop",
            "back",
            "attack",
            "random",
        )  # Order of RL modules to be added to the mix
        self.beginner_modules = {
            "noop": NoopFixedRLModule,
            "back": BackFixedRLModule,
            "attack": AttackFixedRLModule,
            "random": RandomRLModule,
        }
        self.modules_in_mix = list(starting_modules)  # Initial RL modules in the mix
        self._current_mix_size = (
            2  # Initial size with two RL modules for agent p1 and p2
        )
        self._trained_policy_idx = (
            0  # We will use this to create new opponents of the main policy
        )

    def on_episode_end(
        self,
        *,
        episode: MultiAgentEpisode,
        metrics_logger: Optional[MetricsLogger] = None,
        env: Optional[gym.Env] = None,
        env_index: int,
        **kwargs,
    ) -> None:
        # check status of "p1" and "p2"
        last_game_state = env.envs[env_index].unwrapped.last_game_state
        p1_dead = last_game_state.player1.is_dead
        p2_dead = last_game_state.player2.is_dead

        # get the ModuleID for each agent
        p1_module = episode.module_for("p1")
        p2_module = episode.module_for("p2")

        if self.main_policy == p1_module:
            opponent_id = p2_module
            main_policy_win = p2_dead
        elif self.main_policy == p2_module:
            opponent_id = p1_module
            main_policy_win = p1_dead
        else:
            logger.info(
                f"RLlib {self.__class__.__name__}: Main policy: '{self.main_policy}' not found in this episode. "
                f"Policies in this episode are: '{p1_module}' and '{p2_module}'. "
                f"Check your multi_agent 'policy_mapping_fn'. "
                f"Metrics logging for this episode will be skipped."
            )
            return

        if p1_dead and p2_dead:
            metrics_logger.log_value(
                key=f"footsies/both_dead/{self.main_policy}/vs_{opponent_id}",
                value=1,
                reduce="mean",
                window=100,
                clear_on_reduce=True,
            )
        elif not p1_dead and not p2_dead:
            metrics_logger.log_value(
                key=f"footsies/both_alive/{self.main_policy}/vs_{opponent_id}",
                value=1,
                reduce="mean",
                window=100,
                clear_on_reduce=True,
            )
        else:
            # log the win rate against the opponent with an 'opponent_id'
            metrics_logger.log_value(
                key=f"footsies/win_rates/{self.main_policy}/vs_{opponent_id}",
                value=int(main_policy_win),
                reduce="mean",
                window=100,
                clear_on_reduce=True,
            )

            # log the win rate "globally", without specifying the opponent
            # this metric will be used to decide whether to add a new opponent
            # at the current level, the main policy ('self.main_policy') should have
            # a specified 'win_rate_threshold' performance against any opponent in the mix
            metrics_logger.log_value(
                key=f"footsies/win_rates/{self.main_policy}/vs_any",
                value=int(main_policy_win),
                reduce="mean",
                window=100,
                clear_on_reduce=True,
            )

    def on_train_result(
        self,
        *,
        algorithm: Algorithm,
        metrics_logger: Optional[MetricsLogger] = None,
        result: Dict,
        **kwargs,
    ) -> None:
        new_module_id = None
        win_rate = result[ENV_RUNNER_RESULTS][
            f"footsies/win_rates/{self.main_policy}/vs_any"
        ]
        if win_rate > self.win_rate_threshold:
            logger.info(
                f"RLlib {self.__class__.__name__}: Win rate for main policy '{self.main_policy}' "
                f"exceeded threshold ({win_rate} > {self.win_rate_threshold})."
                f" Adding new RL Module to the mix..."
            )

            # check if beginner RL module should be added to the mix, and if so, add it.
            for module_id in self.beginner_modules_progression_sequence:
                if module_id not in self.modules_in_mix:
                    new_module_id = module_id
                    break

            # in case that all beginner RL Modules are already in the mix (together with the main policy),
            # we will add a new RL Module by taking main policy and adding an instance of it to the mix
            if set(self.modules_in_mix) == set(
                self.beginner_modules_progression_sequence
            ).add(self.main_policy):
                new_module_id = f"{self.main_policy}_v{self._trained_policy_idx}"
                self._trained_policy_idx += 1

            assert new_module_id is not None, f"New RL Module_id should not be None."

            # create new policy mapping function, to ensure that the main policy plays against newly added policy
            new_mapping_fn = Matchmaker(
                [
                    Matchup(
                        p1=self.main_policy,
                        p2=new_module_id,
                        prob=1.0,
                    )
                ]
            ).agent_to_module_mapping_fn

            main_module = algorithm.get_module(self.main_policy)
            algorithm.add_module(
                module_id=new_module_id,
                module_spec=RLModuleSpec.from_module(main_module),
                new_agent_to_module_mapping_fn=new_mapping_fn,
                add_to_learners=False,
            )

            algorithm.set_state(
                {
                    "learner_group": {
                        "learner": {
                            "rl_module": {
                                new_module_id: main_module.get_state(),
                            }
                        }
                    }
                }
            )

            self._current_mix_size += 1
            self.modules_in_mix.append(new_module_id)

        else:
            logger.info(
                f"RLlib {self.__class__.__name__}: Win rate for main policy '{self.main_policy}' "
                f"did not exceed threshold ({win_rate} <= {self.win_rate_threshold})."
            )

        result["target_mix_size"] = self._current_mix_size
