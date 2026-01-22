import collections
import logging
from dataclasses import dataclass
from typing import Dict, Optional

import gymnasium as gym
import numpy as np

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.callbacks import RLlibCallback
from ray.rllib.core.rl_module import RLModuleSpec
from ray.rllib.env.env_runner import EnvRunner
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.examples.envs.classes.multi_agent.footsies.game.constants import (
    FOOTSIES_ACTION_IDS,
)
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
        """Mapping function that retrieves policy_id from the sampled matchup"""
        id_ = episode.id_
        if self.current_matchups.get(id_) is None:
            # step 1: sample a matchup according to the specified probabilities
            sampled_matchup = np.random.choice(a=self.matchups, p=self.probs)

            # step 2: Randomize who is player 1 and player 2
            policies = [sampled_matchup.p1, sampled_matchup.p2]
            p1, p2 = np.random.choice(policies, size=2, replace=False)

            # step 3: Set as the current matchup for the episode in question (id_)
            self.current_matchups[id_]["p1"] = p1
            self.current_matchups[id_]["p2"] = p2

        policy_id = self.current_matchups[id_].pop(agent_id)

        # remove (an empty dict) for the current episode with id_
        if not self.current_matchups[id_]:
            del self.current_matchups[id_]

        return policy_id


class MetricsLoggerCallback(RLlibCallback):
    def __init__(self, main_policy: str) -> None:
        """Log experiment metrics

        Logs metrics after each episode step and at the end of each (train or eval) episode.
        Metrics logged at the end of each episode will be later used by MixManagerCallback
        to decide whether to add a new opponent to the mix.
        """
        super().__init__()
        self.main_policy = main_policy
        self.action_id_to_str = {
            action_id: action_str
            for action_str, action_id in FOOTSIES_ACTION_IDS.items()
        }

    def on_episode_step(
        self,
        *,
        episode: MultiAgentEpisode,
        env_runner: Optional[EnvRunner] = None,
        metrics_logger: Optional[MetricsLogger] = None,
        env: Optional[gym.Env] = None,
        env_index: int,
        **kwargs,
    ) -> None:
        """Log action usage frequency

        Log actions performed by both players at each step of the (training or evaluation) episode.
        """
        stage = "eval" if env_runner.config.in_evaluation else "train"

        # get the ModuleID for each agent
        p1_module = episode.module_for("p1")
        p2_module = episode.module_for("p2")

        # get action string for each agent
        p1_action_id = env.envs[
            env_index
        ].unwrapped.last_game_state.player1.current_action_id
        p2_action_id = env.envs[
            env_index
        ].unwrapped.last_game_state.player2.current_action_id
        p1_action_str = self.action_id_to_str[p1_action_id]
        p2_action_str = self.action_id_to_str[p2_action_id]

        metrics_logger.log_value(
            key=f"footsies/{stage}/actions/{p1_module}/{p1_action_str}",
            value=1,
            reduce="sum",
            window=100,
        )
        metrics_logger.log_value(
            key=f"footsies/{stage}/actions/{p2_module}/{p2_action_str}",
            value=1,
            reduce="sum",
            window=100,
        )

    def on_episode_end(
        self,
        *,
        episode: MultiAgentEpisode,
        env_runner: Optional[EnvRunner] = None,
        metrics_logger: Optional[MetricsLogger] = None,
        env: Optional[gym.Env] = None,
        env_index: int,
        **kwargs,
    ) -> None:
        """Log win rates

        Log win rates of the main policy against its opponent at the end of the (training or evaluation) episode.
        """
        stage = "eval" if env_runner.config.in_evaluation else "train"

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
                key=f"footsies/{stage}/both_dead/{self.main_policy}/vs_{opponent_id}",
                value=1,
                reduce="mean",
                window=100,
            )
        elif not p1_dead and not p2_dead:
            metrics_logger.log_value(
                key=f"footsies/{stage}/both_alive/{self.main_policy}/vs_{opponent_id}",
                value=1,
                reduce="mean",
                window=100,
            )
        else:
            # log the win rate against the opponent with an 'opponent_id'
            metrics_logger.log_value(
                key=f"footsies/{stage}/win_rates/{self.main_policy}/vs_{opponent_id}",
                value=int(main_policy_win),
                reduce="mean",
                window=100,
            )

            # log the win rate, without specifying the opponent
            # this metric collected from the eval env runner
            # will be used to decide whether to add
            # a new opponent at the current level.
            metrics_logger.log_value(
                key=f"footsies/{stage}/win_rates/{self.main_policy}/vs_any",
                value=int(main_policy_win),
                reduce="mean",
                window=100,
            )


class MixManagerCallback(RLlibCallback):
    def __init__(
        self,
        win_rate_threshold: float,
        main_policy: str,
        target_mix_size: int,
        starting_modules=list[str],  # default is ["lstm", "noop"]
        fixed_modules_progression_sequence=tuple[str],  # default is ("noop", "back")
    ) -> None:
        """Track win rates and manage mix of opponents"""
        super().__init__()
        self.win_rate_threshold = win_rate_threshold
        self.main_policy = main_policy
        self.target_mix_size = target_mix_size
        self.fixed_modules_progression_sequence = tuple(
            fixed_modules_progression_sequence
        )  # Order of RL modules to be added to the mix
        self.modules_in_mix = list(
            starting_modules
        )  # RLModules that are currently in the mix
        self._trained_policy_idx = (
            0  # We will use this to create new opponents of the main policy
        )

    def on_evaluate_end(
        self,
        *,
        algorithm: Algorithm,
        metrics_logger: Optional[MetricsLogger] = None,
        evaluation_metrics: dict,
        **kwargs,
    ) -> None:
        """Check win rates and add new opponent if necessary.

        Check the win rate of the main policy against its current opponent.
        If the win rate exceeds the specified threshold, add a new opponent to the mix, by modifying:
        1. update the policy_mapping_fn for (training and evaluation) env runners
        2. if the new policy is a trained one (not a fixed RL module), modify Algorithm's state (initialize the state of the newly added RLModule by using the main policy)
        """
        _main_module = algorithm.get_module(self.main_policy)
        new_module_id = None
        new_module_spec = None

        win_rate = evaluation_metrics[ENV_RUNNER_RESULTS][
            f"footsies/eval/win_rates/{self.main_policy}/vs_any"
        ]

        if win_rate > self.win_rate_threshold:
            logger.info(
                f"RLlib {self.__class__.__name__}: Win rate for main policy '{self.main_policy}' "
                f"exceeded threshold ({win_rate} > {self.win_rate_threshold})."
                f" Adding new RL Module to the mix..."
            )

            # check if fixed RL module should be added to the mix,
            # and if so, create new_module_id and new_module_spec for it
            for module_id in self.fixed_modules_progression_sequence:
                if module_id not in self.modules_in_mix:
                    new_module_id = module_id
                    break

            # in case that all fixed RL Modules are already in the mix (together with the main policy),
            # we will add a new RL Module by taking main policy and adding an instance of it to the mix
            if new_module_id is None:
                new_module_id = f"{self.main_policy}_v{self._trained_policy_idx}"
                new_module_spec = RLModuleSpec.from_module(_main_module)
                self._trained_policy_idx += 1

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

            # update (training) env runners with the new mapping function
            algorithm.env_runner_group.foreach_env_runner(
                lambda er: er.config.multi_agent(policy_mapping_fn=new_mapping_fn),
                local_env_runner=True,
            )

            # update (eval) env runners with the new mapping function
            algorithm.eval_env_runner_group.foreach_env_runner(
                lambda er: er.config.multi_agent(policy_mapping_fn=new_mapping_fn),
                local_env_runner=True,
            )

            if new_module_id not in self.fixed_modules_progression_sequence:
                algorithm.add_module(
                    module_id=new_module_id,
                    module_spec=new_module_spec,
                    new_agent_to_module_mapping_fn=new_mapping_fn,
                )
                # newly added trained policy should be initialized with the state of the main policy
                algorithm.set_state(
                    {
                        "learner_group": {
                            "learner": {
                                "rl_module": {
                                    new_module_id: _main_module.get_state(),
                                }
                            }
                        },
                    }
                )
            # we added a new RL Module, so we need to update the current mix list.
            self.modules_in_mix.append(new_module_id)

        else:
            logger.info(
                f"RLlib {self.__class__.__name__}: Win rate for main policy '{self.main_policy}' "
                f"did not exceed threshold ({win_rate} <= {self.win_rate_threshold})."
            )

    def on_train_result(
        self,
        *,
        algorithm: Algorithm,
        metrics_logger: Optional[MetricsLogger] = None,
        result: Dict,
        **kwargs,
    ) -> None:
        """Report the current mix size at the end of training iteration.

        That will tell Ray Tune, whether to stop training (once the 'target_mix_size' has been reached).
        """
        result["mix_size"] = len(self.modules_in_mix)
