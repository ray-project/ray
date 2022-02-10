from abc import ABCMeta
import logging
import numpy as np
import re

from ray.rllib.agents.trainer import Trainer
from ray.rllib.examples.policy.random_policy import RandomPolicy
from ray.rllib.policy.policy import PolicySpec
from ray.rllib.utils.annotations import ExperimentalAPI, override
from ray.rllib.utils.numpy import softmax
from ray.rllib.utils.typing import TrainerConfigDict, ResultDict

logger = logging.getLogger(__name__)


@ExperimentalAPI
class LeagueBuilder(metaclass=ABCMeta):
    def __init__(self, trainer: Trainer, trainer_config: TrainerConfigDict):
        """Initializes a LeagueBuilder instance.

        Args:
            trainer: The Trainer object by which this league builder is used.
                Trainer calls `build_league()` after each training step.
            trainer_config: The (not yet validated) config dict to be
                used on the Trainer. Child classes of `LeagueBuilder`
                should preprocess this to add e.g. multiagent settings
                to this config.
        """
        self.trainer = trainer
        self.config = trainer_config

    def build_league(self, result: ResultDict) -> None:
        """Method containing league-building logic. Called after train step.

        Args:
            result: The most recent result dict with all necessary stats in
                it (e.g. episode rewards) to perform league building
                operations.
        """
        raise NotImplementedError


@ExperimentalAPI
class NoLeagueBuilder(LeagueBuilder):
    """A LeagueBuilder that does nothing.

    Useful for simple, non-league-building multi-agent setups.
    See e.g.
    `rllib/tuned_examples/alpha_star/multi-agent-cart-pole-alpha-star.yaml`
    """

    def build_league(self, result: ResultDict) -> None:
        pass


@ExperimentalAPI
class AlphaStarLeagueBuilder(LeagueBuilder):
    def __init__(
        self,
        trainer: Trainer,
        trainer_config: TrainerConfigDict,
        num_random_policies: int = 2,
        num_learning_league_exploiters: int = 4,
        num_learning_main_exploiters: int = 4,
        win_rate_threshold_for_new_snapshot: float = 0.8,
        keep_new_snapshot_training_prob: float = 0.0,
        prob_league_exploiter_match: float = 0.33,
        prob_main_exploiter_match: float = 0.33,
        prob_main_exploiter_playing_against_learning_main: float = 0.5,
    ):
        """Initializes a AlphaStarLeagueBuilder instance.

        Args:
            trainer: The Trainer object by which this league builder is used.
                Trainer calls `build_league()` after each training step.
            trainer_config: The (not yet validated) config dict to be
                used on the Trainer. Child classes of `LeagueBuilder`
                should preprocess this to add e.g. multiagent settings
                to this config.
            num_random_policies: The number of random policies to add to the
                league. This must be an even number (including 0) as these
                will be evenly distributed amongst league- and main- exploiters.
            num_learning_league_exploiters: The number of learning
                league-exploiters to create.
            num_learning_main_exploiters: The number of learning
                main-exploiters to create.
            win_rate_threshold_for_new_snapshot: The win-rate to be achieved
                for a learning policy to get snapshot'd (forked into `self` +
                a new learning or non-learning copy of `self`).
            keep_new_snapshot_training_prob: The probability with which a new
                snapshot should keep training. Note that the policy from which
                this snapshot is taken will continue to train regardless.
            prob_league_exploiter_match: Probability of an episode to become a
                league-exploiter vs snapshot match.
            prob_main_exploiter_match: Probability of an episode to become a
                main-exploiter vs main match.
            prob_main_exploiter_playing_against_learning_main: Probability of
                a main-exploiter vs (training!) main match.
        """
        super().__init__(trainer, trainer_config)

        self.win_rate_threshold_for_new_snapshot = win_rate_threshold_for_new_snapshot
        self.keep_new_snapshot_training_prob = keep_new_snapshot_training_prob
        self.prob_league_exploiter_match = prob_league_exploiter_match
        self.prob_main_exploiter_match = prob_main_exploiter_match
        self.prob_main_exploiter_playing_against_learning_main = (
            prob_main_exploiter_playing_against_learning_main
        )

        assert num_random_policies % 2 == 0, (
            "ERROR: `num_random_policies` must be even number (we'll distribute "
            "these evenly amongst league- and main-exploiters)!"
        )

        # Build trainer's multiagent config.
        ma_config = self.config["multiagent"]
        # Make sure the multiagent config dict has no policies defined:
        assert not ma_config.get("policies"), (
            "ERROR: `config.multiagent.policies` should not be pre-defined! "
            "AlphaStarLeagueBuilder will construct this itself."
        )
        ma_config["policies"] = policies = {}

        self.main_policies = 1
        self.league_exploiters = (
            num_learning_league_exploiters + num_random_policies / 2
        )
        self.main_exploiters = num_learning_main_exploiters + num_random_policies / 2

        # Add 1 initial (learning) main policy.
        policies["main_0"] = PolicySpec()

        # Train all non-random policies that exist at beginning.
        ma_config["policies_to_train"] = ["main_0"]

        # Add random policies.
        i = -1
        for i in range(num_random_policies // 2):
            policies[f"league_exploiter_{i}"] = PolicySpec(policy_class=RandomPolicy)
            policies[f"main_exploiter_{i}"] = PolicySpec(policy_class=RandomPolicy)
        # Add initial (learning) league-exploiters.
        for j in range(num_learning_league_exploiters):
            pid = f"league_exploiter_{j + i + 1}"
            policies[pid] = PolicySpec()
            ma_config["policies_to_train"].append(pid)
        # Add initial (learning) main-exploiters.
        for j in range(num_learning_league_exploiters):
            pid = f"main_exploiter_{j + i + 1}"
            policies[pid] = PolicySpec()
            ma_config["policies_to_train"].append(pid)

        # Initial policy mapping function: main_0 vs main_exploiter_0.
        ma_config["policy_mapping_fn"] = (
            lambda aid, ep, worker, **kw: "main_0"
            if ep.episode_id % 2 == aid
            else "main_exploiter_0"
        )

    @override(LeagueBuilder)
    def build_league(self, result: ResultDict) -> None:
        local_worker = self.trainer.workers.local_worker()

        # If no evaluation results -> Use hist data gathered for training.
        if "evaluation" in result:
            hist_stats = result["evaluation"]["hist_stats"]
        else:
            hist_stats = result["hist_stats"]

        trainable_policies = local_worker.get_policies_to_train()
        non_trainable_policies = (
            set(local_worker.policy_map.keys()) - trainable_policies
        )

        logger.info(f"League building after iter {self.trainer.iteration}:")

        # Calculate current win-rates.
        for policy_id, rew in hist_stats.items():
            mo = re.match("^policy_(.+)_reward$", policy_id)
            if mo is None:
                continue
            policy_id = mo.group(1)

            # Calculate this policy's win rate.
            won = 0
            for r in rew:
                if r > 0.0:  # win = 1.0; loss = -1.0
                    won += 1
            win_rate = won / len(rew)
            # TODO: This should probably be a running average
            #  (instead of hard-overriding it with the most recent data).
            self.trainer.win_rates[policy_id] = win_rate

            # Policy is a snapshot (frozen) -> Ignore.
            if policy_id not in trainable_policies:
                continue

            logger.info(f"\t{policy_id} win-rate={win_rate} -> ", end="")

            # If win rate is good enough -> Snapshot current policy and decide,
            # whether to freeze the new snapshot or not.
            if win_rate >= self.win_rate_threshold_for_new_snapshot:
                is_main = re.match("^main(_\\d+)?$", policy_id)

                # Probability that the new snapshot is trainable.
                keep_training_p = self.keep_new_snapshot_training_prob
                # For main, new snapshots are never trainable, for all others
                # use `config.keep_new_snapshot_training_prob` (default: 0.0!).
                keep_training = (
                    False
                    if is_main
                    else np.random.choice(
                        [True, False], p=[keep_training_p, 1.0 - keep_training_p]
                    )
                )
                # New league-exploiter policy.
                if policy_id.startswith("league_ex"):
                    new_pol_id = re.sub(
                        "_\\d+$", f"_{self.league_exploiters}", policy_id
                    )
                    self.league_exploiters += 1
                # New main-exploiter policy.
                elif policy_id.startswith("main_ex"):
                    new_pol_id = re.sub("_\\d+$", f"_{self.main_exploiters}", policy_id)
                    self.main_exploiters += 1
                # New main policy snapshot.
                else:
                    new_pol_id = re.sub("_\\d+$", f"_{self.main_policies}", policy_id)
                    self.main_policies += 1

                if keep_training:
                    trainable_policies.add(new_pol_id)
                else:
                    non_trainable_policies.add(new_pol_id)

                logger.info(
                    f"adding new opponents to the mix ({new_pol_id}; "
                    f"trainable={keep_training})."
                )

                num_main_policies = self.main_policies
                probs_match_types = [
                    self.prob_league_exploiter_match,
                    self.prob_main_exploiter_match,
                    1.0
                    - self.prob_league_exploiter_match
                    - self.prob_main_exploiter_match,
                ]
                prob_playing_learning_main = (
                    self.prob_main_exploiter_playing_against_learning_main
                )

                # Update our mapping function accordingly.
                def policy_mapping_fn(agent_id, episode, worker, **kwargs):

                    # Pick, whether this is:
                    # LE: league-exploiter vs snapshot.
                    # ME: main-exploiter vs (any) main.
                    # M: Learning main vs itself.
                    type_ = np.random.choice(["LE", "ME", "M"], p=probs_match_types)

                    # Learning league exploiter vs a snapshot.
                    # Opponent snapshots should be selected based on a win-rate-
                    # derived probability.
                    if type_ == "LE":
                        if episode.episode_id % 2 == agent_id:
                            league_exploiter = np.random.choice(
                                [
                                    p
                                    for p in trainable_policies
                                    if p.startswith("league_ex")
                                ]
                            )
                            logger.debug(
                                f"Episode {episode.episode_id}: AgentID "
                                f"{agent_id} played by {league_exploiter} (training)"
                            )
                            return league_exploiter
                        # Play against any non-trainable policy (excluding itself).
                        else:
                            all_opponents = list(non_trainable_policies)
                            probs = softmax(
                                [
                                    worker.global_vars["win_rates"][pid]
                                    for pid in all_opponents
                                ]
                            )
                            opponent = np.random.choice(all_opponents, p=probs)
                            logger.debug(
                                f"Episode {episode.episode_id}: AgentID "
                                f"{agent_id} played by {opponent} (frozen)"
                            )
                            return opponent

                    # Learning main exploiter vs (learning main OR snapshot main).
                    elif type_ == "ME":
                        if episode.episode_id % 2 == agent_id:
                            main_exploiter = np.random.choice(
                                [
                                    p
                                    for p in trainable_policies
                                    if p.startswith("main_ex")
                                ]
                            )
                            logger.debug(
                                f"Episode {episode.episode_id}: AgentID "
                                f"{agent_id} played by {main_exploiter} (training)"
                            )
                            return main_exploiter
                        else:
                            # n% of the time, play against the learning main.
                            # Also always play againt learning main if no
                            # non-learning mains have been created yet.
                            if num_main_policies == 1 or (
                                np.random.random() < prob_playing_learning_main
                            ):
                                main = "main_0"
                                training = "training"
                            # 100-n% of the time, play against a non-learning
                            # main. Opponent main snapshots should be selected
                            # based on a win-rate-derived probability.
                            else:
                                all_opponents = [
                                    f"main_{p}"
                                    for p in list(range(1, num_main_policies))
                                ]
                                probs = softmax(
                                    [
                                        worker.global_vars["win_rates"][pid]
                                        for pid in all_opponents
                                    ]
                                )
                                main = np.random.choice(all_opponents, p=probs)
                                training = "frozen"
                            logger.debug(
                                f"Episode {episode.episode_id}: AgentID "
                                f"{agent_id} played by {main} ({training})"
                            )
                            return main

                    # Main policy: Self-play.
                    else:
                        logger.debug(f"Episode {episode.episode_id}: main_0 vs main_0")
                        return "main_0"

                # Add and set the weights of the new polic(y/ies).
                state = self.trainer.get_policy(policy_id).get_state()
                self.trainer.add_policy(
                    policy_id=new_pol_id,
                    policy_cls=type(self.trainer.get_policy(policy_id)),
                    policy_state=state,
                    policy_mapping_fn=policy_mapping_fn,
                    policies_to_train=trainable_policies,
                )

            else:
                logger.info("not good enough; will keep learning ...")
