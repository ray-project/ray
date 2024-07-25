from collections import defaultdict
from pprint import pprint
import re

import numpy as np

from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.utils.metrics import ENV_RUNNER_RESULTS


class SelfPlayLeagueBasedCallback(DefaultCallbacks):
    def __init__(self, win_rate_threshold):
        super().__init__()
        # All policies in the league.
        self.main_policies = {"main", "main_0"}
        self.main_exploiters = {"main_exploiter_0", "main_exploiter_1"}
        self.league_exploiters = {"league_exploiter_0", "league_exploiter_1"}
        # Set of currently trainable policies in the league.
        self.trainable_policies = {"main"}
        # Set of currently non-trainable (frozen) policies in the league.
        self.non_trainable_policies = {
            "main_0",
            "league_exploiter_0",
            "main_exploiter_0",
        }
        # The win-rate value reaching of which leads to a new module being added
        # to the leage (frozen copy of main).
        self.win_rate_threshold = win_rate_threshold
        # Store the win rates for league overview printouts.
        self.win_rates = {}

        # Report the matchup counters (who played against whom?).
        self._matching_stats = defaultdict(int)

    def on_train_result(self, *, algorithm, metrics_logger=None, result, **kwargs):
        local_worker = algorithm.env_runner

        # Avoid `self` being pickled into the remote function below.
        _trainable_policies = self.trainable_policies

        # Get the win rate for the train batch.
        # Note that normally, one should set up a proper evaluation config,
        # such that evaluation always happens on the already updated policy,
        # instead of on the already used train_batch.
        league_changed = False
        for module_id, rew in result[ENV_RUNNER_RESULTS]["hist_stats"].items():
            mo = re.match("^policy_(.+)_reward$", module_id)
            if mo is None:
                continue
            module_id = mo.group(1)

            # Calculate this policy's win rate.
            won = 0
            for r in rew:
                if r > 0.0:  # win = 1.0; loss = -1.0
                    won += 1
            win_rate = won / len(rew)
            self.win_rates[module_id] = win_rate

            # Policy is frozen; ignore.
            if module_id in self.non_trainable_policies:
                continue

            print(
                f"Iter={algorithm.iteration} {module_id}'s " f"win-rate={win_rate} -> ",
                end="",
            )

            # If win rate is good -> Snapshot current policy and decide,
            # whether to freeze the copy or not.
            if win_rate > self.win_rate_threshold:
                is_main = re.match("^main(_\\d+)?$", module_id)
                initializing_exploiters = False

                # First time, main manages a decent win-rate against random:
                # Add league_exploiter_1 and main_exploiter_1 as trainables to the mix.
                if is_main and len(self.trainable_policies) == 1:
                    initializing_exploiters = True
                    self.trainable_policies.add("league_exploiter_1")
                    self.trainable_policies.add("main_exploiter_1")
                # If main manages to win (above threshold) against the entire league
                # -> increase the league by another frozen copy of main,
                # main-exploiters or league-exploiters.
                else:
                    keep_training = (
                        False
                        if is_main
                        else np.random.choice([True, False], p=[0.3, 0.7])
                    )
                    if module_id in self.main_policies:
                        new_mod_id = re.sub(
                            "(main)(_\\d+)?$",
                            f"\\1_{len(self.main_policies) - 1}",
                            module_id,
                        )
                        self.main_policies.add(new_mod_id)
                    elif module_id in self.main_exploiters:
                        new_mod_id = re.sub(
                            "_\\d+$", f"_{len(self.main_exploiters)}", module_id
                        )
                        self.main_exploiters.add(new_mod_id)
                    else:
                        new_mod_id = re.sub(
                            "_\\d+$", f"_{len(self.league_exploiters)}", module_id
                        )
                        self.league_exploiters.add(new_mod_id)

                    if keep_training:
                        self.trainable_policies.add(new_mod_id)
                    else:
                        self.non_trainable_policies.add(new_mod_id)

                    print(f"adding new opponents to the mix ({new_mod_id}).")

                # Update our mapping function accordingly.
                def agent_to_module_mapping_fn(agent_id, episode, **kwargs):
                    # Pick, whether this is ...
                    type_ = np.random.choice([1, 2])

                    # 1) League exploiter vs any other.
                    if type_ == 1:
                        league_exploiter = "league_exploiter_" + str(
                            np.random.choice(list(range(len(self.league_exploiters))))
                        )
                        # This league exploiter is frozen: Play against a
                        # trainable policy.
                        if league_exploiter not in self.trainable_policies:
                            opponent = np.random.choice(list(self.trainable_policies))
                        # League exploiter is trainable: Play against any other
                        # non-trainable policy.
                        else:
                            opponent = np.random.choice(
                                list(self.non_trainable_policies)
                            )

                        # Only record match stats once per match.
                        if hash(episode.id_) % 2 == agent_id:
                            self._matching_stats[(league_exploiter, opponent)] += 1
                            return league_exploiter
                        else:
                            return opponent

                    # 2) Main exploiter vs main.
                    else:
                        main_exploiter = "main_exploiter_" + str(
                            np.random.choice(list(range(len(self.main_exploiters))))
                        )
                        # Main exploiter is frozen: Play against the main
                        # policy.
                        if main_exploiter not in self.trainable_policies:
                            main = "main"
                        # Main exploiter is trainable: Play against any
                        # frozen main.
                        else:
                            main = np.random.choice(list(self.main_policies - {"main"}))

                        # Only record match stats once per match.
                        if hash(episode.id_) % 2 == agent_id:
                            self._matching_stats[(main_exploiter, main)] += 1
                            return main_exploiter
                        else:
                            return main

                marl_module = local_worker.module
                main_module = marl_module["main"]

                # Set the weights of the new polic(y/ies).
                if initializing_exploiters:
                    main_state = main_module.get_state()
                    marl_module["main_0"].set_state(main_state)
                    marl_module["league_exploiter_1"].set_state(main_state)
                    marl_module["main_exploiter_1"].set_state(main_state)
                    # We need to sync the just copied local weights to all the
                    # remote workers and remote Learner workers as well.
                    algorithm.env_runner_group.sync_weights(
                        policies=["main_0", "league_exploiter_1", "main_exploiter_1"]
                    )
                    algorithm.learner_group.set_weights(marl_module.get_state())
                else:
                    algorithm.add_module(
                        module_id=new_mod_id,
                        module_spec=SingleAgentRLModuleSpec.from_module(main_module),
                        module_state=marl_module[module_id].get_state(),
                    )

                algorithm.env_runner_group.foreach_worker(
                    lambda env_runner: env_runner.config.multi_agent(
                        policy_mapping_fn=agent_to_module_mapping_fn,
                        # This setting doesn't really matter for EnvRunners (no
                        # training going on there, but we'll update this as well
                        # here for good measure).
                        policies_to_train=_trainable_policies,
                    ),
                    local_env_runner=True,
                )
                # Set all Learner workers' should_module_be_updated to the new
                # value.
                algorithm.learner_group.foreach_learner(
                    func=lambda learner: learner.config.multi_agent(
                        policies_to_train=_trainable_policies,
                    ),
                    timeout_seconds=0.0,  # fire-and-forget
                )
                league_changed = True
            else:
                print("not good enough; will keep learning ...")

        # Add current league size to results dict.
        result["league_size"] = len(self.non_trainable_policies) + len(
            self.trainable_policies
        )

        if league_changed:
            self._print_league()

    def _print_league(self):
        print("--- League ---")
        print("Matchups:")
        pprint(self._matching_stats)
        print("Trainable policies (win-rates):")
        for p in sorted(self.trainable_policies):
            wr = self.win_rates[p] if p in self.win_rates else 0.0
            print(f"\t{p}: {wr}")
        print("Frozen policies:")
        for p in sorted(self.non_trainable_policies):
            wr = self.win_rates[p] if p in self.win_rates else 0.0
            print(f"\t{p}: {wr}")
        print()
