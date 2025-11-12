import re

import numpy as np

from ray._common.deprecation import Deprecated
from ray.rllib.callbacks.callbacks import RLlibCallback
from ray.rllib.utils.metrics import ENV_RUNNER_RESULTS


@Deprecated(help="Use the example for the new RLlib API stack", error=False)
class SelfPlayLeagueBasedCallbackOldAPIStack(RLlibCallback):
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

    def on_train_result(self, *, algorithm, result, **kwargs):
        # Avoid `self` being pickled into the remote function below.
        _trainable_policies = self.trainable_policies

        # Get the win rate for the train batch.
        # Note that normally, you should set up a proper evaluation config,
        # such that evaluation always happens on the already updated policy,
        # instead of on the already used train_batch.
        for policy_id, rew in result[ENV_RUNNER_RESULTS]["hist_stats"].items():
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
            self.win_rates[policy_id] = win_rate

            # Policy is frozen; ignore.
            if policy_id in self.non_trainable_policies:
                continue

            print(
                f"Iter={algorithm.iteration} {policy_id}'s " f"win-rate={win_rate} -> ",
                end="",
            )

            # If win rate is good -> Snapshot current policy and decide,
            # whether to freeze the copy or not.
            if win_rate > self.win_rate_threshold:
                is_main = re.match("^main(_\\d+)?$", policy_id)
                initializing_exploiters = False

                # First time, main manages a decent win-rate against random:
                # Add league_exploiter_0 and main_exploiter_0 to the mix.
                if is_main and len(self.trainable_policies) == 1:
                    initializing_exploiters = True
                    self.trainable_policies.add("league_exploiter_0")
                    self.trainable_policies.add("main_exploiter_0")
                else:
                    keep_training = (
                        False
                        if is_main
                        else np.random.choice([True, False], p=[0.3, 0.7])
                    )
                    if policy_id in self.main_policies:
                        new_pol_id = re.sub(
                            "_\\d+$", f"_{len(self.main_policies) - 1}", policy_id
                        )
                        self.main_policies.add(new_pol_id)
                    elif policy_id in self.main_exploiters:
                        new_pol_id = re.sub(
                            "_\\d+$", f"_{len(self.main_exploiters)}", policy_id
                        )
                        self.main_exploiters.add(new_pol_id)
                    else:
                        new_pol_id = re.sub(
                            "_\\d+$", f"_{len(self.league_exploiters)}", policy_id
                        )
                        self.league_exploiters.add(new_pol_id)

                    if keep_training:
                        self.trainable_policies.add(new_pol_id)
                    else:
                        self.non_trainable_policies.add(new_pol_id)

                    print(f"adding new opponents to the mix ({new_pol_id}).")

                # Update our mapping function accordingly.
                def policy_mapping_fn(agent_id, episode, worker=None, **kwargs):
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
                        print(f"{league_exploiter} vs {opponent}")
                        return (
                            league_exploiter
                            if episode.episode_id % 2 == agent_id
                            else opponent
                        )

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
                        # print(f"{main_exploiter} vs {main}")
                        return (
                            main_exploiter
                            if episode.episode_id % 2 == agent_id
                            else main
                        )

                # Set the weights of the new polic(y/ies).
                if initializing_exploiters:
                    main_state = algorithm.get_policy("main").get_state()
                    pol_map = algorithm.env_runner.policy_map
                    pol_map["main_0"].set_state(main_state)
                    pol_map["league_exploiter_1"].set_state(main_state)
                    pol_map["main_exploiter_1"].set_state(main_state)
                    # We need to sync the just copied local weights to all the
                    # remote workers as well.
                    algorithm.env_runner_group.sync_weights(
                        policies=["main_0", "league_exploiter_1", "main_exploiter_1"]
                    )

                    def _set(worker):
                        worker.set_policy_mapping_fn(policy_mapping_fn)
                        worker.set_is_policy_to_train(_trainable_policies)

                    algorithm.env_runner_group.foreach_env_runner(_set)
                else:
                    base_pol = algorithm.get_policy(policy_id)
                    new_policy = algorithm.add_policy(
                        policy_id=new_pol_id,
                        policy_cls=type(base_pol),
                        policy_mapping_fn=policy_mapping_fn,
                        policies_to_train=self.trainable_policies,
                        config=base_pol.config,
                        observation_space=base_pol.observation_space,
                        action_space=base_pol.action_space,
                    )
                    main_state = base_pol.get_state()
                    new_policy.set_state(main_state)
                    # We need to sync the just copied local weights to all the
                    # remote workers as well.
                    algorithm.env_runner_group.sync_weights(policies=[new_pol_id])

                self._print_league()

            else:
                print("not good enough; will keep learning ...")

    def _print_league(self):
        print("--- League ---")
        print("Trainable policies (win-rates):")
        for p in sorted(self.trainable_policies):
            wr = self.win_rates[p] if p in self.win_rates else 0.0
            print(f"\t{p}: {wr}")
        print("Frozen policies:")
        for p in sorted(self.non_trainable_policies):
            wr = self.win_rates[p] if p in self.win_rates else 0.0
            print(f"\t{p}: {wr}")
        print()
