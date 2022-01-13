import numpy as np
import re
from typing import Set

from ray.rllib.agents.trainer import Trainer
from ray.rllib.utils.typing import PolicyID, ResultDict, TrainerConfigDict


def league_building_fn(
    trainer: Trainer,
    result: ResultDict,
    *,
    config: TrainerConfigDict,
    main_policies: Set[PolicyID],
    main_exploiters: Set[PolicyID],
    league_exploiters: Set[PolicyID],
    trainable_policies: Set[PolicyID],
    non_trainable_policies: Set[PolicyID],
    **kwargs):
    """TODO: Docstring """

    # If no evaluation results -> Use hist data gathered for training.
    if "evaluation" in result:
        hist_stats = result["evaluation"]["hist_stats"]
    else:
        hist_stats = result["hist_stats"]

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
        trainer.win_rates[policy_id] = win_rate

        # Policy is a snapshot (frozen) -> Ignore.
        if policy_id in non_trainable_policies:
            continue

        print(
            f"Iter={trainer.iteration} {policy_id}'s "
            f"win-rate={win_rate} -> ",
            end="")

        # If win rate is good enough -> Snapshot current policy and decide,
        # whether to freeze the new snapshot or not.
        if win_rate >= config["win_rate_threshold_for_new_snapshot"]:
            initializing_first_exploiters = False

            is_main = re.match("^main(_\\d+)?$", policy_id)

            # First time, main manages a decent win-rate against random:
            # Add league_exploiter_0 and main_exploiter_0 to the mix.
            if is_main and \
                    "league_exploiter_1" not in league_exploiters:
                new_pol_id = "main_1"
                initializing_first_exploiters = True
                trainable_policies.add(new_pol_id)

                trainable_policies.add("league_exploiter_1")
                league_exploiters.add("league_exploiter_1")

                trainable_policies.add("main_exploiter_1")
                main_exploiters.add("main_exploiter_1")

                print(f"initializing actual league ({new_pol_id} + "
                      "1st learning league-/main-exploiters).")
            else:
                keep_training_p = config["keep_new_snapshot_training_prob"]
                keep_training = False if is_main else np.random.choice(
                    [True, False], p=[keep_training_p, 1.0 - keep_training_p])
                if policy_id in main_policies:
                    new_pol_id = re.sub(
                        "_\\d+$", f"_{len(main_policies) - 1}", policy_id)
                    main_policies.add(new_pol_id)
                elif policy_id in main_exploiters:
                    new_pol_id = re.sub(
                        "_\\d+$", f"_{len(main_exploiters)}", policy_id)
                    main_exploiters.add(new_pol_id)
                else:
                    new_pol_id = re.sub(
                        "_\\d+$", f"_{len(league_exploiters)}", policy_id)
                    league_exploiters.add(new_pol_id)

                if keep_training:
                    trainable_policies.add(new_pol_id)
                else:
                    non_trainable_policies.add(new_pol_id)

                print(f"adding new opponents to the mix ({new_pol_id}).")

            # Update our mapping function accordingly.
            def policy_mapping_fn(agent_id, episode, worker, **kwargs):

                # Pick, whether this is ...
                type_ = np.random.choice([1, 2])

                # 1) League exploiter vs any other.
                if type_ == 1:
                    league_exploiter = "league_exploiter_" + str(
                        np.random.choice(
                            list(range(len(league_exploiters)))))
                    # This league exploiter is a frozen: Play against a
                    # trainable policy.
                    if league_exploiter not in trainable_policies:
                        opponent = np.random.choice(
                            list(trainable_policies))
                    # League exploiter is trainable: Play against any other
                    # non-trainable policy.
                    else:
                        opponent = np.random.choice(
                            list(non_trainable_policies))
                    print(f"{league_exploiter} vs {opponent}")
                    return league_exploiter if \
                        episode.episode_id % 2 == agent_id else opponent

                # 2) Main exploiter vs main.
                else:
                    main_exploiter = "main_exploiter_" + str(
                        np.random.choice(
                            list(range(len(main_exploiters)))))
                    # Main exploiter is frozen: Play against the main policy.
                    if main_exploiter not in trainable_policies:
                        main = "main_0"
                    # Main exploiter is trainable: Play against any frozen main.
                    else:
                        main = np.random.choice(
                            list(main_policies - {"main_0"}))
                    print(f"{main_exploiter} vs {main}")
                    return main_exploiter if \
                        episode.episode_id % 2 == agent_id else main

            # Add and initialize the first learning league- and
            # main-exploiters.
            if initializing_first_exploiters:
                main_state = trainer.get_policy("main_0").get_state()

                # Add/initialize `league_exploiter_1`.
                trainer.add_policy(
                    policy_id="league_exploiter_1",
                    policy_cls=type(trainer.get_policy("main_0")),
                    policy_state=main_state,
                    policy_mapping_fn=policy_mapping_fn,
                    policies_to_train=trainable_policies,
                )
                # Add/initialize `main_exploiter_1`.
                trainer.add_policy(
                    policy_id="main_exploiter_1",
                    policy_cls=type(trainer.get_policy("main_0")),
                    policy_state=main_state,
                    policy_mapping_fn=policy_mapping_fn,
                    policies_to_train=trainable_policies,
                )

            # Add and set the weights of the new polic(y/ies).
            state = trainer.get_policy(policy_id).get_state()
            trainer.add_policy(
                policy_id=new_pol_id,
                policy_cls=type(trainer.get_policy(policy_id)),
                policy_state=state,
                policy_mapping_fn=policy_mapping_fn,
                policies_to_train=trainer.trainable_policies,
            )

        else:
            print("not good enough; will keep learning ...")
