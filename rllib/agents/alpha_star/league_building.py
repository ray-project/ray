import numpy as np
import re

from ray.rllib.agents.trainer import Trainer
from ray.rllib.utils.typing import ResultDict


def league_building_fn(trainer: Trainer, result: ResultDict, **kwargs):

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
        if policy_id in trainer.non_trainable_policies:
            continue

        print(
            f"Iter={trainer.iteration} {policy_id}'s "
            f"win-rate={win_rate} -> ",
            end="")

        # If win rate is good enough -> Snapshot current policy and decide,
        # whether to freeze the new snapshot or not.
        if win_rate > trainer.config["win_rate_threshold_for_new_snapshot"]:
            is_main = re.match("^main(_\\d+)?$", policy_id)
            initializing_exploiters = False

            # First time, main manages a decent win-rate against random:
            # Add league_exploiter_0 and main_exploiter_0 to the mix.
            if is_main and len(trainer.trainable_policies) == 3:
                initializing_exploiters = True
                trainer.trainable_policies.add("league_exploiter_1")
                trainer.trainable_policies.add("main_exploiter_1")
            else:
                keep_training = False if is_main else np.random.choice(
                    [True, False], p=[0.3, 0.7])
                if policy_id in trainer.main_policies:
                    new_pol_id = re.sub("_\\d+$",
                                        f"_{len(trainer.main_policies) - 1}",
                                        policy_id)
                    trainer.main_policies.add(new_pol_id)
                elif policy_id in trainer.main_exploiters:
                    new_pol_id = re.sub("_\\d+$",
                                        f"_{len(trainer.main_exploiters)}",
                                        policy_id)
                    trainer.main_exploiters.add(new_pol_id)
                else:
                    new_pol_id = re.sub("_\\d+$",
                                        f"_{len(trainer.league_exploiters)}",
                                        policy_id)
                    trainer.league_exploiters.add(new_pol_id)

                if keep_training:
                    trainer.trainable_policies.add(new_pol_id)
                else:
                    trainer.non_trainable_policies.add(new_pol_id)

                print(f"adding new opponents to the mix ({new_pol_id}).")

            # Update our mapping function accordingly.
            def policy_mapping_fn(agent_id, episode, worker, **kwargs):

                # Pick, whether this is ...
                type_ = np.random.choice([1, 2])

                # 1) League exploiter vs any other.
                if type_ == 1:
                    league_exploiter = "league_exploiter_" + str(
                        np.random.choice(
                            list(range(len(trainer.league_exploiters)))))
                    # This league exploiter is a frozen: Play against a
                    # trainable policy.
                    if league_exploiter not in trainer.trainable_policies:
                        opponent = np.random.choice(
                            list(trainer.trainable_policies))
                    # League exploiter is trainable: Play against any other
                    # non-trainable policy.
                    else:
                        opponent = np.random.choice(
                            list(trainer.non_trainable_policies))
                    print(f"{league_exploiter} vs {opponent}")
                    return league_exploiter if \
                        episode.episode_id % 2 == agent_id else opponent

                # 2) Main exploiter vs main.
                else:
                    main_exploiter = "main_exploiter_" + str(
                        np.random.choice(
                            list(range(len(trainer.main_exploiters)))))
                    # Main exploiter is frozen: Play against the main policy.
                    if main_exploiter not in trainer.trainable_policies:
                        main = "main"
                    # Main exploiter is trainable: Play against any frozen main.
                    else:
                        main = np.random.choice(
                            list(trainer.main_policies - {"main"}))
                    # print(f"{main_exploiter} vs {main}")
                    return main_exploiter if \
                        episode.episode_id % 2 == agent_id else main

            # Set the weights of the new polic(y/ies).
            if initializing_exploiters:
                main_state = trainer.get_policy("main").get_state()
                pol_map = trainer.workers.local_worker().policy_map
                pol_map["main_0"].set_state(main_state)
                pol_map["league_exploiter_1"].set_state(main_state)
                pol_map["main_exploiter_1"].set_state(main_state)
                # We need to sync the just copied local weights to all the
                # remote workers as well.
                trainer.workers.sync_weights(policies=[
                    "main_0", "league_exploiter_1", "main_exploiter_1"
                ])

                def _set(worker):
                    worker.set_policy_mapping_fn(policy_mapping_fn)
                    worker.set_policies_to_train(trainer.trainable_policies)

                trainer.workers.foreach_worker(_set)
            else:
                new_policy = trainer.add_policy(
                    policy_id=new_pol_id,
                    policy_cls=type(trainer.get_policy(policy_id)),
                    policy_mapping_fn=policy_mapping_fn,
                    policies_to_train=trainer.trainable_policies,
                )
                main_state = trainer.get_policy(policy_id).get_state()
                new_policy.set_state(main_state)
                # We need to sync the just copied local weights to all the
                # remote workers as well.
                trainer.workers.sync_weights(policies=[new_pol_id])

        else:
            print("not good enough; will keep learning ...")
