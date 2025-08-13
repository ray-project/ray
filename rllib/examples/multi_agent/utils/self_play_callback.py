from collections import defaultdict

import numpy as np

from ray.rllib.callbacks.callbacks import RLlibCallback
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.utils.metrics import ENV_RUNNER_RESULTS


class SelfPlayCallback(RLlibCallback):
    def __init__(self, win_rate_threshold):
        super().__init__()
        # 0=RandomPolicy, 1=1st main policy snapshot,
        # 2=2nd main policy snapshot, etc..
        self.current_opponent = 0

        self.win_rate_threshold = win_rate_threshold

        # Report the matchup counters (who played against whom?).
        self._matching_stats = defaultdict(int)

    def on_episode_end(
        self,
        *,
        episode,
        env_runner,
        metrics_logger,
        env,
        env_index,
        rl_module,
        **kwargs,
    ) -> None:
        # Compute the win rate for this episode and log it with a window of 100.
        main_agent = 0 if episode.module_for(0) == "main" else 1
        rewards = episode.get_rewards()
        if main_agent in rewards:
            main_won = rewards[main_agent][-1] == 1.0
            metrics_logger.log_value(
                "win_rate",
                main_won,
                reduce="mean",
                window=100,
            )

    def on_train_result(self, *, algorithm, metrics_logger=None, result, **kwargs):
        win_rate = result[ENV_RUNNER_RESULTS]["win_rate"]
        print(f"Iter={algorithm.iteration} win-rate={win_rate} -> ", end="")
        # If win rate is good -> Snapshot current policy and play against
        # it next, keeping the snapshot fixed and only improving the "main"
        # policy.
        if win_rate > self.win_rate_threshold:
            self.current_opponent += 1
            new_module_id = f"main_v{self.current_opponent}"
            print(f"adding new opponent to the mix ({new_module_id}).")

            # Re-define the mapping function, such that "main" is forced
            # to play against any of the previously played modules
            # (excluding "random").
            def agent_to_module_mapping_fn(agent_id, episode, **kwargs):
                # agent_id = [0|1] -> policy depends on episode ID
                # This way, we make sure that both modules sometimes play
                # (start player) and sometimes agent1 (player to move 2nd).
                opponent = "main_v{}".format(
                    np.random.choice(list(range(1, self.current_opponent + 1)))
                )
                if hash(episode.id_) % 2 == agent_id:
                    self._matching_stats[("main", opponent)] += 1
                    return "main"
                else:
                    return opponent

            main_module = algorithm.get_module("main")
            algorithm.add_module(
                module_id=new_module_id,
                module_spec=RLModuleSpec.from_module(main_module),
                new_agent_to_module_mapping_fn=agent_to_module_mapping_fn,
            )
            # TODO (sven): Maybe we should move this convenience step back into
            #  `Algorithm.add_module()`? Would be less explicit, but also easier.
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
        else:
            print("not good enough; will keep learning ...")

        # +2 = main + random
        result["league_size"] = self.current_opponent + 2

        print(f"Matchups:\n{self._matching_stats}")
