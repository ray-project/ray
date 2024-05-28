from collections import defaultdict

import numpy as np

from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.utils.metrics import ENV_RUNNER_RESULTS


class SelfPlayCallback(DefaultCallbacks):
    def __init__(self, win_rate_threshold):
        super().__init__()
        # 0=RandomPolicy, 1=1st main policy snapshot,
        # 2=2nd main policy snapshot, etc..
        self.current_opponent = 0

        self.win_rate_threshold = win_rate_threshold

        # Report the matchup counters (who played against whom?).
        self._matching_stats = defaultdict(int)

    def on_train_result(self, *, algorithm, metrics_logger=None, result, **kwargs):
        # Get the win rate for the train batch.
        # Note that normally, one should set up a proper evaluation config,
        # such that evaluation always happens on the already updated policy,
        # instead of on the already used train_batch.
        main_rew = result[ENV_RUNNER_RESULTS]["hist_stats"].pop("policy_main_reward")
        opponent_rew = list(result[ENV_RUNNER_RESULTS]["hist_stats"].values())[0]
        assert len(main_rew) == len(opponent_rew)
        won = 0
        for r_main, r_opponent in zip(main_rew, opponent_rew):
            if r_main > r_opponent:
                won += 1
        win_rate = won / len(main_rew)
        result["win_rate"] = win_rate
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
                module_spec=SingleAgentRLModuleSpec.from_module(main_module),
                module_state=main_module.get_state(),
                new_agent_to_module_mapping_fn=agent_to_module_mapping_fn,
            )
        else:
            print("not good enough; will keep learning ...")

        # +2 = main + random
        result["league_size"] = self.current_opponent + 2

        print(f"Matchups:\n{self._matching_stats}")
