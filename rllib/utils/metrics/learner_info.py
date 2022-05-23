from collections import defaultdict
import numpy as np
import tree  # pip install dm_tree
from typing import Dict

from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.typing import PolicyID

# Instant metrics (keys for metrics.info).
LEARNER_INFO = "learner"
# By convention, metrics from optimizing the loss can be reported in the
# `grad_info` dict returned by learn_on_batch() / compute_grads() via this key.
LEARNER_STATS_KEY = "learner_stats"


class LearnerInfoBuilder:
    def __init__(self, num_devices: int = 1):
        self.num_devices = num_devices
        self.results_all_towers = defaultdict(list)
        self.is_finalized = False

    def add_learn_on_batch_results(
        self,
        results: Dict,
        policy_id: PolicyID = DEFAULT_POLICY_ID,
    ) -> None:
        """Adds a policy.learn_on_(loaded)?_batch() result to this builder.

        Args:
            results: The results returned by Policy.learn_on_batch or
                Policy.learn_on_loaded_batch.
            policy_id: The policy's ID, whose learn_on_(loaded)_batch method
                returned `results`.
        """
        assert (
            not self.is_finalized
        ), "LearnerInfo already finalized! Cannot add more results."

        # No towers: Single CPU.
        if "tower_0" not in results:
            self.results_all_towers[policy_id].append(results)
        # Multi-GPU case:
        else:
            self.results_all_towers[policy_id].append(
                tree.map_structure_with_path(
                    lambda p, *s: all_tower_reduce(p, *s),
                    *(
                        results.pop("tower_{}".format(tower_num))
                        for tower_num in range(self.num_devices)
                    )
                )
            )
            for k, v in results.items():
                if k == LEARNER_STATS_KEY:
                    for k1, v1 in results[k].items():
                        self.results_all_towers[policy_id][-1][LEARNER_STATS_KEY][
                            k1
                        ] = v1
                else:
                    self.results_all_towers[policy_id][-1][k] = v

    def add_learn_on_batch_results_multi_agent(
        self,
        all_policies_results: Dict,
    ) -> None:
        """Adds multiple policy.learn_on_(loaded)?_batch() results to this builder.

        Args:
            all_policies_results: The results returned by all Policy.learn_on_batch or
                Policy.learn_on_loaded_batch wrapped as a dict mapping policy ID to
                results.
        """
        for pid, result in all_policies_results.items():
            if pid != "batch_count":
                self.add_learn_on_batch_results(result, policy_id=pid)

    def finalize(self):
        self.is_finalized = True

        info = {}
        for policy_id, results_all_towers in self.results_all_towers.items():
            # Reduce mean across all minibatch SGD steps (axis=0 to keep
            # all shapes as-is).
            info[policy_id] = tree.map_structure_with_path(
                all_tower_reduce, *results_all_towers
            )

        return info


def all_tower_reduce(path, *tower_data):
    """Reduces stats across towers based on their stats-dict paths."""
    # TD-errors: Need to stay per batch item in order to be able to update
    # each item's weight in a prioritized replay buffer.
    if len(path) == 1 and path[0] == "td_error":
        return np.concatenate(tower_data, axis=0)
    elif tower_data[0] is None:
        return None

    if isinstance(path[-1], str):
        # Min stats: Reduce min.
        if path[-1].startswith("min_"):
            return np.nanmin(tower_data)
        # Max stats: Reduce max.
        elif path[-1].startswith("max_"):
            return np.nanmax(tower_data)
    # Everything else: Reduce mean.
    return np.nanmean(tower_data)
