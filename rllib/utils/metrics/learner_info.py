from collections import defaultdict
import numpy as np
import tree  # pip install dm_tree


# By convention, metrics from optimizing the loss can be reported in the
# `grad_info` dict returned by learn_on_batch() / compute_grads() via this key.
LEARNER_STATS_KEY = "learner_stats"


class LearnerInfoBuilder:
    def __init__(self, num_devices: int = 1):
        self.num_devices = num_devices
        self.results_all_towers = defaultdict(list)
        self.is_finalized = False

    def add_learn_on_batch_results(self, policy_id, results):
        assert not self.is_finalized, \
            "LearnerInfo already finalized! Cannot add more results."

        # No towers: Single CPU.
        if "tower_0" not in results:
            self.results_all_towers[policy_id].append(results)
        # Multi-GPU case:
        else:
            self.results_all_towers[policy_id].append(
                tree.map_structure_with_path(
                    lambda p, *s: all_tower_reduce(p, *s),
                    *(results.pop(
                        "tower_{}".format(tower_num))
                        for tower_num in range(self.num_devices))))
            for k, v in results.items():
                if k == LEARNER_STATS_KEY:
                    for k1, v1 in results[k].items():
                        self.results_all_towers[policy_id][-1][
                            LEARNER_STATS_KEY][k1] = v1
                else:
                    self.results_all_towers[policy_id][-1][k] = v

        #        for k, v in batch_fetches.get(LEARNER_STATS_KEY, {}).items():
        #            learner_stats[k].append(v)
        #        for k, v in batch_fetches.get("model", {}).items():
        #            model_stats[k].append(v)
        #        for k, v in batch_fetches.get("custom_metrics", {}).items():
        #            custom_callbacks_stats[k].append(v)
        #fetches[policy_id][LEARNER_STATS_KEY] = averaged(learner_stats)
        #fetches[policy_id]["model"] = averaged(model_stats)
        #fetches[policy_id]["custom_metrics"] = averaged(custom_callbacks_stats)

    def finalize(self):
        self.is_finalized = True

        info = {}
        for policy_id, results_all_towers in self.results_all_towers.items():
            # Reduce mean across all minibatch SGD steps (axis=0 to keep
            # all shapes as-is).
            info[policy_id] = tree.map_structure(
                lambda *s: None if s[0] is None else np.nanmean(s, axis=0),
                *results_all_towers)

        return info


def all_tower_reduce(path, *tower_data):
    """Reduces stats across towers based on their stats-dict paths."""
    # TD-errors: Need to stay per batch item in order to be able to update
    # each item's weight in a prioritized replay buffer.
    if len(path) == 1 and path[0] == "td_error":
        return np.concatenate(tower_data, axis=0)
    # Min stats: Reduce min.
    elif path[-1].startswith("min_"):
        return np.nanmin(tower_data)
    # Max stats: Reduce max.
    elif path[-1].startswith("max_"):
        return np.nanmax(tower_data)
    # Everything else: Reduce mean.
    return np.nanmean(tower_data)

#def averaged(kv, axis=None):
#    """Average the value lists of a dictionary.

#    For non-scalar values, we simply pick the first value.

#    Args:
#        kv (dict): dictionary with values that are lists of floats.

#    Returns:
#        dictionary with single averaged float as values.
#    """
#    out = {}
#    for k, v in kv.items():
#        if v[0] is not None and not isinstance(v[0], dict):
#            out[k] = np.mean(v, axis=axis)
#        else:
#            out[k] = v[0]
#    return out
