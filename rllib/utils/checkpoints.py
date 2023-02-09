import json
import os
from packaging import version
import tempfile
import re
from typing import Any, Dict, Union

import ray
from ray.air.checkpoint import Checkpoint
from ray.util.annotations import PublicAPI

# The current checkpoint version used by RLlib for Algorithm and Policy checkpoints.
# History:
# 0.1: Ray 2.0.0
#  A single `checkpoint-[iter num]` file for Algorithm checkpoints
#  within the checkpoint directory. Policy checkpoints not supported across all
#  DL frameworks.

# 1.0: Ray >=2.1.0
#  An algorithm_state.pkl file for the state of the Algorithm (excluding
#  individual policy states).
#  One sub-dir inside the "policies" sub-dir for each policy with a
#  dedicated policy_state.pkl in it for the policy state.
CHECKPOINT_VERSION = version.Version("1.0")


@PublicAPI(stability="alpha")
def get_checkpoint_info(checkpoint: Union[str, Checkpoint]) -> Dict[str, Any]:
    """Returns a dict with information about a Algorithm/Policy checkpoint.

    Args:
        checkpoint: The checkpoint directory (str) or an AIR Checkpoint object.

    Returns:
        A dict containing the keys:
        "type": One of "Policy" or "Algorithm".
        "checkpoint_version": A version tuple, e.g. v1.0, indicating the checkpoint
        version. This will help RLlib to remain backward compatible wrt. future
        Ray and checkpoint versions.
        "checkpoint_dir": The directory with all the checkpoint files in it. This might
        be the same as the incoming `checkpoint` arg.
        "state_file": The main file with the Algorithm/Policy's state information in it.
        This is usually a pickle-encoded file.
        "policy_ids": An optional set of PolicyIDs in case we are dealing with an
        Algorithm checkpoint. None if `checkpoint` is a Policy checkpoint.
    """
    # Default checkpoint info.
    info = {
        "type": "Algorithm",
        "checkpoint_version": version.Version("1.0"),
        "checkpoint_dir": None,
        "state_file": None,
        "policy_ids": None,
    }

    # `checkpoint` is a Checkpoint instance: Translate to directory and continue.
    if isinstance(checkpoint, Checkpoint):
        tmp_dir = tempfile.mkdtemp()
        checkpoint.to_directory(tmp_dir)
        checkpoint = tmp_dir

    # Checkpoint is dir.
    if os.path.isdir(checkpoint):
        # Figure out whether this is an older checkpoint format
        # (with a `checkpoint-\d+` file in it).
        for file in os.listdir(checkpoint):
            path_file = os.path.join(checkpoint, file)
            if os.path.isfile(path_file):
                if re.match("checkpoint-\\d+", file):
                    info.update(
                        {
                            "checkpoint_version": version.Version("0.1"),
                            "checkpoint_dir": checkpoint,
                            "state_file": path_file,
                        }
                    )
                    return info

        # No old checkpoint file found.

        # Policy checkpoint file found.
        if os.path.isfile(os.path.join(checkpoint, "policy_state.pkl")):
            info.update(
                {
                    "type": "Policy",
                    "checkpoint_version": version.Version("1.0"),
                    "checkpoint_dir": checkpoint,
                    "state_file": os.path.join(checkpoint, "policy_state.pkl"),
                }
            )
            return info

        # >v0 Algorithm checkpoint file found?
        state_file = os.path.join(checkpoint, "algorithm_state.pkl")
        if not os.path.isfile(state_file):
            raise ValueError(
                "Given checkpoint does not seem to be valid! No file "
                "with the name `algorithm_state.pkl` (or `checkpoint-[0-9]+`) found."
            )

        info.update(
            {
                "checkpoint_dir": checkpoint,
                "state_file": state_file,
            }
        )

        # Collect all policy IDs in the sub-dir "policies/".
        policies_dir = os.path.join(checkpoint, "policies")
        if os.path.isdir(policies_dir):
            policy_ids = set()
            for policy_id in os.listdir(policies_dir):
                policy_ids.add(policy_id)
            info.update({"policy_ids": policy_ids})

    # Checkpoint is a file: Use as-is (interpreting it as old Algorithm checkpoint
    # version).
    elif os.path.isfile(checkpoint):
        info.update(
            {
                "checkpoint_version": version.Version("0.1"),
                "checkpoint_dir": os.path.dirname(checkpoint),
                "state_file": checkpoint,
            }
        )

    else:
        raise ValueError(
            f"Given checkpoint ({checkpoint}) not found! Must be a "
            "checkpoint directory (or a file for older checkpoint versions)."
        )

    return info


@PublicAPI(stability="beta")
def create_python_version_independent_checkpoint(
    checkpoint: Union[str, Checkpoint],
    python_version_independent_checkpoint_dir: str,
) -> None:
    from ray.rllib.algorithms import Algorithm

    # Restore the Algorithm using the python version dependent checkpoint.
    algo = Algorithm.from_checkpoint(checkpoint)
    state = algo.__getstate__()

    # TEST
    msgpack_able_state = state.copy()
    del msgpack_able_state["config"]
    del msgpack_able_state["algorithm_class"]
    del msgpack_able_state["worker"]["policy_states"]
    del msgpack_able_state["worker"]["policy_mapping_fn"]
    # END:TEST

    # Extract policy states from worker state (Policies get their own
    # checkpoint sub-dirs).
    policy_states = {}
    if "worker" in state and "policy_states" in state["worker"]:
        policy_states = state["worker"].pop("policy_states", {})

    # Add RLlib checkpoint version.
    state["checkpoint_version"] = CHECKPOINT_VERSION

    # Write state (w/o policies) to disk.
    checkpoint_dir = python_version_independent_checkpoint_dir
    state_file = os.path.join(checkpoint_dir, "algorithm_state.msgpck")
    with open(state_file, "wb") as f:
        pickle.dump(state, f)

    # Write rllib_checkpoint.json.
    with open(os.path.join(checkpoint_dir, "rllib_checkpoint.json"), "w") as f:
        json.dump(
            {
                "type": "Algorithm",
                "checkpoint_version": str(state["checkpoint_version"]),
                "format": "msgpack",
                "ray_version": ray.__version__,
                "ray_commit": ray.__commit__,
            },
            f,
        )

    # Write individual policies to disk, each in their own sub-directory.
    for pid, policy_state in policy_states.items():
        # From here on, disallow policyIDs that would not work as directory names.
        validate_policy_id(pid, error=True)
        policy_dir = os.path.join(checkpoint_dir, "policies", pid)
        os.makedirs(policy_dir, exist_ok=True)
        policy = algo.get_policy(pid)
        policy.export_checkpoint(policy_dir, policy_state=policy_state)
