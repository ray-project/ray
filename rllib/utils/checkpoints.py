import logging
import json
import os
from packaging import version
import tempfile
import re
from typing import Any, Dict, Union

import ray
from ray.air.checkpoint import Checkpoint
from ray.rllib.utils.serialization import NOT_SERIALIZABLE, serialize_type
from ray.util import log_once
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)

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

# 1.1: Same as 1.0, but has a new "format" field in the rllib_checkpoint.json file
# indicating, whether the checkpoint is `cloudpickle` (default) or `msgpack`.

# 1.2: Introduces the checkpoint for the new Learner API if the Learner api is enabled.

CHECKPOINT_VERSION = version.Version("1.1")
CHECKPOINT_VERSION_LEARNER = version.Version("1.2")


@PublicAPI(stability="alpha")
def get_checkpoint_info(checkpoint: Union[str, Checkpoint]) -> Dict[str, Any]:
    """Returns a dict with information about a Algorithm/Policy checkpoint.

    If the given checkpoint is a >=v1.0 checkpoint directory, try reading all
    information from the contained `rllib_checkpoint.json` file.

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
        "format": "cloudpickle",
        "checkpoint_version": CHECKPOINT_VERSION,
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
        info.update({"checkpoint_dir": checkpoint})

        # Figure out whether this is an older checkpoint format
        # (with a `checkpoint-\d+` file in it).
        for file in os.listdir(checkpoint):
            path_file = os.path.join(checkpoint, file)
            if os.path.isfile(path_file):
                if re.match("checkpoint-\\d+", file):
                    info.update(
                        {
                            "checkpoint_version": version.Version("0.1"),
                            "state_file": path_file,
                        }
                    )
                    return info

        # No old checkpoint file found.

        # If rllib_checkpoint.json file present, read available information from it
        # and then continue with the checkpoint analysis (possibly overriding further
        # information).
        if os.path.isfile(os.path.join(checkpoint, "rllib_checkpoint.json")):
            with open(os.path.join(checkpoint, "rllib_checkpoint.json")) as f:
                rllib_checkpoint_info = json.load(fp=f)
            if "checkpoint_version" in rllib_checkpoint_info:
                rllib_checkpoint_info["checkpoint_version"] = version.Version(
                    rllib_checkpoint_info["checkpoint_version"]
                )
            info.update(rllib_checkpoint_info)
        else:
            # No rllib_checkpoint.json file present: Warn and continue trying to figure
            # out checkpoint info ourselves.
            if log_once("no_rllib_checkpoint_json_file"):
                logger.warning(
                    "No `rllib_checkpoint.json` file found in checkpoint directory "
                    f"{checkpoint}! Trying to extract checkpoint info from other files "
                    f"found in that dir."
                )

        # Policy checkpoint file found.
        for extension in ["pkl", "msgpck"]:
            if os.path.isfile(os.path.join(checkpoint, "policy_state." + extension)):
                info.update(
                    {
                        "type": "Policy",
                        "format": "cloudpickle" if extension == "pkl" else "msgpack",
                        "checkpoint_version": CHECKPOINT_VERSION,
                        "state_file": os.path.join(
                            checkpoint, f"policy_state.{extension}"
                        ),
                    }
                )
                return info

        # Valid Algorithm checkpoint >v0 file found?
        format = None
        for extension in ["pkl", "msgpck"]:
            state_file = os.path.join(checkpoint, f"algorithm_state.{extension}")
            if os.path.isfile(state_file):
                format = "cloudpickle" if extension == "pkl" else "msgpack"
                break
        if format is None:
            raise ValueError(
                "Given checkpoint does not seem to be valid! No file with the name "
                "`algorithm_state.[pkl|msgpck]` (or `checkpoint-[0-9]+`) found."
            )

        info.update(
            {
                "format": format,
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
def convert_to_msgpack_checkpoint(
    checkpoint: Union[str, Checkpoint],
    msgpack_checkpoint_dir: str,
) -> str:
    """Converts an Algorithm checkpoint (pickle based) to a msgpack based one.

    Msgpack has the advantage of being python version independent.

    Args:
        checkpoint: The directory, in which to find the Algorithm checkpoint (pickle
            based).
        msgpack_checkpoint_dir: The directory, in which to create the new msgpack
            based checkpoint.

    Returns:
        The directory in which the msgpack checkpoint has been created. Note that
        this is the same as `msgpack_checkpoint_dir`.
    """
    from ray.rllib.algorithms import Algorithm
    from ray.rllib.utils.policy import validate_policy_id

    # Try to import msgpack and msgpack_numpy.
    msgpack = try_import_msgpack(error=True)

    # Restore the Algorithm using the python version dependent checkpoint.
    algo = Algorithm.from_checkpoint(checkpoint)
    state = algo.__getstate__()

    # Convert all code in state into serializable data.
    # Serialize the algorithm class.
    state["algorithm_class"] = serialize_type(state["algorithm_class"])
    # Serialize the algorithm's config object.
    state["config"] = state["config"].serialize()

    # Extract policy states from worker state (Policies get their own
    # checkpoint sub-dirs).
    policy_states = {}
    if "worker" in state and "policy_states" in state["worker"]:
        policy_states = state["worker"].pop("policy_states", {})

    # Policy mapping fn.
    state["worker"]["policy_mapping_fn"] = NOT_SERIALIZABLE
    # Is Policy to train function.
    state["worker"]["is_policy_to_train"] = NOT_SERIALIZABLE

    # Add RLlib checkpoint version (as string).
    if state["config"]["_enable_learner_api"]:
        state["checkpoint_version"] = str(CHECKPOINT_VERSION_LEARNER)
    else:
        state["checkpoint_version"] = str(CHECKPOINT_VERSION)

    # Write state (w/o policies) to disk.
    state_file = os.path.join(msgpack_checkpoint_dir, "algorithm_state.msgpck")
    with open(state_file, "wb") as f:
        msgpack.dump(state, f)

    # Write rllib_checkpoint.json.
    with open(os.path.join(msgpack_checkpoint_dir, "rllib_checkpoint.json"), "w") as f:
        json.dump(
            {
                "type": "Algorithm",
                "checkpoint_version": state["checkpoint_version"],
                "format": "msgpack",
                "state_file": state_file,
                "policy_ids": list(policy_states.keys()),
                "ray_version": ray.__version__,
                "ray_commit": ray.__commit__,
            },
            f,
        )

    # Write individual policies to disk, each in their own sub-directory.
    for pid, policy_state in policy_states.items():
        # From here on, disallow policyIDs that would not work as directory names.
        validate_policy_id(pid, error=True)
        policy_dir = os.path.join(msgpack_checkpoint_dir, "policies", pid)
        os.makedirs(policy_dir, exist_ok=True)
        policy = algo.get_policy(pid)
        policy.export_checkpoint(
            policy_dir,
            policy_state=policy_state,
            checkpoint_format="msgpack",
        )

    # Release all resources used by the Algorithm.
    algo.stop()

    return msgpack_checkpoint_dir


@PublicAPI
def try_import_msgpack(error: bool = False):
    """Tries importing msgpack and msgpack_numpy and returns the patched msgpack module.

    Returns None if error is False and msgpack or msgpack_numpy is not installed.
    Raises an error, if error is True and the modules could not be imported.

    Args:
        error: Whether to raise an error if msgpack/msgpack_numpy cannot be imported.

    Returns:
        The `msgpack` module.

    Raises:
        ImportError: If error=True and msgpack/msgpack_numpy is not installed.
    """
    try:
        import msgpack
        import msgpack_numpy

        # Make msgpack_numpy look like msgpack.
        msgpack_numpy.patch()

        return msgpack

    except Exception:
        if error:
            raise ImportError(
                "Could not import or setup msgpack and msgpack_numpy! "
                "Try running `pip install msgpack msgpack_numpy` first."
            )
