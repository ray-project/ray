import abc
import logging
import json
import os
from packaging import version
import pathlib
import re
import tempfile
from typing import Any, Collection, Dict, List, Optional, Tuple, Union

import ray
import ray.cloudpickle as pickle
from ray.rllib.utils.actor_manager import FaultTolerantActorManager
from ray.rllib.utils.annotations import (
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)
from ray.rllib.utils.serialization import NOT_SERIALIZABLE, serialize_type
from ray.rllib.utils.typing import StateDict
from ray.train import Checkpoint
from ray.tune.utils.file_transfer import sync_dir_between_nodes
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

# 1.2: Introduces the checkpoint for the new Learner API if the Learner API is enabled.

# 2.0: Introduces the Checkpointable API for all components on the new API stack
# (if the Learner-, RLModule, EnvRunner, and ConnectorV2 APIs are enabled).

CHECKPOINT_VERSION = version.Version("1.1")
CHECKPOINT_VERSION_LEARNER = version.Version("1.2")
CHECKPOINT_VERSION_LEARNER_AND_ENV_RUNNER = version.Version("2.0")


@PublicAPI(stability="alpha")
class Checkpointable(abc.ABC):
    """Abstract base class for a component of RLlib that can be checkpointed to disk.

    Subclasses must implement the following APIs:
    - save_to_path()
    - restore_from_path()
    - from_checkpoint()
    - get_state()
    - set_state()
    - get_ctor_args_and_kwargs()
    - get_metadata()
    - get_checkpointable_components()
    """

    # The state file for the implementing class.
    # This file contains any state information that does NOT belong to any subcomponent
    # of the implementing class (which are `Checkpointable` themselves and thus should
    # have their own state- and metadata files).
    # After a `save_to_path([path])` this file can be found directly in: `path/`.
    STATE_FILE_NAME = "state.pkl"

    # The filename of the pickle file that contains the class information of the
    # Checkpointable as well as all constructor args to be passed to such a class in
    # order to construct a new instance.
    CLASS_AND_CTOR_ARGS_FILE_NAME = "class_and_ctor_args.pkl"

    # Subclasses may set this to their own metadata filename.
    # The dict returned by self.get_metadata() is stored in this JSON file.
    METADATA_FILE_NAME = "metadata.json"

    def save_to_path(
        self,
        path: Optional[Union[str, pathlib.Path]] = None,
        state: Optional[StateDict] = None,
    ) -> str:
        """Saves the state of the implementing class (or `state`) to `path`.

        The state of the implementing class is always saved in the following format:

        .. testcode::
            :skipif: True

            path/
                [component1]/
                    [component1 subcomponentA]/
                        ...
                    [component1 subcomponentB]/
                        ...
                [component2]/
                        ...
                [cls.METADATA_FILE_NAME].json
                [cls.STATE_FILE_NAME].pkl

        Args:
            path: The path to the directory to save the state of the implementing class
                to. If `path` doesn't exist or is None, then a new directory will be
                created (and returned).
            state: An optional state dict to be used instead of getting a new state of
                the implementing class through `self.get_state()`.

        Returns:
            The path (str) where the state has been saved.
        """
        # Create path, if necessary.
        if path is None:
            path = path or tempfile.mkdtemp()

        # Make sure, path exists.
        path = pathlib.Path(path)
        path.mkdir(parents=True, exist_ok=True)

        # Write metadata file to disk.
        metadata = self.get_metadata()
        if "checkpoint_version" not in metadata:
            metadata["checkpoint_version"] = str(
                CHECKPOINT_VERSION_LEARNER_AND_ENV_RUNNER
            )
        with open(path / self.METADATA_FILE_NAME, "w") as f:
            json.dump(metadata, f)

        # Write the class and constructor args information to disk.
        with open(path / self.CLASS_AND_CTOR_ARGS_FILE_NAME, "wb") as f:
            pickle.dump(
                {
                    "class": type(self),
                    "ctor_args_and_kwargs": self.get_ctor_args_and_kwargs(),
                },
                f,
            )

        # Get the entire state of this Checkpointable, or use provided `state`.
        state = state or self.get_state()

        # Write components of `self` that themselves are `Checkpointable`.
        for component_name, component in self.get_checkpointable_components():
            # If subcomponent's name is not in `state`, ignore it and don't write this
            # subcomponent's state to disk.
            if component_name not in state:
                continue
            component_state = state.pop(component_name)
            component_path = path / component_name

            # If component is an ActorManager, save the manager's first healthy
            # actor's state to disk (even if it's on another node, in which case, we'll
            # sync the generated file(s) back to this node).
            if isinstance(component, FaultTolerantActorManager):
                actor_to_use = component.healthy_actor_ids()[0]

                def _get_ip(_=None):
                    import ray

                    return ray.util.get_node_ip_address()

                _result = next(
                    iter(
                        component.foreach_actor(
                            _get_ip,
                            remote_actor_ids=[actor_to_use],
                        )
                    )
                )
                if not _result.ok:
                    raise _result.get()
                worker_ip_addr = _result.get()
                self_ip_addr = _get_ip()

                # Save the state to a temporary location on the `actor_to_use`'s
                # node.
                component_state_ref = ray.put(component_state)

                if worker_ip_addr == self_ip_addr:
                    component.foreach_actor(
                        lambda w, _path=component_path, _state=component_state_ref: (
                            w.save_to_path(_path, state=ray.get(_state))
                        ),
                        remote_actor_ids=[actor_to_use],
                    )
                else:
                    # Save the checkpoint to the temporary directory on the worker.
                    def _save(_worker, _state=component_state_ref):
                        import tempfile

                        # Create a temporary directory on the worker.
                        tmpdir = tempfile.mkdtemp()
                        _worker.save_to_path(tmpdir, state=ray.get(_state))
                        return tmpdir

                    _result = next(
                        iter(
                            component.foreach_actor(
                                _save, remote_actor_ids=[actor_to_use]
                            )
                        )
                    )
                    if not _result.ok:
                        raise _result.get()
                    worker_temp_dir = _result.get()

                    # Sync the temporary directory from the worker to this node.
                    sync_dir_between_nodes(
                        worker_ip_addr,
                        worker_temp_dir,
                        self_ip_addr,
                        str(component_path),
                    )

                    # Remove the temporary directory on the worker.
                    def _rmdir(w, _dir=worker_temp_dir):
                        import shutil

                        shutil.rmtree(_dir)

                    component.foreach_actor(_rmdir, remote_actor_ids=[actor_to_use])

            # Local component (instance stored in a property of `self`).
            else:
                # By providing the `state` arg, we make sure that the component does not
                # have to call its own `get_state()` anymore, but uses what's provided
                # here.
                component.save_to_path(path / component_name, state=component_state)

        # Write all the remaining state to disk.
        with open(path / self.STATE_FILE_NAME, "wb") as f:
            pickle.dump(state, f)

        return str(path)

    def restore_from_path(self, path: Union[str, pathlib.Path], **kwargs) -> None:
        """Restores the state of the implementing class from the given path.

        The given `path` should have the following structure and contain the following
        files:

        .. testcode::
            :skipif: True

            path/
                [component1]/
                    [component1 subcomponentA]/
                        ...
                    [component1 subcomponentB]/
                        ...
                [component2]/
                        ...
                [cls.STATE_FILE_NAME].pkl

        Note that the self.METADATA_FILE_NAME file is not required to restore the state.

        Args:
            path: The path to load the implementing class' state from.
            **kwargs: Forward compatibility kwargs.
        """
        path = pathlib.Path(path)

        # Restore components of `self` that themselves are `Checkpointable`.
        for component_name, component in self.get_checkpointable_components():
            component_dir = path / component_name
            # If subcomponent's dir is not in path, ignore it and don't restore this
            # subcomponent's state from disk.
            if not component_dir.is_dir():
                continue
            # Call `restore_from_path()` on subcomponent, thereby passing in the
            # **kwargs.
            component.restore_from_path(component_dir, **kwargs)

        state = pickle.load(open(path / self.STATE_FILE_NAME, "rb"))
        self.set_state(state)

    @classmethod
    def from_checkpoint(
        cls, path: Union[str, pathlib.Path], **kwargs
    ) -> "Checkpointable":
        """Creates a new Checkpointable instance from the given location and returns it.

        Args:
            path: The checkpoint path to load (a) the information on how to construct
                a new instance of the implementing class and (b) the state to restore
                the created instance to.
            kwargs: Forward compatibility kwargs. Note that these kwargs are sent to
                each subcomponent's `from_checkpoint()` call.

        Returns:
             A new instance of the implementing class, already set to the state stored
             under `path`.
        """
        path = pathlib.Path(path)

        # Get the class constructor to call.
        ctor_info = pickle.load(open(path / cls.CLASS_AND_CTOR_ARGS_FILE_NAME, "rb"))
        # Construct an initial object.
        obj = ctor_info["class"](
            *ctor_info["ctor_args_and_kwargs"][0],
            **ctor_info["ctor_args_and_kwargs"][1],
        )
        # Restore the state of the constructed object.
        obj.restore_from_path(path, **kwargs)
        # Return the new object.
        return obj

    @abc.abstractmethod
    def get_state(
        self,
        components: Optional[Collection[str]] = None,
        *,
        not_components: Optional[Collection[str]] = None,
        **kwargs,
    ) -> StateDict:
        """Returns the implementing class's current state as a dict.

        Args:
            components: An optional list of string keys to be included in the
                returned state. This might be useful, if getting certain components
                of the state is expensive (e.g. reading/compiling the weights of a large
                NN) and at the same time, these components are not required by the
                caller.
            not_components: An optional list of string keys to be excluded in the
                returned state, even if the same string is part of `components`.
                This is useful to get the complete state of the class, except
                one or a few components.
            kwargs: Forward-compatibility kwargs.

        Returns:
            The current state of the implementing class (or only the `components`
            specified, w/o those in `not_components`).
        """

    @abc.abstractmethod
    def set_state(self, state: StateDict) -> None:
        """Sets the implementing class' state to the given state dict.

        If component keys are missing in `state`, these components of the implementing
        class will not be updated/set.

        Args:
            state: The state dict to restore the state from. Maps component keys
                to the corresponding subcomponent's own state.
        """

    @abc.abstractmethod
    def get_ctor_args_and_kwargs(self) -> Tuple[Tuple, Dict[str, Any]]:
        """Returns the args/kwargs used to create `self` from its constructor.

        Returns:
            A tuple of the args (as a tuple) and kwargs (as a Dict[str, Any]) used to
            construct `self` from its class constructor.
        """

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def get_metadata(self) -> Dict:
        """Returns JSON writable metadata further describing the implementing class.

        Note that this metadata is NOT part of any state and is thus NOT needed to
        restore the state of a Checkpointable instance from a directory. Rather, the
        metadata will be written into `self.METADATA_FILE_NAME` when calling
        `self.save_to_path()` for the user's convenience.

        Returns:
            A JSON-encodable dict of metadata information.
        """
        return {
            "class_and_ctor_args_file": self.CLASS_AND_CTOR_ARGS_FILE_NAME,
            "state_file": self.STATE_FILE_NAME,
            "ray_version": ray.__version__,
            "ray_commit": ray.__commit__,
        }

    def get_checkpointable_components(self) -> List[Tuple[str, "Checkpointable"]]:
        """Returns the implementing class's own Checkpointable subcomponents.

        Returns:
            A list of 2-tuples (name, subcomponent) describing the implementing class'
            subcomponents, all of which have to be `Checkpointable` themselves and
            whose state is therefore written into subdirectories (rather than the main
            state file (self.STATE_FILE_NAME) when calling `self.save_to_path()`).
        """
        return []


@PublicAPI(stability="alpha")
def get_checkpoint_info(checkpoint: Union[str, Checkpoint]) -> Dict[str, Any]:
    """Returns a dict with information about an Algorithm/Policy checkpoint.

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
        checkpoint: str = checkpoint.to_directory()

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
    if state["config"]["enable_rl_module_and_learner"]:
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

    # Write individual policies to disk, each in their own subdirectory.
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


@PublicAPI(stability="beta")
def convert_to_msgpack_policy_checkpoint(
    policy_checkpoint: Union[str, Checkpoint],
    msgpack_checkpoint_dir: str,
) -> str:
    """Converts a Policy checkpoint (pickle based) to a msgpack based one.

    Msgpack has the advantage of being python version independent.

    Args:
        policy_checkpoint: The directory, in which to find the Policy checkpoint (pickle
            based).
        msgpack_checkpoint_dir: The directory, in which to create the new msgpack
            based checkpoint.

    Returns:
        The directory in which the msgpack checkpoint has been created. Note that
        this is the same as `msgpack_checkpoint_dir`.
    """
    from ray.rllib.policy.policy import Policy

    policy = Policy.from_checkpoint(policy_checkpoint)

    os.makedirs(msgpack_checkpoint_dir, exist_ok=True)
    policy.export_checkpoint(
        msgpack_checkpoint_dir,
        policy_state=policy.get_state(),
        checkpoint_format="msgpack",
    )

    # Release all resources used by the Policy.
    del policy

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
