import abc
import logging
import json
import os
from packaging import version
import pathlib
import re
import tempfile
from types import MappingProxyType
from typing import Any, Collection, Dict, List, Optional, Tuple, Union

import pyarrow.fs

import ray
import ray.cloudpickle as pickle
from ray.rllib.core import (
    COMPONENT_LEARNER,
    COMPONENT_LEARNER_GROUP,
    COMPONENT_RL_MODULE,
)
from ray.rllib.utils import force_list
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
        *,
        state: Optional[StateDict] = None,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
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
                [cls.METADATA_FILE_NAME] (json)
                [cls.STATE_FILE_NAME] (pkl)

        The main logic is to loop through all subcomponents of this Checkpointable
        and call their respective `save_to_path` methods. Then save the remaining
        (non subcomponent) state to this Checkpointable's STATE_FILE_NAME.
        In the exception that a component is a FaultTolerantActorManager instance,
        instead of calling `save_to_path` directly on that manager, the first healthy
        actor is interpreted as the component and its `save_to_path` method is called.
        Even if that actor is located on another node, the created file is automatically
        synced to the local node.

        Args:
            path: The path to the directory to save the state of the implementing class
                to. If `path` doesn't exist or is None, then a new directory will be
                created (and returned).
            state: An optional state dict to be used instead of getting a new state of
                the implementing class through `self.get_state()`.
            filesystem: PyArrow FileSystem to use to access data at the `path`.
                If not specified, this is inferred from the URI scheme of `path`.

        Returns:
            The path (str) where the state has been saved.
        """

        # If no path is given create a local temporary directory.
        if path is None:
            import uuid

            # Get the location of the temporary directory on the OS.
            tmp_dir = pathlib.Path(tempfile.gettempdir())
            # Create a random directory name.
            random_dir_name = str(uuid.uuid4())
            # Create the path, but do not craet the directory on the
            # filesystem, yet. This is done by `PyArrow`.
            path = path or tmp_dir / random_dir_name

        # We need a string path for `pyarrow.fs.FileSystem.from_uri`.
        path = path if isinstance(path, str) else path.as_posix()

        # If we have no filesystem, figure it out.
        if path and not filesystem:
            # Note the path needs to be a path that is relative to the
            # filesystem (e.g. `gs://tmp/...` -> `tmp/...`).
            filesystem, path = pyarrow.fs.FileSystem.from_uri(path)

        # Make sure, path exists.
        filesystem.create_dir(path, recursive=True)

        # Convert to `pathlib.Path` for easy handling.
        path = pathlib.Path(path)

        # Write metadata file to disk.
        metadata = self.get_metadata()
        if "checkpoint_version" not in metadata:
            metadata["checkpoint_version"] = str(
                CHECKPOINT_VERSION_LEARNER_AND_ENV_RUNNER
            )
        with filesystem.open_output_stream(
            (path / self.METADATA_FILE_NAME).as_posix()
        ) as f:
            f.write(json.dumps(metadata).encode("utf-8"))

        # Write the class and constructor args information to disk.
        with filesystem.open_output_stream(
            (path / self.CLASS_AND_CTOR_ARGS_FILE_NAME).as_posix()
        ) as f:
            pickle.dump(
                {
                    "class": type(self),
                    "ctor_args_and_kwargs": self.get_ctor_args_and_kwargs(),
                },
                f,
            )

        # Get the entire state of this Checkpointable, or use provided `state`.
        _state_provided = state is not None
        state = state or self.get_state(
            not_components=[c[0] for c in self.get_checkpointable_components()]
        )

        # Write components of `self` that themselves are `Checkpointable`.
        for comp_name, comp in self.get_checkpointable_components():
            # If subcomponent's name is not in `state`, ignore it and don't write this
            # subcomponent's state to disk.
            if _state_provided and comp_name not in state:
                continue
            comp_path = path / comp_name

            # If component is an ActorManager, save the manager's first healthy
            # actor's state to disk (even if it's on another node, in which case, we'll
            # sync the generated file(s) back to this node).
            if isinstance(comp, FaultTolerantActorManager):
                actor_to_use = comp.healthy_actor_ids()[0]

                def _get_ip(_=None):
                    import ray

                    return ray.util.get_node_ip_address()

                _result = next(
                    iter(
                        comp.foreach_actor(
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
                comp_state_ref = None
                if _state_provided:
                    comp_state_ref = ray.put(state.pop(comp_name))

                if worker_ip_addr == self_ip_addr:
                    comp.foreach_actor(
                        lambda w, _path=comp_path, _state=comp_state_ref: (
                            w.save_to_path(
                                _path,
                                state=(
                                    ray.get(_state)
                                    if _state is not None
                                    else w.get_state()
                                ),
                            )
                        ),
                        remote_actor_ids=[actor_to_use],
                    )
                else:
                    # Save the checkpoint to the temporary directory on the worker.
                    def _save(w, _state=comp_state_ref):
                        import tempfile

                        # Create a temporary directory on the worker.
                        tmpdir = tempfile.mkdtemp()
                        w.save_to_path(
                            tmpdir,
                            state=(
                                ray.get(_state) if _state is not None else w.get_state()
                            ),
                        )
                        return tmpdir

                    _result = next(
                        iter(comp.foreach_actor(_save, remote_actor_ids=[actor_to_use]))
                    )
                    if not _result.ok:
                        raise _result.get()
                    worker_temp_dir = _result.get()

                    # Sync the temporary directory from the worker to this node.
                    sync_dir_between_nodes(
                        worker_ip_addr,
                        worker_temp_dir,
                        self_ip_addr,
                        str(comp_path),
                    )

                    # Remove the temporary directory on the worker.
                    def _rmdir(_, _dir=worker_temp_dir):
                        import shutil

                        shutil.rmtree(_dir)

                    comp.foreach_actor(_rmdir, remote_actor_ids=[actor_to_use])

            # Local component (instance stored in a property of `self`).
            else:
                if _state_provided:
                    comp_state = state.pop(comp_name)
                else:
                    comp_state = self.get_state(components=comp_name)[comp_name]
                # By providing the `state` arg, we make sure that the component does not
                # have to call its own `get_state()` anymore, but uses what's provided
                # here.
                comp.save_to_path(comp_path, filesystem=filesystem, state=comp_state)

        # Write all the remaining state to disk.
        with filesystem.open_output_stream(
            (path / self.STATE_FILE_NAME).as_posix()
        ) as f:
            pickle.dump(state, f)

        return str(path)

    def restore_from_path(
        self,
        path: Union[str, pathlib.Path],
        *,
        component: Optional[str] = None,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        **kwargs,
    ) -> None:
        """Restores the state of the implementing class from the given path.

        If the `component` arg is provided, `path` refers to a checkpoint of a
        subcomponent of `self`, thus allowing the user to load only the subcomponent's
        state into `self` without affecting any of the other state information (for
        example, loading only the NN state into a Checkpointable, which contains such
        an NN, but also has other state information that should NOT be changed by
        calling this method).

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
                [cls.METADATA_FILE_NAME] (json)
                [cls.STATE_FILE_NAME] (pkl)

        Note that the self.METADATA_FILE_NAME file is not required to restore the state.

        Args:
            path: The path to load the implementing class' state from or to load the
                state of only one subcomponent's state of the implementing class (if
                `component` is provided).
            component: If provided, `path` is interpreted as the checkpoint path of only
                the subcomponent and thus, only that subcomponent's state is
                restored/loaded. All other state of `self` remains unchanged in this
                case.
            filesystem: PyArrow FileSystem to use to access data at the `path`. If not
                specified, this is inferred from the URI scheme of `path`.
            **kwargs: Forward compatibility kwargs.
        """
        path = path if isinstance(path, str) else path.as_posix()

        if path and not filesystem:
            # Note the path needs to be a path that is relative to the
            # filesystem (e.g. `gs://tmp/...` -> `tmp/...`).
            filesystem, path = pyarrow.fs.FileSystem.from_uri(path)
        # Only here convert to a `Path` instance b/c otherwise
        # cloud path gets broken (i.e. 'gs://' -> 'gs:/').
        path = pathlib.Path(path)

        if not _exists_at_fs_path(filesystem, path.as_posix()):
            raise FileNotFoundError(f"`path` ({path}) not found!")

        # Restore components of `self` that themselves are `Checkpointable`.
        for comp_name, comp in self.get_checkpointable_components():

            # The value of the `component` argument for the upcoming
            # `[subcomponent].restore_from_path(.., component=..)` call.
            comp_arg = None

            if component is None:
                comp_dir = path / comp_name
                # If subcomponent's dir is not in path, ignore it and don't restore this
                # subcomponent's state from disk.
                if not _exists_at_fs_path(filesystem, comp_dir.as_posix()):
                    continue
            else:
                comp_dir = path

                # `component` is a path that starts with `comp` -> Remove the name of
                # `comp` from the `component` arg in the upcoming call to `restore_..`.
                if component.startswith(comp_name + "/"):
                    comp_arg = component[len(comp_name) + 1 :]
                # `component` has nothing to do with `comp` -> Skip.
                elif component != comp_name:
                    continue

            # If component is an ActorManager, restore all the manager's healthy
            # actors' states from disk (even if they are on another node, in which case,
            # we'll sync checkpoint file(s) to the respective node).
            if isinstance(comp, FaultTolerantActorManager):
                head_node_ip = ray.util.get_node_ip_address()
                all_healthy_actors = comp.healthy_actor_ids()

                def _restore(
                    w,
                    _kwargs=MappingProxyType(kwargs),
                    _path=comp_dir,
                    _head_ip=head_node_ip,
                    _comp_arg=comp_arg,
                ):
                    import ray
                    import tempfile

                    worker_node_ip = ray.util.get_node_ip_address()
                    # If the worker is on the same node as the head, load the checkpoint
                    # directly from the path otherwise sync the checkpoint from the head
                    # to the worker and load it from there.
                    if worker_node_ip == _head_ip:
                        w.restore_from_path(_path, component=_comp_arg, **_kwargs)
                    else:
                        with tempfile.TemporaryDirectory() as temp_dir:
                            sync_dir_between_nodes(
                                _head_ip, _path, worker_node_ip, temp_dir
                            )
                            w.restore_from_path(
                                temp_dir, component=_comp_arg, **_kwargs
                            )

                comp.foreach_actor(_restore, remote_actor_ids=all_healthy_actors)

            # Call `restore_from_path()` on local subcomponent, thereby passing in the
            # **kwargs.
            else:
                comp.restore_from_path(
                    comp_dir, filesystem=filesystem, component=comp_arg, **kwargs
                )

        # Restore the rest of the state (not based on subcomponents).
        if component is None:
            with filesystem.open_input_stream(
                (path / self.STATE_FILE_NAME).as_posix()
            ) as f:
                state = pickle.load(f)
            self.set_state(state)

    @classmethod
    def from_checkpoint(
        cls,
        path: Union[str, pathlib.Path],
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        **kwargs,
    ) -> "Checkpointable":
        """Creates a new Checkpointable instance from the given location and returns it.

        Args:
            path: The checkpoint path to load (a) the information on how to construct
                a new instance of the implementing class and (b) the state to restore
                the created instance to.
            filesystem: PyArrow FileSystem to use to access data at the `path`. If not
                specified, this is inferred from the URI scheme of `path`.
            kwargs: Forward compatibility kwargs. Note that these kwargs are sent to
                each subcomponent's `from_checkpoint()` call.

        Returns:
             A new instance of the implementing class, already set to the state stored
             under `path`.
        """
        # We need a string path for the `PyArrow` filesystem.
        path = path if isinstance(path, str) else path.as_posix()

        # If no filesystem is passed in create one.
        if path and not filesystem:
            # Note the path needs to be a path that is relative to the
            # filesystem (e.g. `gs://tmp/...` -> `tmp/...`).
            filesystem, path = pyarrow.fs.FileSystem.from_uri(path)
        # Only here convert to a `Path` instance b/c otherwise
        # cloud path gets broken (i.e. 'gs://' -> 'gs:/').
        path = pathlib.Path(path)

        # Get the class constructor to call.
        with filesystem.open_input_stream(
            (path / cls.CLASS_AND_CTOR_ARGS_FILE_NAME).as_posix()
        ) as f:
            ctor_info = pickle.load(f)
        ctor = ctor_info["class"]

        # Check, whether the constructor actually goes together with `cls`.
        if not issubclass(ctor, cls):
            raise ValueError(
                f"The class ({ctor}) stored in checkpoint ({path}) does not seem to be "
                f"a subclass of `cls` ({cls})!"
            )
        elif not issubclass(ctor, Checkpointable):
            raise ValueError(
                f"The class ({ctor}) stored in checkpoint ({path}) does not seem to be "
                "an implementer of the `Checkpointable` API!"
            )

        obj = ctor(
            *ctor_info["ctor_args_and_kwargs"][0],
            **ctor_info["ctor_args_and_kwargs"][1],
        )
        # Restore the state of the constructed object.
        obj.restore_from_path(path, filesystem=filesystem, **kwargs)
        # Return the new object.
        return obj

    @abc.abstractmethod
    def get_state(
        self,
        components: Optional[Union[str, Collection[str]]] = None,
        *,
        not_components: Optional[Union[str, Collection[str]]] = None,
        **kwargs,
    ) -> StateDict:
        """Returns the implementing class's current state as a dict.

        Args:
            components: An optional collection of string keys to be included in the
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

    def _check_component(self, name, components, not_components) -> bool:
        comp_list = force_list(components)
        not_comp_list = force_list(not_components)
        if (
            components is None
            or any(c.startswith(name + "/") for c in comp_list)
            or name in comp_list
        ) and (not_components is None or name not in not_comp_list):
            return True
        return False

    def _get_subcomponents(self, name, components):
        if components is None:
            return None

        components = force_list(components)
        subcomponents = []
        for comp in components:
            if comp.startswith(name + "/"):
                subcomponents.append(comp[len(name) + 1 :])

        return None if not subcomponents else subcomponents


def _exists_at_fs_path(fs: pyarrow.fs.FileSystem, path: str) -> bool:
    """Returns `True` if the path can be found in the filesystem."""
    valid = fs.get_file_info(path)
    return valid.type != pyarrow.fs.FileType.NotFound


def _is_dir(file_info: pyarrow.fs.FileInfo) -> bool:
    """Returns `True`, if the file info is from a directory."""
    return file_info.type == pyarrow.fs.FileType.Directory


@PublicAPI(stability="alpha")
def get_checkpoint_info(
    checkpoint: Union[str, Checkpoint],
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
) -> Dict[str, Any]:
    """Returns a dict with information about an Algorithm/Policy checkpoint.

    If the given checkpoint is a >=v1.0 checkpoint directory, try reading all
    information from the contained `rllib_checkpoint.json` file.

    Args:
        checkpoint: The checkpoint directory (str) or an AIR Checkpoint object.
        filesystem: PyArrow FileSystem to use to access data at the `checkpoint`. If not
            specified, this is inferred from the URI scheme provided by `checkpoint`.

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
        "module_ids": None,
    }

    # `checkpoint` is a Checkpoint instance: Translate to directory and continue.
    if isinstance(checkpoint, Checkpoint):
        checkpoint = checkpoint.to_directory()

    if checkpoint and not filesystem:
        # Note the path needs to be a path that is relative to the
        # filesystem (e.g. `gs://tmp/...` -> `tmp/...`).
        filesystem, checkpoint = pyarrow.fs.FileSystem.from_uri(checkpoint)
    # Only here convert to a `Path` instance b/c otherwise
    # cloud path gets broken (i.e. 'gs://' -> 'gs:/').
    checkpoint = pathlib.Path(checkpoint)

    # Checkpoint is dir.
    if _exists_at_fs_path(filesystem, checkpoint.as_posix()) and _is_dir(
        filesystem.get_file_info(checkpoint.as_posix())
    ):
        info.update({"checkpoint_dir": str(checkpoint)})

        # Figure out whether this is an older checkpoint format
        # (with a `checkpoint-\d+` file in it).
        file_info_list = filesystem.get_file_info(
            pyarrow.fs.FileSelector(checkpoint.as_posix(), recursive=False)
        )
        for file_info in file_info_list:
            if file_info.is_file:
                if re.match("checkpoint-\\d+", file_info.base_name):
                    info.update(
                        {
                            "checkpoint_version": version.Version("0.1"),
                            "state_file": str(file_info.base_name),
                        }
                    )
                    return info

        # No old checkpoint file found.

        # If rllib_checkpoint.json file present, read available information from it
        # and then continue with the checkpoint analysis (possibly overriding further
        # information).
        if _exists_at_fs_path(
            filesystem, (checkpoint / "rllib_checkpoint.json").as_posix()
        ):
            # if (checkpoint / "rllib_checkpoint.json").is_file():
            with filesystem.open_input_stream(
                (checkpoint / "rllib_checkpoint.json").as_posix()
            ) as f:
                # with open(checkpoint / "rllib_checkpoint.json") as f:
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
            if _exists_at_fs_path(
                filesystem, (checkpoint / ("policy_state." + extension)).as_posix()
            ):
                # if (checkpoint / ("policy_state." + extension)).is_file():
                info.update(
                    {
                        "type": "Policy",
                        "format": "cloudpickle" if extension == "pkl" else "msgpack",
                        "checkpoint_version": CHECKPOINT_VERSION,
                        "state_file": str(checkpoint / f"policy_state.{extension}"),
                    }
                )
                return info

        # Valid Algorithm checkpoint >v0 file found?
        format = None
        for extension in ["pkl", "msgpck"]:
            state_file = checkpoint / f"algorithm_state.{extension}"
            if (
                _exists_at_fs_path(filesystem, state_file.as_posix())
                and filesystem.get_file_info(state_file.as_posix()).is_file
            ):
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
                "state_file": str(state_file),
            }
        )

        # Collect all policy IDs in the sub-dir "policies/".
        policies_dir = checkpoint / "policies"
        if _exists_at_fs_path(filesystem, policies_dir.as_posix()) and _is_dir(
            filesystem.get_file_info(policies_dir.as_posix())
        ):
            policy_ids = set()
            file_info_list = filesystem.get_file_info(
                pyarrow.fs.FileSelector(policies_dir.as_posix(), recursive=False)
            )
            for file_info in file_info_list:
                policy_ids.add(file_info.base_name)
            info.update({"policy_ids": policy_ids})

        # Collect all module IDs in the sub-dir "learner/module_state/".
        modules_dir = (
            checkpoint
            / COMPONENT_LEARNER_GROUP
            / COMPONENT_LEARNER
            / COMPONENT_RL_MODULE
        )
        if _exists_at_fs_path(filesystem, checkpoint.as_posix()) and _is_dir(
            filesystem.get_file_info(modules_dir.as_posix())
        ):
            module_ids = set()
            file_info_list = filesystem.get_file_info(
                pyarrow.fs.FileSelector(modules_dir.as_posix(), recursive=False)
            )
            for file_info in file_info_list:
                # Only add subdirs (those are the ones where the RLModule data
                # is stored, not files (could be json metadata files).
                module_dir = modules_dir / file_info.base_name
                if _is_dir(filesystem.get_file_info(module_dir.as_posix())):
                    module_ids.add(file_info.base_name)
            info.update({"module_ids": module_ids})

    # Checkpoint is a file: Use as-is (interpreting it as old Algorithm checkpoint
    # version).
    elif (
        _exists_at_fs_path(filesystem, checkpoint.as_posix())
        and filesystem.get_file_info(checkpoint.as_posix()).is_file
    ):
        info.update(
            {
                "checkpoint_version": version.Version("0.1"),
                "checkpoint_dir": str(checkpoint.parent),
                "state_file": str(checkpoint),
            }
        )

    else:
        raise ValueError(
            f"Given checkpoint ({str(checkpoint)}) not found! Must be a "
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
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
    from ray.rllib.core.rl_module import validate_module_id

    # Try to import msgpack and msgpack_numpy.
    msgpack = try_import_msgpack(error=True)

    # Restore the Algorithm using the python version dependent checkpoint.
    algo = Algorithm.from_checkpoint(checkpoint)
    state = algo.__getstate__()

    # Convert all code in state into serializable data.
    # Serialize the algorithm class.
    state["algorithm_class"] = serialize_type(state["algorithm_class"])
    # Serialize the algorithm's config object.
    if not isinstance(state["config"], dict):
        state["config"] = state["config"].serialize()
    else:
        state["config"] = AlgorithmConfig._serialize_dict(state["config"])

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
        validate_module_id(pid, error=True)
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
