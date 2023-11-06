import glob
import inspect
import logging
import os
import shutil
import types
from typing import Any, Callable, Dict, Optional, Type, Union, TYPE_CHECKING

import pandas as pd

import ray
import ray.cloudpickle as pickle
from ray.tune.execution.placement_groups import (
    PlacementGroupFactory,
    resource_dict_to_pg_factory,
)
from ray.air._internal.uri_utils import URI
from ray.air.config import ScalingConfig
from ray.tune.registry import _ParameterRegistry
from ray.tune.utils import _detect_checkpoint_function
from ray.util.annotations import DeveloperAPI, PublicAPI

if TYPE_CHECKING:
    from ray.tune.trainable import Trainable

logger = logging.getLogger(__name__)


_TUNE_METADATA_FILENAME = ".tune_metadata"


@DeveloperAPI
class TrainableUtil:
    @staticmethod
    def write_metadata(checkpoint_dir: str, metadata: Dict) -> None:
        with open(os.path.join(checkpoint_dir, _TUNE_METADATA_FILENAME), "wb") as f:
            pickle.dump(metadata, f)

    @staticmethod
    def load_metadata(checkpoint_dir: str) -> Dict:
        with open(os.path.join(checkpoint_dir, _TUNE_METADATA_FILENAME), "rb") as f:
            return pickle.load(f)

    @staticmethod
    def find_checkpoint_dir(checkpoint_path):
        """Returns the directory containing the checkpoint path.

        Raises:
            FileNotFoundError if the directory is not found.
        """
        if not os.path.exists(checkpoint_path):
            raise FileNotFoundError("Path does not exist", checkpoint_path)
        if os.path.isdir(checkpoint_path):
            checkpoint_dir = checkpoint_path
        else:
            checkpoint_dir = os.path.dirname(checkpoint_path)
        while checkpoint_dir != os.path.dirname(checkpoint_dir):
            if os.path.exists(os.path.join(checkpoint_dir, ".is_checkpoint")):
                break
            checkpoint_dir = os.path.dirname(checkpoint_dir)
        else:
            raise FileNotFoundError(
                "Checkpoint directory not found for {}".format(checkpoint_path)
            )
        return os.path.normpath(checkpoint_dir)

    @staticmethod
    def find_rel_checkpoint_dir(logdir, checkpoint_path):
        """Returns the (relative) directory name of the checkpoint.

        Note, the assumption here is `logdir` should be the prefix of
        `checkpoint_path`.
        For example, returns `checkpoint00000`.
        """
        assert checkpoint_path.startswith(logdir), (
            f"expecting `logdir` to be a prefix of `checkpoint_path`, got "
            f"{checkpoint_path} (not in {logdir})"
        )
        rel_path = os.path.relpath(checkpoint_path, logdir)
        tokens = rel_path.split(os.sep)
        return os.path.join(tokens[0])

    @staticmethod
    def _make_checkpoint_dir_name(index: Union[int, str]):
        """Get the name of the checkpoint directory suffix."""
        suffix = "checkpoint"
        if index is not None:
            suffix += f"_{index:06d}" if isinstance(index, int) else f"_{index}"
        return suffix

    @staticmethod
    def make_checkpoint_dir(
        checkpoint_dir: str, index: Union[int, str], override: bool = False
    ):
        """Creates a checkpoint directory within the provided path.

        Args:
            checkpoint_dir: Path to checkpoint directory.
            index: A subdirectory will be created
                at the checkpoint directory named 'checkpoint_{index}'.
            override: Deletes checkpoint_dir before creating
                a new one.
        """
        suffix = TrainableUtil._make_checkpoint_dir_name(index)
        checkpoint_dir = os.path.join(checkpoint_dir, suffix)

        if override and os.path.exists(checkpoint_dir):
            shutil.rmtree(checkpoint_dir)
        os.makedirs(checkpoint_dir, exist_ok=True)

        TrainableUtil.mark_as_checkpoint_dir(checkpoint_dir)

        return checkpoint_dir

    @staticmethod
    def mark_as_checkpoint_dir(checkpoint_dir: str):
        """Drop marker in directory to identify it as a checkpoint dir."""
        open(os.path.join(checkpoint_dir, ".is_checkpoint"), "a").close()

    @staticmethod
    def get_checkpoints_paths(logdir):
        """Finds the checkpoints within a specific folder.

        Returns a pandas DataFrame of training iterations and checkpoint
        paths within a specific folder.

        Raises:
            FileNotFoundError if the directory is not found.
        """
        marker_paths = glob.glob(
            os.path.join(glob.escape(logdir), "checkpoint_*/.is_checkpoint")
        )
        iter_chkpt_pairs = []
        for marker_path in marker_paths:
            chkpt_dir = os.path.dirname(marker_path)
            basename = os.path.basename(chkpt_dir)

            # Skip temporary checkpoints
            if basename.startswith("checkpoint_tmp"):
                continue

            metadata_file = glob.glob(
                os.path.join(glob.escape(chkpt_dir), f"*{_TUNE_METADATA_FILENAME}")
            )
            # glob.glob: filenames starting with a dot are special cases
            # that are not matched by '*' and '?' patterns.
            metadata_file += glob.glob(
                os.path.join(glob.escape(chkpt_dir), _TUNE_METADATA_FILENAME)
            )
            metadata_file = list(set(metadata_file))  # avoid duplication
            if len(metadata_file) == 0:
                logger.warning(
                    f"The checkpoint {basename} does not have a metadata file. "
                    f"This usually means that the training process was interrupted "
                    f"while the checkpoint was being written. The checkpoint will be "
                    f"excluded from analysis. Consider deleting the directory. "
                    f"Full path: {chkpt_dir}"
                )
                continue
            elif len(metadata_file) > 1:
                raise ValueError(
                    f"The checkpoint {basename} contains more than one metadata file. "
                    f"If this happened without manual intervention, please file an "
                    f"issue at https://github.com/ray-project/ray/issues. "
                    f"Full path: {chkpt_dir}"
                )

            metadata_file = metadata_file[0]

            try:
                with open(metadata_file, "rb") as f:
                    metadata = pickle.load(f)
            except Exception as e:
                logger.warning(f"Could not read metadata from checkpoint: {e}")
                metadata = {}

            chkpt_path = metadata_file[: -len(_TUNE_METADATA_FILENAME)]
            chkpt_iter = metadata.get("iteration", -1)
            iter_chkpt_pairs.append([chkpt_iter, chkpt_path])

        chkpt_df = pd.DataFrame(
            iter_chkpt_pairs, columns=["training_iteration", "chkpt_path"]
        )
        return chkpt_df

    @staticmethod
    def get_remote_storage_path(
        local_path: str, local_path_prefix: str, remote_path_prefix: str
    ) -> str:
        """Converts a ``local_path`` to be based off of
        ``remote_path_prefix`` instead of ``local_path_prefix``.

        ``local_path_prefix`` is assumed to be a prefix of ``local_path``.

        Example:

            >>> TrainableUtil.get_remote_storage_path("/a/b/c", "/a", "s3://bucket/")
            's3://bucket/b/c'
        """
        rel_local_path = os.path.relpath(local_path, local_path_prefix)
        uri = URI(remote_path_prefix)
        return str(uri / rel_local_path)


@PublicAPI(stability="beta")
def with_parameters(trainable: Union[Type["Trainable"], Callable], **kwargs):
    """Wrapper for trainables to pass arbitrary large data objects.

    This wrapper function will store all passed parameters in the Ray
    object store and retrieve them when calling the function. It can thus
    be used to pass arbitrary data, even datasets, to Tune trainables.

    This can also be used as an alternative to ``functools.partial`` to pass
    default arguments to trainables.

    When used with the function API, the trainable function is called with
    the passed parameters as keyword arguments. When used with the class API,
    the ``Trainable.setup()`` method is called with the respective kwargs.

    If the data already exists in the object store (are instances of
    ObjectRef), using ``tune.with_parameters()`` is not necessary. You can
    instead pass the object refs to the training function via the ``config``
    or use Python partials.

    Args:
        trainable: Trainable to wrap.
        **kwargs: parameters to store in object store.

    Function API example:

    .. code-block:: python

        from ray import train, tune

        def train_fn(config, data=None):
            for sample in data:
                loss = update_model(sample)
                train.report(loss=loss)

        data = HugeDataset(download=True)

        tuner = Tuner(
            tune.with_parameters(train_fn, data=data),
            # ...
        )
        tuner.fit()

    Class API example:

    .. code-block:: python

        from ray import tune

        class MyTrainable(tune.Trainable):
            def setup(self, config, data=None):
                self.data = data
                self.iter = iter(self.data)
                self.next_sample = next(self.iter)

            def step(self):
                loss = update_model(self.next_sample)
                try:
                    self.next_sample = next(self.iter)
                except StopIteration:
                    return {"loss": loss, done: True}
                return {"loss": loss}

        data = HugeDataset(download=True)

        tuner = Tuner(
            tune.with_parameters(MyTrainable, data=data),
            # ...
        )
    """
    from ray.tune.trainable import Trainable

    if not callable(trainable) or (
        inspect.isclass(trainable) and not issubclass(trainable, Trainable)
    ):
        raise ValueError(
            f"`tune.with_parameters() only works with function trainables "
            f"or classes that inherit from `tune.Trainable()`. Got type: "
            f"{type(trainable)}."
        )

    parameter_registry = _ParameterRegistry()
    ray._private.worker._post_init_hooks.append(parameter_registry.flush)

    # Objects are moved into the object store
    prefix = f"{str(trainable)}_"
    for k, v in kwargs.items():
        parameter_registry.put(prefix + k, v)

    trainable_name = getattr(trainable, "__name__", "tune_with_parameters")
    keys = set(kwargs.keys())

    if inspect.isclass(trainable):
        # Class trainable

        class _Inner(trainable):
            def setup(self, config):
                setup_kwargs = {}
                for k in keys:
                    setup_kwargs[k] = parameter_registry.get(prefix + k)
                super(_Inner, self).setup(config, **setup_kwargs)

        trainable_with_params = _Inner
    else:
        # Function trainable
        if _detect_checkpoint_function(trainable, partial=True):
            from ray.tune.trainable.function_trainable import (
                _CHECKPOINT_DIR_ARG_DEPRECATION_MSG,
            )

            raise DeprecationWarning(_CHECKPOINT_DIR_ARG_DEPRECATION_MSG)

        def inner(config):
            fn_kwargs = {}
            for k in keys:
                fn_kwargs[k] = parameter_registry.get(prefix + k)
            return trainable(config, **fn_kwargs)

        trainable_with_params = inner

        if hasattr(trainable, "__mixins__"):
            trainable_with_params.__mixins__ = trainable.__mixins__

        # If the trainable has been wrapped with `tune.with_resources`, we should
        # keep the `_resources` attribute around
        if hasattr(trainable, "_resources"):
            trainable_with_params._resources = trainable._resources

    trainable_with_params.__name__ = trainable_name
    return trainable_with_params


@PublicAPI(stability="beta")
def with_resources(
    trainable: Union[Type["Trainable"], Callable],
    resources: Union[
        Dict[str, float],
        PlacementGroupFactory,
        ScalingConfig,
        Callable[[dict], PlacementGroupFactory],
    ],
):
    """Wrapper for trainables to specify resource requests.

    This wrapper allows specification of resource requirements for a specific
    trainable. It will override potential existing resource requests (use
    with caution!).

    The main use case is to request resources for function trainables when used
    with the Tuner() API.

    Class trainables should usually just implement the ``default_resource_request()``
    method.

    Args:
        trainable: Trainable to wrap.
        resources: Resource dict, placement group factory, ``ScalingConfig``
            or callable that takes in a config dict and returns a placement
            group factory.

    Example:

    .. code-block:: python

        from ray import tune
        from ray.tune.tuner import Tuner

        def train_fn(config):
            return len(ray.get_gpu_ids())  # Returns 2

        tuner = Tuner(
            tune.with_resources(train_fn, resources={"gpu": 2}),
            # ...
        )
        results = tuner.fit()

    """
    from ray.tune.trainable import Trainable

    if not callable(trainable) or (
        inspect.isclass(trainable) and not issubclass(trainable, Trainable)
    ):
        raise ValueError(
            f"`tune.with_resources() only works with function trainables "
            f"or classes that inherit from `tune.Trainable()`. Got type: "
            f"{type(trainable)}."
        )

    if isinstance(resources, PlacementGroupFactory):
        pgf = resources
    elif isinstance(resources, ScalingConfig):
        pgf = resources.as_placement_group_factory()
    elif isinstance(resources, dict):
        pgf = resource_dict_to_pg_factory(resources)
    elif callable(resources):
        pgf = resources
    else:
        raise ValueError(
            f"Invalid resource type for `with_resources()`: {type(resources)}"
        )

    if not inspect.isclass(trainable):
        if isinstance(trainable, types.MethodType):
            # Methods cannot set arbitrary attributes, so we have to wrap them
            if _detect_checkpoint_function(trainable, partial=True):
                from ray.tune.trainable.function_trainable import (
                    _CHECKPOINT_DIR_ARG_DEPRECATION_MSG,
                )

                raise DeprecationWarning(_CHECKPOINT_DIR_ARG_DEPRECATION_MSG)

            def _trainable(config):
                return trainable(config)

            _trainable._resources = pgf
            return _trainable

        # Just set an attribute. This will be resolved later in `wrap_function()`.
        try:
            trainable._resources = pgf
        except AttributeError as e:
            raise RuntimeError(
                "Could not use `tune.with_resources()` on the supplied trainable. "
                "Wrap your trainable in a regular function before passing it "
                "to Ray Tune."
            ) from e
    else:

        class ResourceTrainable(trainable):
            @classmethod
            def default_resource_request(
                cls, config: Dict[str, Any]
            ) -> Optional[PlacementGroupFactory]:
                if not isinstance(pgf, PlacementGroupFactory) and callable(pgf):
                    return pgf(config)
                return pgf

        ResourceTrainable.__name__ = trainable.__name__
        trainable = ResourceTrainable

    return trainable
