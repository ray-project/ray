from typing import Dict, Any

import glob
import inspect
import io
import logging
import shutil

import pandas as pd
import ray.cloudpickle as pickle
import os

import ray
from ray.tune.registry import parameter_registry
from ray.tune.utils import detect_checkpoint_function
from ray.util import placement_group
from six import string_types

logger = logging.getLogger(__name__)


class TrainableUtil:
    @staticmethod
    def process_checkpoint(checkpoint, parent_dir, trainable_state):
        saved_as_dict = False
        if isinstance(checkpoint, string_types):
            if not checkpoint.startswith(parent_dir):
                raise ValueError(
                    "The returned checkpoint path must be within the "
                    "given checkpoint dir {}: {}".format(
                        parent_dir, checkpoint))
            checkpoint_path = checkpoint
            if os.path.isdir(checkpoint_path):
                # Add trailing slash to prevent tune metadata from
                # being written outside the directory.
                checkpoint_path = os.path.join(checkpoint_path, "")
        elif isinstance(checkpoint, dict):
            saved_as_dict = True
            checkpoint_path = os.path.join(parent_dir, "checkpoint")
            with open(checkpoint_path, "wb") as f:
                pickle.dump(checkpoint, f)
        else:
            raise ValueError("Returned unexpected type {}. "
                             "Expected str or dict.".format(type(checkpoint)))

        with open(checkpoint_path + ".tune_metadata", "wb") as f:
            trainable_state["saved_as_dict"] = saved_as_dict
            pickle.dump(trainable_state, f)
        return checkpoint_path

    @staticmethod
    def pickle_checkpoint(checkpoint_path):
        """Pickles checkpoint data."""
        checkpoint_dir = TrainableUtil.find_checkpoint_dir(checkpoint_path)
        data = {}
        for basedir, _, file_names in os.walk(checkpoint_dir):
            for file_name in file_names:
                path = os.path.join(basedir, file_name)
                with open(path, "rb") as f:
                    data[os.path.relpath(path, checkpoint_dir)] = f.read()
        # Use normpath so that a directory path isn't mapped to empty string.
        name = os.path.relpath(
            os.path.normpath(checkpoint_path), checkpoint_dir)
        name += os.path.sep if os.path.isdir(checkpoint_path) else ""
        data_dict = pickle.dumps({
            "checkpoint_name": name,
            "data": data,
        })
        return data_dict

    @staticmethod
    def checkpoint_to_object(checkpoint_path):
        data_dict = TrainableUtil.pickle_checkpoint(checkpoint_path)
        out = io.BytesIO()
        if len(data_dict) > 10e6:  # getting pretty large
            logger.info("Checkpoint size is {} bytes".format(len(data_dict)))
        out.write(data_dict)
        return out.getvalue()

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
                "Checkpoint directory not found for {}".format(
                    checkpoint_path))
        return os.path.normpath(checkpoint_dir)

    @staticmethod
    def make_checkpoint_dir(checkpoint_dir, index, override=False):
        """Creates a checkpoint directory within the provided path.

        Args:
            checkpoint_dir (str): Path to checkpoint directory.
            index (int|str): A subdirectory will be created
                at the checkpoint directory named 'checkpoint_{index}'.
            override (bool): Deletes checkpoint_dir before creating
                a new one.
        """
        suffix = "checkpoint"
        if index is not None:
            suffix += f"_{index:06d}" if isinstance(index,
                                                    int) else f"_{index}"
        checkpoint_dir = os.path.join(checkpoint_dir, suffix)

        if override and os.path.exists(checkpoint_dir):
            shutil.rmtree(checkpoint_dir)
        os.makedirs(checkpoint_dir, exist_ok=True)
        # Drop marker in directory to identify it as a checkpoint dir.
        open(os.path.join(checkpoint_dir, ".is_checkpoint"), "a").close()
        return checkpoint_dir

    @staticmethod
    def create_from_pickle(obj, tmpdir):
        info = pickle.loads(obj)
        data = info["data"]
        checkpoint_path = os.path.join(tmpdir, info["checkpoint_name"])

        for relpath_name, file_contents in data.items():
            path = os.path.join(tmpdir, relpath_name)

            # This may be a subdirectory, hence not just using tmpdir
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(file_contents)
        return checkpoint_path

    @staticmethod
    def get_checkpoints_paths(logdir):
        """ Finds the checkpoints within a specific folder.

        Returns a pandas DataFrame of training iterations and checkpoint
        paths within a specific folder.

        Raises:
            FileNotFoundError if the directory is not found.
        """
        marker_paths = glob.glob(
            os.path.join(glob.escape(logdir), "checkpoint_*/.is_checkpoint"))
        iter_chkpt_pairs = []
        for marker_path in marker_paths:
            chkpt_dir = os.path.dirname(marker_path)
            metadata_file = glob.glob(
                os.path.join(glob.escape(chkpt_dir), "*.tune_metadata"))
            if len(metadata_file) != 1:
                raise ValueError(
                    "{} has zero or more than one tune_metadata.".format(
                        chkpt_dir))

            chkpt_path = metadata_file[0][:-len(".tune_metadata")]
            chkpt_iter = int(chkpt_dir[chkpt_dir.rfind("_") + 1:])
            iter_chkpt_pairs.append([chkpt_iter, chkpt_path])

        chkpt_df = pd.DataFrame(
            iter_chkpt_pairs, columns=["training_iteration", "chkpt_path"])
        return chkpt_df


class PlacementGroupUtil:
    @staticmethod
    def get_remote_worker_options(
            num_workers, num_cpus_per_worker, num_gpus_per_worker,
            num_workers_per_host,
            timeout_s) -> (Dict[str, Any], placement_group):
        """ Returns the option for remote workers.

        Args:
            num_workers (int): Number of training workers to include in
                world.
            num_cpus_per_worker (int): Number of CPU resources to reserve
                per training worker.
            num_gpus_per_worker (int): Number of GPU resources to reserve
                per training worker.
            num_workers_per_host: Optional[int]: Number of workers to
                colocate per host.
            timeout_s (Optional[int]): Seconds before the torch process group
                times out. Useful when machines are unreliable. Defaults
                to 60 seconds. This value is also reused for triggering
                placement timeouts if forcing colocation.


        Returns:
            type(Dict[Str, Any]): option that contains CPU/GPU count of
                the remote worker and the placement group information.
            pg(placement_group): return a reference to the placement group
        """
        pg = None
        options = dict(
            num_cpus=num_cpus_per_worker, num_gpus=num_gpus_per_worker)
        if num_workers_per_host:
            num_hosts = int(num_workers / num_workers_per_host)
            cpus_per_node = num_cpus_per_worker * num_workers_per_host
            gpus_per_node = \
                num_gpus_per_worker * num_workers_per_host
            bundle = {"CPU": cpus_per_node, "GPU": gpus_per_node}

            all_bundles = [bundle] * num_hosts
            pg = placement_group(all_bundles, strategy="STRICT_SPREAD")
            logger.debug("Waiting for placement_group to start.")
            ray.get(pg.ready(), timeout=timeout_s)
            logger.debug("Placement_group started.")
            options["placement_group"] = pg

        return options, pg


def with_parameters(trainable, **kwargs):
    """Wrapper for trainables to pass arbitrary large data objects.

    This wrapper function will store all passed parameters in the Ray
    object store and retrieve them when calling the function. It can thus
    be used to pass arbitrary data, even datasets, to Tune trainables.

    This can also be used as an alternative to ``functools.partial`` to pass
    default arguments to trainables.

    When used with the function API, the trainable function is called with
    the passed parameters as keyword arguments. When used with the class API,
    the ``Trainable.setup()`` method is called with the respective kwargs.

    Args:
        trainable: Trainable to wrap.
        **kwargs: parameters to store in object store.

    Function API example:

    .. code-block:: python

        from ray import tune

        def train(config, data=None):
            for sample in data:
                loss = update_model(sample)
                tune.report(loss=loss)

        data = HugeDataset(download=True)

        tune.run(
            tune.with_parameters(train, data=data),
            # ...
        )

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

        tune.run(
            tune.with_parameters(MyTrainable, data=data),
            # ...
        )

    """
    from ray.tune.trainable import Trainable

    if not callable(trainable) or (inspect.isclass(trainable)
                                   and not issubclass(trainable, Trainable)):
        raise ValueError(
            f"`tune.with_parameters() only works with function trainables "
            f"or classes that inherit from `tune.Trainable()`. Got type: "
            f"{type(trainable)}.")

    # Objects are moved into the object store
    prefix = f"{str(trainable)}_"
    for k, v in kwargs.items():
        parameter_registry.put(prefix + k, v)

    trainable_name = getattr(trainable, "__name__", "tune_with_parameters")

    if inspect.isclass(trainable):
        # Class trainable
        keys = list(kwargs.keys())

        class _Inner(trainable):
            def setup(self, config):
                setup_kwargs = {}
                for k in keys:
                    setup_kwargs[k] = parameter_registry.get(prefix + k)
                super(_Inner, self).setup(config, **setup_kwargs)

        _Inner.__name__ = trainable_name
        return _Inner
    else:
        # Function trainable
        use_checkpoint = detect_checkpoint_function(trainable, partial=True)
        keys = list(kwargs.keys())

        def inner(config, checkpoint_dir=None):
            fn_kwargs = {}
            if use_checkpoint:
                default = checkpoint_dir
                sig = inspect.signature(trainable)
                if "checkpoint_dir" in sig.parameters:
                    default = sig.parameters["checkpoint_dir"].default \
                              or default
                fn_kwargs["checkpoint_dir"] = default

            for k in keys:
                fn_kwargs[k] = parameter_registry.get(prefix + k)
            trainable(config, **fn_kwargs)

        inner.__name__ = trainable_name

        # Use correct function signature if no `checkpoint_dir` parameter
        # is set
        if not use_checkpoint:

            def _inner(config):
                inner(config, checkpoint_dir=None)

            _inner.__name__ = trainable_name

            if hasattr(trainable, "__mixins__"):
                _inner.__mixins__ = trainable.__mixins__
            return _inner

        if hasattr(trainable, "__mixins__"):
            inner.__mixins__ = trainable.__mixins__

        return inner
