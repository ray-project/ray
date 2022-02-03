import abc
import functools
import inspect
import re
import textwrap
from typing import List, Callable, Optional, Tuple, Dict, Any

import ray
from ray.data.dataset import Dataset
from ray.data.dataset_pipeline import (
    DatasetPipeline,
    PER_DATASET_OPS,
    HOLISTIC_PER_DATASET_OPS,
    PER_DATASET_OUTPUT_OPS,
    OUTPUT_ITER_OPS,
)
from ray.data.grouped_dataset import GroupedDataset


DATASET_PROXY_OPS = [
    "map",
    "map_batches",
    "flat_map",
    "filter",
    "repartition",
    "random_shuffle",
    "union",
    "sort",
    "zip",
    "limit",
    "force_reads",
]
TO_PIPELINE_OPS = ["repeat", "window"]
GROUPED_DATASET_PROXY_OPS = ["aggregate", "sum", "min", "max", "mean", "std"]
CONSUME_OPS = [
    "take",
    "take_all",
    "show",
    "iter_rows",
    "iter_batches",
    "count",
    "schema",
    "num_blocks",
    "size_bytes",
    "input_files",
    "get_internal_block_refs",
    "stats",
    "sum",
    "min",
    "max",
    "mean",
    "std",
    "aggregate",
    "write_parquet",
    "write_json",
    "write_csv",
    "write_numpy",
    "write_datasource",
    "to_torch",
    "to_tf",
    "to_dask",
    "to_mars",
    "to_modin",
    "to_spark",
    "to_pandas",
    "to_pandas_refs",
    "to_numpy_refs",
    "to_arrow_refs",
]

PIPELINE_PROXY_OPS = PER_DATASET_OPS + [
    f"{op}_each_window" for op in HOLISTIC_PER_DATASET_OPS
]
PIPELINE_CONSUME_OPS = PER_DATASET_OUTPUT_OPS + OUTPUT_ITER_OPS


class LazyComputable(abc.ABC):
    """
    A set of lazy operations that can be computed via calling .compute().
    """

    def __init__(
        self,
        func: Callable,
        args: Optional[Tuple] = None,
        kwargs: Optional[Dict[str, Any]] = None,
    ):
        """
        Builds a LazyComputable by holding a function application specification
        (func, args, kwargs) whose application would produce a new concrete result
        (Dataset/DatasetPipeline/etc.)

        Args:
            func: The function whose application will produce a new result.
            args: Positional arguments for the function.
            kwargs: Keyword arguments for the function.
        """
        self.func = func
        if args is None:
            args = tuple()
        self.args = args
        if kwargs is None:
            kwargs = {}
        self.kwargs = kwargs

    def cache(self) -> "LazyComputable":
        """
        Cache the computable, such that multiple references to the same computable won't
        result in redundant computation.

        Returns:
            A new LazyComputable whose computation is coordinated by a centralized
            actor.
        """
        coordinator = ComputeCoordinator.remote(self)
        return type(self)(func=lambda: ray.get(coordinator.get_or_compute.remote()))

    def compute(self):
        """
        Compute the Dataset, executing the underlying Dataset operations and
        returning the resulting Dataset.

        Returns:
            A Dataset.
        """
        args = [
            arg.compute() if isinstance(arg, LazyComputable) else arg
            for arg in self.args
        ]
        kwargs = {
            k: v.compute() if isinstance(v, LazyComputable) else v
            for k, v in self.kwargs
        }
        return self.func(*args, **kwargs)


class LazyDataset(LazyComputable):
    """
    A lazy version of Dataset, exposing the same API but executing the
    operations lazily.

    A LazyDataset consists of a function application specification for producing
    a new dataset.

    To materialize the result of all Dataset operations, call .compute().
    All functions that produce a non-Dataset/DatasetPipeline/GroupedDataset
    output will trigger computation, as will all consuming functions
    (e.g. .iter_batches(), .to_torch(), etc.)
    """

    def split(self, num_splits, *args, **kwargs) -> List["LazyDataset"]:
        parent = LazyDataset(
            func=Dataset.split, args=(self, num_splits) + args, kwargs=kwargs
        )
        coordinator = ComputeCoordinator.remote(parent)
        return [
            LazyDataset(
                func=lambda: ray.get(coordinator.get_or_compute.remote(split_idx))
            )
            for split_idx in range(num_splits)
        ]

    def split_at_indices(self, indices, *args, **kwargs) -> List["LazyDataset"]:
        parent = LazyDataset(
            func=Dataset.split_at_indices,
            args=(self, indices) + args,
            kwargs=kwargs,
        )
        coordinator = ComputeCoordinator.remote(parent)
        return [
            LazyDataset(
                func=lambda: ray.get(coordinator.get_or_compute.remote(split_idx))
            )
            for split_idx in range(len(indices) + 1)
        ]

    def groupby(self, *args, **kwargs) -> "LazyGroupedDataset":
        return LazyGroupedDataset(
            func=Dataset.groupby, args=(self,) + args, kwargs=kwargs
        )


LazyDataset.split.__doc__ = (
    """
Lazy version of ``Dataset.split``, returning a List[LazyDataset].
"""
    + Dataset.split.__doc__
)

LazyDataset.split_at_indices.__doc__ = (
    """
Lazy version of ``Dataset.split_at_indices``, returning a List[LazyDataset].
"""
    + Dataset.split_at_indices.__doc__
)

LazyDataset.groupby.__doc__ = (
    """
Lazy version of ``Dataset.groupby``, returning a LazyGroupedDataset.
"""
    + Dataset.groupby.__doc__
)


for method in DATASET_PROXY_OPS:

    def make_impl(method):
        delegate = getattr(Dataset, method)

        def impl(self, *args, **kwargs) -> "LazyDataset":
            return LazyDataset(func=delegate, args=(self,) + args, kwargs=kwargs)

        impl.__name__ = delegate.__name__
        lazy_preamble = """
        Lazy version of ``Dataset.{method}``, returning a LazyDataset.
        """.format(
            method=method
        )
        impl.__doc__ = lazy_preamble + delegate.__doc__
        setattr(
            impl,
            "__signature__",
            inspect.signature(delegate).replace(return_annotation="LazyDataset"),
        )
        return impl

    setattr(LazyDataset, method, make_impl(method))

for method in TO_PIPELINE_OPS:

    def make_impl(method):
        delegate = getattr(Dataset, method)

        def impl(self, *args, **kwargs) -> "LazyDatasetPipeline":
            return LazyDatasetPipeline(
                func=delegate, args=(self,) + args, kwargs=kwargs
            )

        impl.__name__ = delegate.__name__
        lazy_preamble = """
        Lazy version of ``Dataset.{method}``, returning a LazyDatasetPipeline.
        """.format(
            method=method
        )
        impl.__doc__ = lazy_preamble + delegate.__doc__
        setattr(
            impl,
            "__signature__",
            inspect.signature(delegate).replace(
                return_annotation="LazyDatasetPipeline"
            ),
        )
        return impl

    setattr(LazyDataset, method, make_impl(method))

for method in CONSUME_OPS:

    def make_impl(method):
        delegate = getattr(Dataset, method)

        def impl(self, *args, **kwargs):
            return getattr(self.compute(), method)(*args, **kwargs)

        impl.__name__ = delegate.__name__
        lazy_preamble = """
        NOTE: This will trigger computation and materialize the lazy Dataset!
        """
        impl.__doc__ = lazy_preamble + delegate.__doc__
        setattr(
            impl,
            "__signature__",
            inspect.signature(delegate),
        )
        return impl

    setattr(LazyDataset, method, make_impl(method))


@ray.remote(num_cpus=0, placement_group=None)
class ComputeCoordinator:
    """
    Actor that coordinates the computation of a dataset. This is used when a dataset
    is needed by multiple downstream operations/consumers, such as branching
    computations or splits.
    """

    def __init__(self, parent):
        self.parent = parent
        self.computed = None

    def get_or_compute(self, split_idx=None):
        if self.computed is None:
            self.computed = self.parent.compute()
        if split_idx is not None:
            assert isinstance(self.computed, list)
            return self.computed[split_idx]
        return self.computed


class LazyGroupedDataset(LazyComputable):
    """
    A lazy proxy for a GroupedDataset.
    """

    pass


for method in GROUPED_DATASET_PROXY_OPS:

    def make_impl(method):
        delegate = getattr(GroupedDataset, method)

        def impl(self, *args, **kwargs) -> "LazyDataset":
            return LazyDataset(func=delegate, args=(self,) + args, kwargs=kwargs)

        impl.__name__ = delegate.__name__
        lazy_preamble = """
        Lazy version of ``GroupedDataset.{method}``, returning a LazyDataset.
        """.format(
            method=method
        )
        impl.__doc__ = lazy_preamble + delegate.__doc__
        setattr(
            impl,
            "__signature__",
            inspect.signature(delegate).replace(return_annotation="LazyDataset"),
        )
        return impl

    setattr(LazyGroupedDataset, method, make_impl(method))


class LazyDatasetPipeline(LazyComputable):
    """
    A lazy version of DatasetPipeline, exposing the same API but executing the
    operations lazily.

    A LazyDatasetPipeline consists of a reference to a function application
    specification for producing a new DatasetPipeline.

    To materialize the result of all DatasetPipeline operations, call .compute().
    All functions that produce a non-Dataset/DatasetPipeline/GroupedDataset
    output will trigger computation, as will all consuming functions
    (e.g. .iter_batches(), .to_torch(), etc.)
    """

    def split(self, num_splits, *args, **kwargs) -> List["LazyDatasetPipeline"]:
        parent = LazyDatasetPipeline(
            func=DatasetPipeline.split,
            args=(self, num_splits) + args,
            kwargs=kwargs,
        )
        coordinator = ComputeCoordinator.remote(parent)
        return [
            LazyDatasetPipeline(
                func=lambda: ray.get(coordinator.get_or_compute.remote(split_idx))
            )
            for split_idx in range(num_splits)
        ]

    def split_at_indices(self, indices, *args, **kwargs) -> List["LazyDatasetPipeline"]:
        parent = LazyDatasetPipeline(
            func=DatasetPipeline.split_at_indices,
            args=(self, indices) + args,
            kwargs=kwargs,
        )
        coordinator = ComputeCoordinator.remote(parent)
        return [
            LazyDatasetPipeline(
                func=lambda: ray.get(coordinator.get_or_compute.remote(split_idx))
            )
            for split_idx in range(len(indices) + 1)
        ]


LazyDatasetPipeline.split.__doc__ = (
    """
Lazy version of ``DatasetPipeline.split``, returning a List[LazyDatasetPipeline].
"""
    + DatasetPipeline.split.__doc__
)

LazyDatasetPipeline.split_at_indices.__doc__ = (
    """
Lazy version of ``DatasetPipeline.split_at_indices``, returning a
List[LazyDatasetPipeline].
"""
    + DatasetPipeline.split_at_indices.__doc__
)


for method in PIPELINE_PROXY_OPS:

    def make_impl(method):
        delegate = getattr(DatasetPipeline, method)

        def impl(self, *args, **kwargs) -> "LazyDatasetPipeline":
            return LazyDatasetPipeline(
                func=delegate, args=(self,) + args, kwargs=kwargs
            )

        impl.__name__ = delegate.__name__
        lazy_preamble = """
        Lazy version of ``DatasetPipeline.{method}``, returning a LazyDatasetPipeline.
        """.format(
            method=method
        )
        impl.__doc__ = lazy_preamble + delegate.__doc__
        setattr(
            impl,
            "__signature__",
            inspect.signature(delegate).replace(
                return_annotation="LazyDatasetPipeline"
            ),
        )
        return impl

    setattr(LazyDatasetPipeline, method, make_impl(method))

for method in PIPELINE_CONSUME_OPS:

    def make_impl(method):
        delegate = getattr(DatasetPipeline, method)

        def impl(self, *args, **kwargs):
            return getattr(self.compute(), method)(*args, **kwargs)

        impl.__name__ = delegate.__name__
        lazy_preamble = """
        NOTE: This will trigger computation and materialize the lazy DatasetPipeline!
        """
        impl.__doc__ = lazy_preamble + delegate.__doc__
        setattr(
            impl,
            "__signature__",
            inspect.signature(delegate),
        )
        return impl

    setattr(LazyDatasetPipeline, method, make_impl(method))


def add_lazy_option(fn):
    """
    Add a lazy kwarg to a Dataset-generating function, instead returning a
    LazyDataset.
    """

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        if kwargs.pop("lazy", False):
            return LazyDataset(func=fn, args=args, kwargs=kwargs)
        return fn(*args, **kwargs)

    doc = fn.__doc__
    wrapper.__doc__ = _add_arg_to_docstring(doc)

    return wrapper


def _add_arg_to_docstring(doc):
    lines = doc.split("\n")
    i = 0
    while i < len(lines):
        if lines[i].strip().startswith("Args:"):
            # Found the start of the args section.
            break
        i += 1
    else:
        # No args section found.
        return doc
    i += 1
    # Get indentation.
    indent = re.match(r"\s*", lines[i]).group(0)
    while i < len(lines):
        if len(lines[i]) == 0:
            # Found the end of the args section (double-newline).
            break
        i += 1
    else:
        # No args section found.
        return doc
    lazy_arg_doc = (
        "lazy: Whether to create the dataset lazily. If True, a LazyDataset will be "
        "returned and the dataset won't be created and processed until .compute() is "
        "called or it is consumed. See the LazyDataset docs for more information."
    )
    wrapper = textwrap.TextWrapper(
        width=80, initial_indent=indent, subsequent_indent=indent + "    "
    )
    lines.insert(i, wrapper.fill(lazy_arg_doc))
    return "\n".join(lines)
