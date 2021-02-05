import copy
import operator

from dask import core, config
from dask.core import istask
from dask.dataframe.core import _concat
from dask.dataframe.shuffle import SimpleShuffleLayer, shuffle_group
from dask.dataframe.optimize import optimize_read_parquet_getitem
from dask.optimization import cull, fuse
from dask.blockwise import optimize_blockwise, fuse_roots
from dask.highlevelgraph import HighLevelGraph
from dask.utils import ensure_dict

from .scheduler import MultipleReturnFunc, multiple_return_get


class MultipleReturnSimpleShuffleLayer(SimpleShuffleLayer):
    @classmethod
    def clone(cls, layer: SimpleShuffleLayer):
        # TODO(Clark): Probably don't need this since SimpleShuffleLayer
        # implements __copy__() and the shallow clone should be enough?
        return cls(
            name=layer.name,
            column=layer.column,
            npartitions=layer.npartitions,
            npartitions_input=layer.npartitions_input,
            ignore_index=layer.ignore_index,
            name_input=layer.name_input,
            meta_input=layer.meta_input,
            parts_out=layer.parts_out,
            annotations=layer.annotations,
        )

    def __repr__(self):
        return "MultipleReturnSimpleShuffleLayer<name='{}', npartitions={}>".format(
            self.name, self.npartitions
        )

    def __reduce__(self):
        attrs = [
            "name",
            "column",
            "npartitions",
            "npartitions_input",
            "ignore_index",
            "name_input",
            "meta_input",
            "parts_out",
            "annotations",
        ]
        return (MultipleReturnSimpleShuffleLayer, tuple(getattr(self, attr) for attr in attrs))

    def _cull(self, parts_out):
        return MultipleReturnSimpleShuffleLayer(
            self.name,
            self.column,
            self.npartitions,
            self.npartitions_input,
            self.ignore_index,
            self.name_input,
            self.meta_input,
            parts_out=parts_out,
        )

    def _construct_graph(self):
        """Construct graph for a simple shuffle operation."""

        shuffle_group_name = "group-" + self.name
        shuffle_split_name = "split-" + self.name

        dsk = {}
        n_parts_out = len(self.parts_out)
        for part_out in self.parts_out:
            # TODO(Clark): Find better pattern than in-scheduler concat.
            _concat_list = [
                (shuffle_split_name, part_out, part_in)
                for part_in in range(self.npartitions_input)
            ]
            dsk[(self.name, part_out)] = (
                _concat, _concat_list, self.ignore_index)
            for _, _part_out, _part_in in _concat_list:
                dsk[(shuffle_split_name, _part_out, _part_in)] = (
                    multiple_return_get,
                    (shuffle_group_name, _part_in),
                    _part_out,
                )
                if (shuffle_group_name, _part_in) not in dsk:
                    dsk[(shuffle_group_name, _part_in)] = (
                        MultipleReturnFunc(
                            shuffle_group,
                            n_parts_out,
                        ),
                        (self.name_input, _part_in),
                        self.column,
                        0,
                        self.npartitions,
                        self.npartitions,
                        self.ignore_index,
                        self.npartitions,
                    )

        return dsk


def rewrite_simple_shuffle_layer(dsk, keys):
    if not isinstance(dsk, HighLevelGraph):
        dsk = HighLevelGraph.from_collections(id(dsk), dsk, dependencies=())
    else:
        dsk = dsk.copy()

    layers = dsk.layers.copy()
    for key, layer in layers.items():
        if type(layer) is SimpleShuffleLayer:
            dsk.layers[key] = MultipleReturnSimpleShuffleLayer.clone(layer)
        else:
            layer = copy.copy(layer)
            for inner_key, inner_layer in layer.items():
                if type(inner_layer) is SimpleShuffleLayer:
                    dsk.layers[key].layers[inner_key] = MultipleReturnSimpleShuffleLayer.clone(inner_layer)
    return dsk


def dataframe_optimize(dsk, keys, **kwargs):
    if not isinstance(keys, (list, set)):
        keys = [keys]
    keys = list(core.flatten(keys))

    if not isinstance(dsk, HighLevelGraph):
        dsk = HighLevelGraph.from_collections(id(dsk), dsk, dependencies=())

    dsk = rewrite_simple_shuffle_layer(dsk, keys=keys)
    dsk = optimize_read_parquet_getitem(dsk, keys=keys)
    dsk = optimize_blockwise(dsk, keys=keys)
    dsk = fuse_roots(dsk, keys=keys)
    dsk = dsk.cull(set(keys))

    if not config.get("optimization.fuse.active"):
        return dsk

    dependencies = dsk.get_all_dependencies()
    dsk = ensure_dict(dsk)

    fuse_subgraphs = config.get("optimization.fuse.subgraphs")
    if fuse_subgraphs is None:
        fuse_subgraphs = True
    dsk, _ = fuse(
        dsk,
        keys,
        dependencies=dependencies,
        fuse_subgraphs=fuse_subgraphs,
    )
    dsk, _ = cull(dsk, keys)
    return dsk


# Stale approaches below.


def fuse_splits_into_multiple_return(dsk, keys):
    if not isinstance(dsk, HighLevelGraph):
        dsk = HighLevelGraph.from_collections(id(dsk), dsk, dependencies=())
    else:
        dsk = dsk.copy()
    dependencies = dsk.dependencies.copy()
    for k, v in dsk.items():
        if istask(v) and v[0] == shuffle_group:
            task_deps = dependencies[k]
            # Only rewrite shuffle group split if all downstream dependencies
            # are splits.
            if all(
                istask(dsk[dep]) and dsk[dep][0] == operator.getitem
                for dep in task_deps
            ):
                for dep in task_deps:
                    # Rewrite split
                    pass
