from collections import OrderedDict
from collections.abc import Iterator
from operator import getitem
import uuid

import ray

from dask.core import quote
from dask.core import get as get_sync
from dask.utils import apply

try:
    from dataclasses import is_dataclass, fields as dataclass_fields
except ImportError:
    # Python < 3.7
    def is_dataclass(x):
        return False

    def dataclass_fields(x):
        return []


def unpack_object_refs(*args):
    """
    Extract Ray object refs from a set of potentially arbitrarily nested
    Python objects.

    Intended use is to find all Ray object references in a set of (possibly
    nested) Python objects, do something to them (get(), wait(), etc.), then
    repackage them into equivalent Python objects.

    Args:
        *args: One or more (potentially nested) Python objects that contain
            Ray object references.

    Returns:
        A 2-tuple of a flat list of all contained Ray object references, and a
        function that, when given the corresponding flat list of concrete
        values, will return a set of Python objects equivalent to that which
        was given in *args, but with all Ray object references replaced with
        their corresponding concrete values.
    """
    object_refs = []
    repack_dsk = {}

    object_refs_token = uuid.uuid4().hex

    def _unpack(expr):
        if isinstance(expr, ray.ObjectRef):
            token = expr.hex()
            repack_dsk[token] = (getitem, object_refs_token, len(object_refs))
            object_refs.append(expr)
            return token

        token = uuid.uuid4().hex
        # Treat iterators like lists
        typ = list if isinstance(expr, Iterator) else type(expr)
        if typ in (list, tuple, set):
            repack_task = (typ, [_unpack(i) for i in expr])
        elif typ in (dict, OrderedDict):
            repack_task = (typ, [[_unpack(k), _unpack(v)] for k, v in expr.items()])
        elif is_dataclass(expr):
            repack_task = (
                apply,
                typ,
                (),
                (
                    dict,
                    [
                        [f.name, _unpack(getattr(expr, f.name))]
                        for f in dataclass_fields(expr)
                    ],
                ),
            )
        else:
            return expr
        repack_dsk[token] = repack_task
        return token

    out = uuid.uuid4().hex
    repack_dsk[out] = (tuple, [_unpack(i) for i in args])

    def repack(results):
        dsk = repack_dsk.copy()
        dsk[object_refs_token] = quote(results)
        return get_sync(dsk, out)

    return object_refs, repack
