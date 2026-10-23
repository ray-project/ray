from typing import List, Sequence

from ray.data.preprocessor import Preprocessor


def _fit_levels(preprocessors: Sequence[Preprocessor]) -> List[int]:
    """Layered topological sort of a chain's column-dependency graph.

    Returns each member's level. A member's statistics are computed in scan
    number ``level``, over the fit dataset with every member it depends on,
    and every member before those, already applied; members that share a level
    share one scan, so a deferred fit costs ``max(levels) + 1`` dataset scans.

    A member depends on an earlier member if and only if it reads a column
    the earlier one writes (a member that doesn't declare both its input and
    output columns may read and write any column, so it depends on every
    earlier member and every later member depends on it)::

        chain:   scaler ----------->  imputer ---------->  encoder
                 reads/writes A,B     reads/writes B       reads/writes C
                    ^                    |
                    '------ reads B -----'
        level:      0                    1                    0

        scan 0 (raw dataset):     scaler and encoder statistics,
                                  then the scaler transforms
        scan 1 (scaled dataset):  imputer statistics, then the
                                  imputer and encoder transform

    One twist closes the graph over chain order: transforms are applied
    strictly in chain order, so waiting for member ``j``'s transform means
    waiting for every member before ``j`` as well::

        chain:   a --------> b --------> c --------> d
                 writes x    reads x     writes y    reads y
                 ^           |           ^           |
                 '--reads x--'           '--reads y--'
        level:   0           1           0           2   (not 1!)

        d reads only what c writes, but c's transform cannot be applied
        until the chain reaches it - until b (level 1) has fitted - so d
        aggregates one scan later. ``prefix_levels[j]``, the running
        maximum ``max(levels[:j + 1])``, is the level after which members
        ``0..j`` have all transformed; a dependency on ``j`` schedules a
        member at ``prefix_levels[j] + 1``.
    """
    declares = [_declares_columns(p) for p in preprocessors]
    reads = [set(p.get_input_columns()) for p in preprocessors]
    writes = [set(p.get_output_columns()) for p in preprocessors]

    levels: List[int] = []
    prefix_levels: List[int] = []
    for i in range(len(preprocessors)):
        level = 0
        for j in range(i):
            if not declares[i] or not declares[j] or reads[i] & writes[j]:
                level = max(level, prefix_levels[j] + 1)
        levels.append(level)
        prefix_levels.append(max(level, prefix_levels[i - 1]) if i else level)
    return levels


def _fit_order(preprocessors: Sequence[Preprocessor]) -> List[int]:
    """Order in which to fit a chain's members: a permutation of
    ``range(len(preprocessors))`` that lets independent members share the
    earliest scan possible, while preserving the relative order of every
    conflicting pair.

    Two members conflict when they touch (read or write) any common column,
    or when either doesn't declare its columns. Reordering a conflicting pair
    could change what one of them computes or produces - note that even two
    readers of the same column conflict, because a transform may consume the
    columns it reads (``Concatenator`` drops its input columns). Members that
    don't conflict touch disjoint columns, so their transforms commute and
    they may be fitted in either order.

    Sorting members by their level in this conflict graph (a stable sort, so
    ties keep chain order) places every member directly behind the members it
    conflicts with, removing the wait on unrelated slower members that chain
    order would otherwise impose::

        chain:                    scaler(x)  imputer(x)  scaler(y)  imputer(y)
        levels in chain order:    [0,        1,          0,         2]
        fit order:                [0,        2,          1,         3]
        levels in fit order:      [0,        0,          1,         1]

    In chain order, imputer(y) needs three scans: it must aggregate over
    scaler(y)-transformed data, and scaler(y)'s transform sits behind
    imputer(x) (fitted in scan 1) in the chain. Fitted in the reordered
    sequence, both scalers share scan 0 and both imputers share scan 1.
    """
    declares = [_declares_columns(p) for p in preprocessors]
    touched = [
        set(p.get_input_columns()) | set(p.get_output_columns()) for p in preprocessors
    ]

    def conflicts(i: int, j: int) -> bool:
        return not declares[i] or not declares[j] or bool(touched[i] & touched[j])

    conflict_levels: List[int] = []
    for i in range(len(preprocessors)):
        level = 0
        for j in range(i):
            if conflicts(i, j):
                level = max(level, conflict_levels[j] + 1)
        conflict_levels.append(level)
    return sorted(range(len(preprocessors)), key=conflict_levels.__getitem__)


def _declares_columns(preprocessor: Preprocessor) -> bool:
    """Whether a preprocessor declares both the columns it reads and writes.

    Declaring only one side isn't enough: a user-defined member that sets
    ``columns`` and rewrites them in place declares no output columns, yet
    writes the columns it reads. A Chain declares its columns only if every
    member does.
    """
    from ray.data.preprocessors.chain import Chain

    if isinstance(preprocessor, Chain):
        return all(_declares_columns(p) for p in preprocessor.preprocessors)
    return bool(preprocessor.get_input_columns()) and bool(
        preprocessor.get_output_columns()
    )
