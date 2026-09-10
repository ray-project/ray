"""Expose rule classes in ray.data._internal.logical.rules."""

from .combine_downloads import CombineDownloads
from .combine_shuffles import CombineShuffles
from .common_subexpr_elimination import CommonSubExprElimination
from .configure_map_task_memory import (
    ConfigureMapTaskMemoryRule,
    ConfigureMapTaskMemoryUsingOutputSize,
)
from .derive_list_files_pushdown import DeriveListFilesPushdown
from .inherit_target_max_block_size import InheritTargetMaxBlockSizeRule
from .limit_pushdown import LimitPushdownRule
from .operator_fusion import FuseOperators, are_remote_args_compatible
from .predicate_pushdown import PredicatePushdown
from .projection_pushdown import ProjectionPushdown
from .pushdown_count_files import PushdownCountFiles
from .set_read_parallelism import (
    SetReadParallelismRule,
    compute_additional_split_factor,
)

__all__ = [
    "CombineDownloads",
    "CombineShuffles",
    "CommonSubExprElimination",
    "ConfigureMapTaskMemoryRule",
    "ConfigureMapTaskMemoryUsingOutputSize",
    "DeriveListFilesPushdown",
    "FuseOperators",
    "InheritTargetMaxBlockSizeRule",
    "LimitPushdownRule",
    "PredicatePushdown",
    "ProjectionPushdown",
    "PushdownCountFiles",
    "SetReadParallelismRule",
    "are_remote_args_compatible",
    "compute_additional_split_factor",
]
