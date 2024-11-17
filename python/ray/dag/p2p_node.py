from typing import Any, Dict, List, Union, Tuple

from ray.dag import (
    DAGNode,
    ClassMethodNode,
)
from ray.dag.sync_group import _SynchronousGroup
from ray.dag.constants import P2P_GROUP_KEY
from ray.util.annotations import DeveloperAPI


class _P2PGroup(_SynchronousGroup):
    """
    Represent a group of actors in a NCCL P2P operation.
    """

    def __init__(self):
        super().__init__()

    def execute(self, arg):
        return arg


class _NcclP2PNode(ClassMethodNode):
    """Represents a NCCL P2P operation in a Ray DAG."""

    def __init__(
        self,
        method_name: str,
        method_args: Tuple[DAGNode],
        method_kwargs: Dict[str, Any],
        method_options: Dict[str, Any],
        other_args_to_resolve: Dict[str, Any],
    ):
        super().__init__(
            method_name,
            method_args,
            method_kwargs,
            method_options,
            other_args_to_resolve,
        )

        # Parse the P2P operation.
        self._p2p_group: _P2PGroup = other_args_to_resolve.get(P2P_GROUP_KEY, None)
        if self._p2p_group is None:
            raise ValueError("Expected a P2P group")

    @property
    def sync_group(self) -> _P2PGroup:
        return self._p2p_group

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ):
        raise NotImplementedError("Cannot copy an abstract NCCL P2P node")

    def _execute_impl(self, *args, **kwargs):
        raise NotImplementedError(
            "NCCL P2P nodes are only supported with dag.experimental_compile()"
        )


@DeveloperAPI
class _NcclSendNode(_NcclP2PNode):
    """Represents a NCCL P2P send operation in a Ray DAG."""

    def __init__(
        self,
        method_args: Tuple[ClassMethodNode],
        other_args_to_resolve: Dict[str, Any],
    ):
        super().__init__(
            method_name="nccl_send",
            method_args=method_args,
            method_kwargs=dict(),
            method_options=dict(),
            other_args_to_resolve=other_args_to_resolve,
        )

        # Parse the input node.
        if not (
            isinstance(method_args, tuple)
            and len(method_args) == 1
            and isinstance(method_args[0], ClassMethodNode)
        ):
            raise ValueError("Expected a single input node that is a ClassMethodNode")
        elif isinstance(method_args[0], _NcclP2PNode):
            raise ValueError("NCCL send node cannot bind to another NCCL P2P node")
        self.set_requires_nccl_write(True)

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ):
        return _NcclSendNode(
            self._method_name,
            new_args,
            new_kwargs,
            new_options,
            other_args_to_resolve=new_other_args_to_resolve,
        )


@DeveloperAPI
class _NcclRecvNode(_NcclP2PNode):
    """Represents a NCCL P2P recv operation in a Ray DAG."""

    def __init__(
        self,
        method_args: Tuple[_NcclSendNode],
        other_args_to_resolve: Dict[str, Any],
    ):
        super().__init__(
            method_name="nccl_recv",
            method_args=method_args,
            method_kwargs=dict(),
            method_options=dict(),
            other_args_to_resolve=other_args_to_resolve,
        )

        # Parse the input node.
        if not (
            isinstance(method_args, tuple)
            and len(method_args) == 1
            and isinstance(method_args[0], _NcclSendNode)
        ):
            raise ValueError("Expected a single input node that is a _NcclSendNode")
        self.set_requires_nccl_read(True)

    def _copy_impl(
        self,
        new_args: List[Any],
        new_kwargs: Dict[str, Any],
        new_options: Dict[str, Any],
        new_other_args_to_resolve: Dict[str, Any],
    ):
        return _NcclRecvNode(
            self._method_name,
            new_args,
            new_kwargs,
            new_options,
            other_args_to_resolve=new_other_args_to_resolve,
        )
