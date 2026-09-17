"""Compatibility shims for the pinned vLLM and Cutlass versions."""

from __future__ import annotations


def _install_vllm_cutlass_compatibility() -> None:
    """Restore Cutlass aliases and safe TCPStore port selection."""
    try:
        import cutlass.cute
        import cutlass.cute.core as core
    except ModuleNotFoundError:
        # The AIPerf client environment does not install serving dependencies.
        return

    for name in ("ThrMma", "ThrCopy"):
        value = getattr(cutlass.cute, name, None)
        if isinstance(value, type) and not hasattr(core, name):
            setattr(core, name, value)

    try:
        from vllm.v1.executor.ray_executor_v2 import RayExecutorV2, get_open_port
    except ModuleNotFoundError:
        return

    select_tcpstore_port = RayExecutorV2._select_tcpstore_port

    def select_tcpstore_port_with_zero_port(local_dp_rank: int | None, master_port: int) -> int:
        if local_dp_rank is None or master_port <= 0:
            return get_open_port()
        return select_tcpstore_port(local_dp_rank, master_port)

    RayExecutorV2._select_tcpstore_port = staticmethod(select_tcpstore_port_with_zero_port)


_install_vllm_cutlass_compatibility()
