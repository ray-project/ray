from typing import Any, Dict, Iterable, Tuple

import ray
from ray.data.block import Block


def _iter_sliced_blocks(
    blocks: Iterable[Block], per_task_row_limit: int
) -> Iterable[Block]:
    """Iterate over blocks, accumulating rows up to the per-task row limit."""
    rows_read = 0
    for block in blocks:
        if rows_read >= per_task_row_limit:
            break

        from ray.data.block import BlockAccessor

        accessor = BlockAccessor.for_block(block)
        block_rows = accessor.num_rows()

        if rows_read + block_rows <= per_task_row_limit:
            yield block
            rows_read += block_rows
        else:
            # Slice the block to meet the limit exactly
            remaining_rows = per_task_row_limit - rows_read
            sliced_block = accessor.slice(0, remaining_rows, copy=True)
            yield sliced_block
            break


def _validate_head_node_resources_for_local_scheduling(
    ray_remote_args: Dict[str, Any],
    *,
    op_description: str,
    default_num_cpus: int = 1,
    default_num_gpus: int = 0,
    default_memory: int = 0,
) -> None:
    """Ensure the head node has enough resources before pinning work there.

    Local paths (``local://``) and other driver-local I/O schedule tasks on the
    head node via ``NodeAffinitySchedulingStrategy``. If the head node was
    intentionally started with zero logical resources (a common practice to
    avoid OOMs), those tasks become unschedulable. Detect this upfront and
    raise a clear error with remediation steps.
    """

    # Ray defaults to reserving 1 CPU per task when num_cpus isn't provided.
    num_cpus = ray_remote_args.get("num_cpus", default_num_cpus)
    num_gpus = ray_remote_args.get("num_gpus", default_num_gpus)
    memory = ray_remote_args.get("memory", default_memory)

    # Resource keys follow the Resources map of ray.nodes() (e.g., CPU, GPU, memory).
    required_resources: Dict[str, float] = {}
    required_resources["CPU"] = float(num_cpus)
    required_resources["GPU"] = float(num_gpus)
    required_resources["memory"] = float(memory)

    # Include any additional custom resources requested.
    custom_resources = ray_remote_args.get("resources", {})
    for name, amount in custom_resources.items():
        if amount is None:
            continue
        try:
            amount = float(amount)
        except (TypeError, ValueError) as err:
            raise ValueError(f"Invalid resource amount for '{name}': {amount}") from err
        required_resources[name] = amount

    head_node = next(
        (
            node
            for node in ray.nodes()
            if node.get("Alive")
            and "node:__internal_head__" in node.get("Resources", {})
        ),
        None,
    )
    if not head_node:
        # The head node metadata is unavailable (e.g., during shutdown). Fall back
        # to the default behavior and let Ray surface its own error.
        return

    # Build a map of required vs available resources on the head node.
    head_resources: Dict[str, float] = head_node.get("Resources", {})
    # Map: resource name -> (required, available).
    insufficient: Dict[str, Tuple[float, float]] = {}
    for name, req in required_resources.items():
        avail = head_resources.get(name, 0.0)
        if avail < req:
            insufficient[name] = (req, avail)

    # If nothing is below the required amount, we are good to proceed.
    if not insufficient:
        return

    details = "; ".join(
        f"{name} required {req:g} but head has {avail:g}"
        for name, (req, avail) in insufficient.items()
    )

    raise ValueError(
        f"{op_description} must run on the head node (e.g., for local:// paths), "
        f"but the head node doesn't have enough resources: {details}. "
        "Add resources to the head node, switch to a shared filesystem instead "
        "of local://, or set the resource requests on this operation to 0 "
        "(for example, num_cpus=0) so it can run without head resources."
    )
