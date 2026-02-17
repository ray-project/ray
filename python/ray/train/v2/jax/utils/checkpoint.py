from typing import Any, Dict, Optional

import ray.train
from ray.train import Checkpoint


def save_checkpoint(
    item: Any,
    checkpoint_dir: str,
    metrics: Optional[Dict[str, Any]] = None,
    force: bool = False,
) -> None:
    """
    Saves a checkpoint using Orbax PyTreeCheckpointer and reports it to Ray Train.

    Args:
        item: The PyTree to save.
        checkpoint_dir: Path to save the checkpoint to.
        metrics: Optional dictionary of metrics to report to Ray Train along with the checkpoint.
        force: if True, allows overwriting an existing directory. May add overhead due to the need to delete any existing files.
    """
    import orbax.checkpoint as ocp

    checkpointer = ocp.PyTreeCheckpointer()
    checkpointer.save(checkpoint_dir, item, force=force)

    # Report to Ray Train
    ray.train.report(
        metrics or {},
        checkpoint=Checkpoint.from_directory(checkpoint_dir),
    )


def restore_checkpoint(
    target: Any,
) -> Optional[Any]:
    """
    Restores the latest checkpoint reported to Ray Train using Orbax PyTreeCheckpointer.

    Args:
        target: The target PyTree structure (with sharding info) to restore into.
                Restoration will enforce the sharding found in this target.

    Returns:
        The restored PyTree, or None if no checkpoint exists.
    """
    checkpoint = ray.train.get_checkpoint()
    if not checkpoint:
        return None

    with checkpoint.as_directory() as checkpoint_dir:
        import jax
        import orbax.checkpoint as ocp
        from orbax.checkpoint import type_handlers

        # Infer restore_args from target to enforce sharding
        restore_args = jax.tree_util.tree_map(
            lambda x: type_handlers.ArrayRestoreArgs(
                mesh=x.sharding.mesh, sharding=x.sharding
            )
            if isinstance(x, (jax.Array, jax.ShapeDtypeStruct))
            and hasattr(x, "sharding")
            else ocp.checkpoint_utils.construct_restore_args(x),
            target,
            is_leaf=lambda x: isinstance(x, (jax.Array, jax.ShapeDtypeStruct)),
        )
        checkpointer = ocp.PyTreeCheckpointer()
        restored = checkpointer.restore(
            checkpoint_dir, item=target, restore_args=restore_args
        )
        return restored
