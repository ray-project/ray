from typing import Dict, Optional

import ray
from ray._private.label_utils import (
    validate_node_label_syntax,
    validate_node_labels,
)


def update_node_labels(
    node_id: str,
    labels: Dict[str, str],
    timeout: Optional[float] = None,
) -> None:
    """Replace a node's user-defined labels at runtime (replace semantics).

    The provided ``labels`` replace the node's existing user-defined labels.
    Reserved ``ray.io/`` labels (managed by Ray, e.g. ``ray.io/node-id``) are
    always preserved. The update is forwarded to the target node's raylet and,
    once applied, propagates to the rest of the cluster so that
    label-selector-based scheduling of *future* tasks and actors observes the new
    labels. Tasks and actors that are already running or already pending are not
    re-evaluated; the new labels apply to placement decisions made after the
    update. Dynamic labels are not persisted: a raylet restart reverts the node to
    its startup labels, and after a GCS restart the node re-registers with its
    startup labels (so ``ray.nodes()`` may transiently show the startup set until
    the next update).

    This is an experimental API and may change in future releases.

    Args:
        node_id: Hex node ID of the target node (e.g. the ``"NodeID"`` field
            returned by :func:`ray.nodes`).
        labels: Mapping of label key -> value to apply. Passing an empty mapping
            clears all user-defined labels (reserved labels are still preserved).
            Keys must not use the reserved ``ray.io/`` prefix and keys/values must
            be valid Kubernetes-style label keys/values.
        timeout: Optional RPC timeout in seconds. ``None`` (the default) uses an
            unlimited gRPC deadline.

    Raises:
        ValueError: If ``labels`` contains a reserved key or an invalid
            key/value.
    """
    if labels is None:
        labels = {}
    for key, value in labels.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise ValueError(
                "Node label keys and values must be strings, got "
                f"({type(key).__name__}, {type(value).__name__})."
            )
    # Reuse the same validation applied to `ray start --labels`: reject reserved
    # `ray.io/`-prefixed keys and enforce Kubernetes-style key/value syntax.
    validate_node_labels(labels)
    validate_node_label_syntax(labels)

    worker = ray._private.worker.global_worker
    worker.check_connected()
    node_id_bytes = ray.NodeID.from_hex(node_id).binary()
    worker.gcs_client.update_node_labels(node_id_bytes, labels, timeout=timeout)
