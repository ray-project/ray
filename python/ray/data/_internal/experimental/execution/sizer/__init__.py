"""Actor-pool sizing for the actor-only backend (see ``operator_sizer``).

Glossary of terms used throughout the sizer's code and comments.

Capacity
--------
reserved
    This dataset's slice of each node, as handed out by the autoscaling
    coordinator (``DatasetAllocationView``).

committed
    Capacity held by this dataset's non-PG actors on a node: running actors
    plus pending actors assigned to that node.

free
    Remaining capacity on a node: ``reserved - committed``.

schedulable nodes
    Nodes on which an op may run: the op's label selector matches, and the
    dataset's slice is large enough to fit at least one of its actors.

constrained
    An op that can run on only a subset of nodes, either because it has a
    label selector or requires a GPU.

nodes to reclaim
    Nodes where committed capacity exceeds the dataset's current slice, for
    example because the coordinator shrank the allocation or the node is
    draining. Actors are evicted from these nodes until usage fits the slice.

PG-managed
    An op whose actors are placed through a placement group. The sizer does
    not choose their nodes. Non-PG ops are placed directly by the sizer.


Per-op sizing (``scale_how_many``)
----------------------------------
tick
    One sizing evaluation, performed every ``SIZER_CADENCE_S``.

warmup
    The initial phase of a run. Until every critical op has produced output,
    there is no backpressure signal to react to, so each op is sized to an
    equal share of the allocation.

has work
    The op has queued input or tasks in flight.

backpressured
    The op's output is blocked: enough of its actors have exhausted their
    output budget (``SIZER_OUTQUEUE_FULL_FRACTION``).

demand
    The number of actors the op needs for its current backlog: queued input
    plus tasks in flight.

want
    The actor-count delta requested by the op based on its own signals,
    before capacity constraints are applied.

ledger
    The free capacity available during one sizing tick, debited as growth
    requests are granted.

shortfall
    The number of requested actors that could not be granted.

ungranted
    An op that requested growth but was granted zero actors.


Cross-op optimization (``optimizer``)
--------------------------------------
guard
    A lock over ops. While one optimization rule is resizing an op, no other
    rule may modify it.

keeping up
    A consumer that processes its producer's output as fast as it arrives,
    so the consumer is not what limits the pipeline.

silent bottleneck
    An op that limits the pipeline without asking to grow: its actors are busy
    and its output is backpressured, while its consumers are keeping up.

capacity transfer
    Moving capacity from ops with spare actors (donors) to an op whose growth
    requests repeatedly go ungranted (the recipient).

victim
    A donor actor evicted to make room for the recipient.
"""
