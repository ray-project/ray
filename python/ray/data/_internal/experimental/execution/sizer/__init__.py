"""Actor-pool sizing for the actor-only backend (see ``operator_sizer``).

Glossary of terms used throughout the sizer's code and comments.

Capacity
--------
reserved
    This dataset's slice of each node, as handed out by the autoscaling
    coordinator (``DatasetReservationView``).

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


State machine (``operator_sizer``, ``policy``)
----------------------------------------------
tick
    One sizing evaluation, performed every ``SIZER_CADENCE_S``.

phase (``SizerPhase``)
    WARMUP until every critical op has produced output: there is no feedback
    yet, so each op is sized to an equal share of the allocation.
    MODEL_WARMING until the bottleneck model has seen a full window of every
    unfinished op (the ones it has not seen keep their equal share). STEADY
    after that.

decision (``DecisionKind``)
    The one primary action a tick takes from the bottleneck model: grow a
    bottleneck into free capacity, grow balanced stages, fund the recipient
    from donors, shrink blocked surplus, or hold.

lifecycle (``OpLifecycle``)
    Whether an op may be resized now: awaiting its on_start, active,
    receiving or donating in a transfer, cooling down after donating, or
    completed.

how_many
    Per-op wants that need no model: warmup equal shares, idle shedding,
    min-size refill, releasing completed pools.

has work
    The op has queued input or tasks in flight.

ledger
    The free capacity available during one sizing tick, debited as how_many
    requests are granted.


Bottleneck model (``bottleneck_observer``)
------------------------------------------
busy / obp / starved
    Shares of an op's slot time spent running, output-backpressured, or idle
    with nothing queued.

bottleneck
    An op no edge's arrow points away from, with enough severity (how much
    its neighbours wait on it). Stable once named for a full window.

recipient
    The most severe stable bottleneck that can use more capacity.

slack
    Actors an op could give up and keep up after the recipient grows.

capacity transfer
    Moving capacity from ops with slack (donors) to the recipient.

victim
    A donor actor evicted to make room for the recipient.
"""
