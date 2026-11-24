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
step
    One pass of the executor's scheduling loop, many times a second. The
    sizer counts some things in steps, such as a donor's cooldown.

tick
    A step on which the sizer decides, at most once per ``SIZER_CADENCE_S``
    (about a second or two). Every tick is a step; most steps are not ticks.
    A tick runs: how many, where, apply.

phase (``SizerPhase``)
    WARMUP until every op has received input and every critical op has
    produced output: there is no feedback yet, so each op is sized to an
    equal share of the allocation. An op starts no actors before its first
    input; its share is held for it until then.
    MODEL_WARMING until the bottleneck model has seen a full window of every
    unfinished op (the ones it has not seen keep their equal share). STEADY
    after that.

decision (``DecisionKind``)
    The one primary action a tick takes from the bottleneck model: grow a
    bottleneck into free capacity, grow balanced stages, fund the recipient
    from donors, shrink blocked surplus, or hold.

status (``OpStatus``)
    What the sizer may do to an op now, a permission rather than a size (see
    ``op_status.py`` for the full state machine): held until its first input
    (room reserved, no actors), released (for one tick: grows to the room it
    held), active (any action whose guard passes may resize it), receiving
    or donating in a transfer, cooling down after donating, or completed.

rule (``OpRule``)
    The one action that sized an op this tick. From the bottleneck model:
    GROW_FREE, GROW_BALANCED, FUND, DONATE, SHRINK_SURPLUS. Per op:
    RELEASE_POOL, GRANT_HELD, WARMUP_SHARE, SHED_IDLE, REFILL_TO_MIN. Or
    NONE.

how_many
    The tick's first step: ``SizingPolicy.decide`` picks one rule per op,
    then the ledger clamps each per-op upscale to what still fits.

has work
    The op has queued input or tasks in flight.

ledger
    The free capacity left during one tick. The how-many step debits it as
    it grants per-op upscales.


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
