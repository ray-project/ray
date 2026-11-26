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
    draining. Actors are removed from these nodes until usage fits the slice.

PG-managed
    An op whose actors are placed through a placement group. The sizer does
    not choose their nodes. Non-PG ops are placed directly by the sizer.


State machine (``operator_sizer``, ``policy``)
----------------------------------------------
step
    One pass of the executor's scheduling loop, many times a second.

tick
    A step on which the sizer decides, at most once per ``SIZER_CADENCE_S``
    (about a second or two). Every tick is a step; most steps are not ticks.
    A tick runs: how many, where, apply.

phase (``SizerPhase``)
    WARMUP until every op has received input, every critical op has
    produced output, and the bottleneck observer has then measured one full
    window: there is no feedback yet, so each op is sized to an equal share
    of the allocation. An op starts no actors before its first input; its
    share is held for it until then. STEADY after that.

status (``OpStatus``)
    What the sizer may do to an op now, a permission rather than a size (see
    ``op_status.py`` for the full state machine): held until its first input
    (room reserved, no actors), active (any rule may resize it), cooling
    after the policy resized it, or completed.

rule (``OpRule``)
    What sized an op this tick (``SizingPolicy.decide_op``): RELEASE_POOL,
    UPSCALE_TO_MIN, UPSCALE_WARMUP, UPSCALE_BOTTLENECK,
    UPSCALE_BUSY, DOWNSCALE_BLOCKED, DOWNSCALE_IDLE, or NONE.

upscale / downscale
    Adding or removing actors. Every rule does one or the other, or nothing.
    "Drain" means only what the pool does to a running actor it removes:
    the actor finishes its tasks before it exits.

step size
    How far one upscale or downscale moves an op after warmup, as a fraction
    of its size (``policy/steps.py``). Each op's steps double while it keeps
    moving the same way and halve when it turns around.

how_many
    The tick's first step: ``SizingPolicy.decide`` picks one rule and count
    per op. WHERE then places the counts.

ledger
    The free capacity left during one tick (``ConstraintAwareCapacityLedger``).
    Every upscale in the how-many step checks and spends it.


Bottleneck snapshot (``bottleneck_observer``)
---------------------------------------------
busy / obp / starved
    Shares of an op's slot time spent running, output-backpressured, or idle
    with nothing queued.

blocked / self-limited
    The two parts of a producer's obp: blocked while its consumer had no
    idle slot, self-limited while the consumer had one and the producer's
    own output budget held it back.

bottleneck
    An op no edge's arrow points away from, with enough severity (how much
    its neighbours wait on it).
"""
