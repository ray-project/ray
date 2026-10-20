"""Which messages fill each time step of a topic, for ``TopicSpec``."""

from ray.data._internal.datasource_v2.formats.mcap.mcap_topic_spec import (
    AllInStep,
    Backward,
    Nearest,
)
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
def nearest() -> Nearest:
    """Each step takes the message logged nearest to it, the earlier on a tie.

    A step never takes a message from the other side of the anchor. This is
    ``TopicSpec``'s default.
    """
    return Nearest()


@PublicAPI(stability="alpha")
def backward() -> Backward:
    """Each step takes the last message logged at or before it.

    No step sees a message logged after its own time, as with pandas'
    ``merge_asof(direction="backward")``: the usual rule for signals such as
    joint state or CAN. A step after the anchor takes only messages logged
    after the anchor.
    """
    return Backward()


@PublicAPI(stability="alpha")
def all_in_step() -> AllInStep:
    """Each step takes every message logged in its slot: from just after the
    previous step up to and including this one.

    The first step's slot reaches one step period back, and no slot crosses the
    anchor. A row holds a list of messages per step, such as the packets of a
    lidar spin. An empty slot is padded as ``TopicSpec(pad=...)`` says.
    """
    return AllInStep()
