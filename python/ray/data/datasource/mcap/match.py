"""Which message fills each time step of a topic, for ``TopicSpec``."""

from ray.data._internal.datasource_v2.formats.mcap.mcap_topic_spec import Nearest
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
def nearest() -> Nearest:
    """Each step takes the message logged nearest to it, the earlier on a tie.

    A step never takes a message from the other side of the anchor. This is
    ``TopicSpec``'s default.
    """
    return Nearest()
