from typing import Tuple

from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class NodeLaunchException(Exception):
    """A structured exception that can be thrown by a node provider during a
    `create_node` call to pass additional information for observability.
    """

    def __init__(
        self,
        category: str,
        description: str,
        src_exc_info: Tuple["type", "value", "traceback"],  # The
        # tuple
        # outputted
        # by
        # sys.exc_info()
        # during
        # an
        # exception.
    ):
        super().__init__(f"Node Launch Exception ({category}): {description}")
        self.category = category
        self.description = description
        self.src_exc_info = src_exc_info
