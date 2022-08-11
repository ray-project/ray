from typing import Optional

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
        source_exception: Optional[Exception] = None,
    ):
        super().__init__(f"Node Launch Exception ({category}): {description}")
        self.category = category
        self.description = description
        self.source_exception = source_exception
