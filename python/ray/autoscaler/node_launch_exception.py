from typing import Any, Optional, Tuple

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
        src_exc_info: Optional[Tuple[Any, Any, Any]],  # The
    ):
        """Args:
        category: A short (<20 chars) label for the error.
        description: A longer, human readable description of the error.
        src_exc_info: The source exception info if applicable. This is a
              tuple of (type, exception, traceback) as returned by
              sys.exc_info()

        """
        super().__init__(f"Node Launch Exception ({category}): {description}")
        self.category = category
        self.description = description
        self.src_exc_info = src_exc_info

    def __reduce__(self):
        # NOTE: Since tracebacks can't be pickled, we'll drop the optional
        # traceback if we have to serialize this object.
        return (self.__class__, (self.category, self.description, None),)
