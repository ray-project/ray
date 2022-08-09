from typing import Optional
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class NodeLaunchException(Exception):
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

    def __repr__(self) -> str:
        return self.__str__()

    def __str__(self) -> str:
        if self.source_exception:
            return str(self.source_exception)
        else:
            return super().__str__()
