from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List, Optional

if TYPE_CHECKING:

    from ray.data.dataset import RefBundle


class SourceOperator(ABC):
    """Mixin for Logical operators that can be logical source nodes.
    Subclasses: Read, InputData, FromAbstract.
    """

    @abstractmethod
    def output_data(self) -> Optional[List["RefBundle"]]:
        """The output data of this operator if already known, or ``None``."""
        pass
