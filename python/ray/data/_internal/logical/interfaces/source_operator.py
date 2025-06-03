from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List, Optional, Union

if TYPE_CHECKING:
    import pyarrow as pa

    from ray.data.block import BlockMetadata, PandasBlockSchema
    from ray.data.dataset import RefBundle

# TODO(jusitn): split this into 2, it's not always the case
# that both schema and metadata are correlated
class GuessMetadataMixin(ABC):
    """Mixin for Logical operators that can infer metadata from
    input dependencies prior to execution.
    """

    @abstractmethod
    def guess_schema(
        self,
    ) -> Optional[Union[type, "PandasBlockSchema", "pa.lib.Schema"]]:
        """Return best guess of schema for dataset."""
        pass

    @abstractmethod
    def guess_metadata(self) -> "BlockMetadata":
        """Return best guess of metadata for dataset."""
        pass


class SourceOperatorMixin(GuessMetadataMixin, ABC):
    """Mixin for Logical operators that can be logical source nodes.
    Subclasses: Read, InputData, FromAbstract.
    """

    @abstractmethod
    def output_data(self) -> Optional[List["RefBundle"]]:
        """The output data of this operator if already known, or ``None``."""
        pass
