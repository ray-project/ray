from dataclasses import dataclass

from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
@dataclass
class OperatorOptions:
    """Options for configuring individual operators."""

    # Whether to disable fusion for this operator. By default, fusion is enabled.
    disable_fusion: bool = False
