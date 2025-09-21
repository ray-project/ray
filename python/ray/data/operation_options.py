from dataclasses import dataclass

@dataclass
class OperatorOptions:
    """Options for configuring individual operators."""

    # Whether to disable fusion for this operator. By default, fusion is enabled.
    disable_fusion: bool = False
