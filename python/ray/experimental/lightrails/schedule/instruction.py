from abc import ABCMeta, abstractmethod


class Instruction(metaclass=ABCMeta):
    """An instruction represents a single step in the execution schedule."""

    pass


class SendActivation(Instruction):
    """Send data to dest rank."""

    def __init__(self, dest_rank: int, count: int = 1):
        self.dest_rank = dest_rank
        self.count = count


class ReceiveActivation(Instruction):
    """Receive data from dest rank."""

    def __init__(self, src_rank: int, count: int = 1):
        self.src_rank = src_rank
        self.count = count


class SendGradient(Instruction):
    """Send data to dest rank."""

    def __init__(self, dest_rank: int, count: int = 1):
        self.dest_rank = dest_rank
        self.count = count


class ReceiveGradient(Instruction):
    """Receive data from dest rank."""

    def __init__(self, src_rank: int, count: int = 1):
        self.src_rank = src_rank
        self.count = count


class Optimize(Instruction):
    """Receive data from dest rank."""

    pass


class Forward(Instruction):
    """Apply forward computation against the model."""

    def __init__(self, count: int = 1):
        self.count = count


class Backward(Instruction):
    """Apply backward computation against the model."""

    def __init__(self, count: int = 1):
        self.count = count


class LoadBatch(Instruction):
    """Load a batch of data from the data loader."""

    def __init__(self, count: int = 1, is_label: bool = False):
        self.count = count
        self.is_label = is_label


class PrintOutput(Instruction):
    """Print the output."""

    def __init__(self, count: int = 1):
        self.count = count
