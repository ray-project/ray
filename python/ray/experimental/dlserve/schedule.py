from abc import ABC

class Instruction(ABC):
    """An instruction represents a single step in the execution schedule."""

    pass


class Send(Instruction):
    """Send data to dest rank."""

    def __init__(self, dest_rank: int, count: int = 1):
        self.dest_rank = dest_rank
        self.count = count


class Receive(Instruction):
    """Receive data from dest rank."""

    def __init__(self, src_rank: int, count: int = 1):
        self.src_rank = src_rank
        self.count = count


class Forward(Instruction):
    """Apply forward computation against the model."""

    def __init__(self, count: int = 1):
        self.count = count


class LoadBatch(Instruction):
    """Load a batch of data from the data loader."""

    def __init__(self, count: int = 1):
        self.count = count


class Schedule(ABC):
    """A schedule represents the execution schedule of a stage replica."""

    def steps(self):
        """Yield a list of :class:`Instructions` for each step in the schedule."""
        pass