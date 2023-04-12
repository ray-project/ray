from abc import ABCMeta, abstractmethod

from ray.experimental.lightrails.schedule.instruction import (
    Forward,
    LoadBatch,
    PrintOutput,
    ReceiveActivation,
    SendActivation,
)


class Schedule(metaclass=ABCMeta):
    """A schedule represents the execution schedule of a stage replica."""

    @abstractmethod
    def steps(self):
        """Yield a list of :class:`Instructions` for each step in the schedule."""
        pass


# Inferencing schedules
class InputSchedule(Schedule):
    def __init__(self, downstream_rank: int) -> None:
        super().__init__()
        self.downstream_rank = downstream_rank

    def steps(self):
        """Yield a list of :class:`Instructions` for each step in the schedule."""
        while True:
            yield [LoadBatch(1), Forward(1), SendActivation(self.downstream_rank, 1)]


class ExecuteSchedule(Schedule):
    def __init__(self, upstream_rank: int, downstream_rank: int) -> None:
        super().__init__()
        self.upstream_rank = upstream_rank
        self.downstream_rank = downstream_rank

    def steps(self):
        """Yield a list of :class:`Instructions` for each step in the schedule."""
        while True:
            yield [
                ReceiveActivation(self.upstream_rank, 1),
                Forward(1),
                SendActivation(self.downstream_rank, 1),
            ]


class OutputSchedule(Schedule):
    def __init__(self, upstream_rank: int) -> None:
        super().__init__()
        self.upstream_rank = upstream_rank

    def steps(self):
        """Yield a list of :class:`Instructions` for each step in the schedule."""
        while True:
            yield [ReceiveActivation(self.upstream_rank, 1), Forward(1), PrintOutput(1)]
