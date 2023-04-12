from ray.experimental.lightrails.schedule.instruction import (
    Backward,
    Forward,
    LoadBatch,
    Optimize,
    ReceiveActivation,
    ReceiveGradient,
    SendActivation,
    SendGradient,
)
from ray.experimental.lightrails.schedule.schedule import Schedule


# This is a very simple schedule that is used for testing.
class FirstStageSchedule(Schedule):
    def __init__(self, downstream_rank: int, num_batches: int) -> None:
        super().__init__()
        self.downstream_rank = downstream_rank
        self.num_batches = num_batches

    def steps(self):
        """Yield a list of :class:`Instructions` for each step in the schedule."""
        schedule = [
            LoadBatch(1),
            Forward(1),
            SendActivation(self.downstream_rank, 1),
            ReceiveGradient(self.downstream_rank, 1),
            Backward(1),
            Optimize(),
        ]
        for _ in range(self.num_batches):
            yield schedule


class MiddleStageSchedule(Schedule):
    def __init__(
        self, upstream_rank: int, downstream_rank: int, num_batches: int
    ) -> None:
        super().__init__()
        self.upstream_rank = upstream_rank
        self.downstream_rank = downstream_rank
        self.num_batches = num_batches

    def steps(self):
        """Yield a list of :class:`Instructions` for each step in the schedule."""
        schedule = [
            ReceiveActivation(self.upstream_rank, 1),
            Forward(1),
            SendActivation(self.downstream_rank, 1),
            ReceiveGradient(self.downstream_rank, 1),
            Backward(1),
            SendGradient(self.upstream_rank, 1),
            Optimize(),
        ]
        for _ in range(self.num_batches):
            yield schedule


class LastStageSchedule(Schedule):
    def __init__(self, upstream_rank: int, num_batches: int) -> None:
        super().__init__()
        self.upstream_rank = upstream_rank
        self.num_batches = num_batches

    def steps(self):
        """Yield a list of :class:`Instructions` for each step in the schedule."""
        schedule = [
            LoadBatch(1, is_label=True),
            ReceiveActivation(self.upstream_rank, 1),
            Forward(1),
            Backward(1),
            SendGradient(self.upstream_rank, 1),
            Optimize(),
        ]

        for _ in range(self.num_batches):
            yield schedule
