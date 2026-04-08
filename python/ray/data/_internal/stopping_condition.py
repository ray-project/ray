import enum


class StoppingCondition(enum.Enum):
    """Controls when a mix pipeline terminates.

    STOP_ON_SHORTEST: Pipeline ends when the shortest dataset is exhausted.
        Other datasets are truncated.
    STOP_ON_LONGEST_DROP: Pipeline ends when the longest dataset is exhausted.
        Shorter datasets drop out once exhausted; later batches are drawn
        entirely from longer datasets.
    """

    STOP_ON_SHORTEST = "stop_on_shortest"
    STOP_ON_LONGEST_DROP = "stop_on_longest_drop"
