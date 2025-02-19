import asyncio
from enum import Enum
from typing import TYPE_CHECKING, AsyncIterator, Optional

from pydantic import BaseModel
from ray.util import metrics
from ray.util.metrics import Counter

if TYPE_CHECKING:
    from vllm.outputs import RequestOutput


engine_metrics_prefix = "vllm_engine_stats"
num_current_pending_requests_gauge = metrics.Gauge(
    f"{engine_metrics_prefix}_num_current_pending_requests",
    "current pending requests.",
)
num_current_running_requests_gauge = metrics.Gauge(
    f"{engine_metrics_prefix}_num_current_running_requests",
    "current running requests.",
)


class RequestState(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    ERRORED = "errored"
    CANCELLED = "cancelled"
    FINISHED = "finished"


state_counters = {
    RequestState.PENDING: Counter(
        f"{engine_metrics_prefix}_total_requests_submitted",
        "total submitted requests.",
    ),
    RequestState.RUNNING: Counter(
        f"{engine_metrics_prefix}_total_requests_started",
        "total started requests.",
    ),
    RequestState.ERRORED: Counter(
        f"{engine_metrics_prefix}_total_requests_errored",
        "total errored requests.",
    ),
    RequestState.CANCELLED: Counter(
        f"{engine_metrics_prefix}_total_requests_cancelled",
        "total cancelled requests.",
    ),
    RequestState.FINISHED: Counter(
        f"{engine_metrics_prefix}_total_requests_finished",
        "total finished requests.",
    ),
}

usage_metrics_prefix = "vllm_arg_usage_stats"


class ArgUsage(str, Enum):
    BEST_OF = "best_of"
    PRESENCE_PENALTY = "presence_penalty"
    FREQUENCY_PENALTY = "frequency_penalty"
    PRESENCE_AND_FREQUENCY_PENALTY = "presence_and_frequency_penalty"
    TEMPERATURE = "temperature"
    TOP_P = "top_p"
    TOP_K = "top_k"
    STOP = "stop"
    MAX_TOKENS = "max_tokens"
    LOGPROBS = "logprobs"


usage_counters = {
    ArgUsage.BEST_OF: Counter(
        f"{usage_metrics_prefix}_best_of_usage_count",
        "total number of usage of best of.",
    ),
    ArgUsage.PRESENCE_PENALTY: Counter(
        f"{usage_metrics_prefix}_presence_penalty_usage_count",
        "total number of usage of presence penalty.",
    ),
    ArgUsage.FREQUENCY_PENALTY: Counter(
        f"{usage_metrics_prefix}_frequency_penalty_usage_count",
        "total number of usage of frequency penalty.",
    ),
    ArgUsage.PRESENCE_AND_FREQUENCY_PENALTY: Counter(
        f"{usage_metrics_prefix}_presence_and_frequency_penalty_usage_count",
        "total number of usage when both presence penalty and frequency penalty are on.",
    ),
    ArgUsage.TEMPERATURE: Counter(
        f"{usage_metrics_prefix}_temperature_usage_count",
        "total number of usage of temperature.",
    ),
    ArgUsage.TOP_P: Counter(
        f"{usage_metrics_prefix}_top_p_usage_count",
        "total number of usage of top p.",
    ),
    ArgUsage.TOP_K: Counter(
        f"{usage_metrics_prefix}_top_k_usage_count",
        "total number of usage of top k.",
    ),
    ArgUsage.STOP: Counter(
        f"{usage_metrics_prefix}_stop_usage_count",
        "total number of usage of stop.",
    ),
    ArgUsage.MAX_TOKENS: Counter(
        f"{usage_metrics_prefix}_max_tokens_usage_count",
        "total number of usage of max tokens.",
    ),
    ArgUsage.LOGPROBS: Counter(
        f"{usage_metrics_prefix}_logprobs_usage_count",
        "total number of usage of logprobs.",
    ),
}


class StateStats(BaseModel):
    total_count: int = 0
    num_current: int = 0


class VLLMEngineStats(BaseModel):
    num_current_pending_requests: int
    num_current_running_requests: int
    total_requests_submitted: int
    total_requests_started: int
    total_requests_errored: int
    total_requests_cancelled: int
    total_requests_finished: int


class VLLMEngineStatTracker:
    def __init__(self):
        self.stats = {r: StateStats() for r in RequestState}

    def _update_gauges(self):
        num_current_pending_requests_gauge.set(
            self.stats[RequestState.PENDING].num_current
        )
        num_current_running_requests_gauge.set(
            self.stats[RequestState.RUNNING].num_current
        )

    def enter_state(self, state: RequestState):
        self.stats[state].total_count += 1
        self.stats[state].num_current += 1
        state_counters[state].inc()
        self._update_gauges()

    def exit_state(self, state: RequestState):
        self.stats[state].num_current -= 1
        self._update_gauges()

    async def auto_track(
        self, async_iterator: AsyncIterator["RequestOutput"]
    ) -> AsyncIterator["RequestOutput"]:
        # The request is pending right now
        request_state_tracker = RequestStateTracker(self)
        request_state_tracker.state = RequestState.PENDING
        try:
            async for x in async_iterator:
                request_state_tracker.state = RequestState.RUNNING
                yield x
            request_state_tracker.state = RequestState.FINISHED
        except asyncio.CancelledError:
            request_state_tracker.state = RequestState.CANCELLED
            raise
        except Exception:
            request_state_tracker.state = RequestState.ERRORED
            raise
        finally:
            # Remove the state
            request_state_tracker.state = None

    def to_stats(self) -> VLLMEngineStats:
        return VLLMEngineStats(
            num_current_pending_requests=self.stats[RequestState.PENDING].num_current,
            num_current_running_requests=self.stats[RequestState.RUNNING].num_current,
            total_requests_submitted=self.stats[RequestState.PENDING].total_count,
            total_requests_started=self.stats[RequestState.RUNNING].total_count,
            total_requests_cancelled=self.stats[RequestState.CANCELLED].total_count,
            total_requests_errored=self.stats[RequestState.ERRORED].total_count,
            total_requests_finished=self.stats[RequestState.FINISHED].total_count,
        )


class RequestStateTracker:
    """Track the stats for a single request"""

    def __init__(self, global_stats: VLLMEngineStatTracker):
        self._state: Optional[RequestState] = None
        self.global_stats = global_stats

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, state: RequestState):
        if state == self._state:
            # Noop
            return

        if self._state is not None:
            self.global_stats.exit_state(self._state)

        if state is not None:
            self.global_stats.enter_state(state)

        self._state = state

    def __del__(self):
        # Remove the state automatically when the object is deleted
        self.state = None
