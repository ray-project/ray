import asyncio

from ray.serve.constants import METRIC_PUSH_INTERVAL_S
from ray.serve.metric.types import EventType

MetricMetadata = Dict[str, Dict]
MetricEventBatch = List[Tuple[EventType, str, float]]


class MetricClient:
    def __init__(self,
                 push_batch_callback: Callable[
                     [MetricMetadata, MetricEventBatch], None],
                 default_labels: Optional[Dict[str, str]] = None):
        self.default_labels = default_labels or dict()

        self.metric_metadata: MetricMetadata = dict()
        self.metric_events: MetricEventBatch = []

        self.push_batch_callback = push_batch_callback
        self.push_task = asyncio.get_event_loop().create_task(
            self.push_forever(METRIC_PUSH_INTERVAL_S))
        self.new_metric_added = asyncio.Event()

    @staticmethod
    def connect_from_serve(default_labels=None):
        from ray.serve.api import _get_master_actor

        master_actor = _get_master_actor()
        [metric_sink] = ray.get(master_actor.get_metric_sink.remote())
        return MetricClient(
            lambda *args: metric_sink.push_batch.remote(*args),
            default_labels=default_labels)

    def new_counter(self,
                    name,
                    *,
                    description: Optional[str] = "",
                    labels: Optional[Dict[str, str]] = None) -> Counter:
        return self._new_metric(EventType.COUNTER, name, description, labels)

    def new_measure(self,
                    name,
                    *,
                    description: Optional[str] = "",
                    labels: Optional[Dict[str, str]] = None) -> Measure:
        return self._new_metric(EventType.MEASURE, name, description, labels)

    def _new_metric(self,
                    metric_type: EventType,
                    name,
                    description: Optional[str] = "",
                    labels: Optional[Dict[str, str]] = None):
        # Normalize Labels
        labels = labels or dict()
        merged_labels = self.default_labels.copy()
        merged_labels.update(labels)

        self.metric_metadata[name] = {
            "type": metric_type,
            "description": description,
            "labels": merged_labels
        }

        metric_class = metric_type.to_user_facing_class()
        return metric_class(collector=self, name=name)

    def push_event(self, metric_type: EventType, name, value):
        self.metric_events.append((metric_type, name, value))

    async def push_once(self):
        old_batch, self.metric_events = self.metric_events, []
        await self.push_batch_callback(self.metric_metadata, old_batch)

    async def push_forever(self, interval_s):
        while True:
            await self.push_once()
            await asyncio.sleep(interval_s)
