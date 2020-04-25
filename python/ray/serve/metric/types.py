# The metric types are inspired by OpenTelemetry spec:
# https://github.com/open-telemetry/opentelemetry-specification/blob/master/specification/metrics/api.md#three-kinds-of-instrument
class Counter:
    def __init__(self, client, name):
        self.client = client
        self.name = name

    def add(self, increment=1):
        self.client.push_event(EventType.COUNTER, self.name, increment)


class Measure:
    def __init__(self, client, name):
        self.client = client
        self.name = name

    def record(self, value):
        self.client.push_event(EventType.MEASURE, self.name, value)


class EventType(enum.IntEnum):
    COUNTER = 1
    MEASURE = 2

    def to_user_facing_class(self):
        return {self.COUNTER: Counter, self.MEASURE: Measure}[self.value]

    def to_prometheus_class(self):
        return {self.COUNTER: PromCounter, self.MEASURE: PromGauge}[self.value]
