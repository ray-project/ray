from ray._raylet import (
    Count,
    Histogram,
    Gauge,
    Sum,
)  # noqa: E402
"""Metric/Stats module for worker.

This module is responsible for providing four classes mapping from stats of
cpp.

How to use:
  For Count, Gauge and Sum, we may define a metric like this following:
    gauge = Gauge(
      'ray.worker.metric',
      'description',
      'unit',
      ['tagk1', 'tagk2']).
  The last parameter is default tag map. You can use gauge.record(1.0) with
  default tags or gauge.record(1.0, {'tagk1', 'tagv1'}) that means the tagk1
  is updating in tagv1.

  It's addtional boundaries to Histogram measurement,
    histogram = Histogram(
      'ray.worker.histogram1',
      'a', 'b', [1.0, 2.0],
      ['tagk1'])

  Recommended metric name pattern : ray.{component_name}.{module_name}, and
  name format must be in [0-9a-zA-Z].
"""

__all__ = [
    "Count",
    "Histogram",
    "Gauge",
    "Sum",
]
