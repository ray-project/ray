"""Observability — stats, logging, progress, and diagnostics.

Modules:
    stats: DatasetStats, timing, counters, histograms, _StatsManager
    logging: Ray Data logging setup, SessionFileHandler
    memory_tracing: Object store allocation tracing (debug)
    metadata_exporter: Dashboard metadata export
    operator_event_exporter: Operator-level event export
    operator_schema_exporter: Schema export for observability
    dataset_repr: ASCII repr for datasets
    average_calculator: Rolling time-window average for metrics
"""
