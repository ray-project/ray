import json
import logging
import os
import threading
import time
import traceback
from collections import namedtuple, defaultdict
from typing import List, Tuple, Any, List, Dict

from prometheus_client import CollectorRegistry
from prometheus_client.core import (
    REGISTRY,
    CounterMetricFamily,
    GaugeMetricFamily,
    HistogramMetricFamily,
    UnknownMetricFamily,
)
from opencensus.metrics.export.value import ValueDouble
from opencensus.stats import aggregation
from opencensus.stats import measure as measure_module
from opencensus.stats.view_manager import ViewManager
from opencensus.stats.stats_recorder import StatsRecorder
from opencensus.stats.base_exporter import StatsExporter
from opencensus.stats.aggregation_data import (
    CountAggregationData,
    DistributionAggregationData,
    LastValueAggregationData,
)
from opencensus.stats.view import View
from opencensus.stats.view_data import ViewData
from opencensus.tags import tag_key as tag_key_module
from opencensus.tags import tag_map as tag_map_module
from opencensus.tags import tag_value as tag_value_module

import ray
from ray._private.gcs_utils import GcsClient
from ray.core.generated.metrics_pb2 import Metric

logger = logging.getLogger(__name__)

# Env var key to decide worker timeout.
# If the worker doesn't report for more than
# this time, we treat workers as dead.
RAY_WORKER_TIMEOUT_S = "RAY_WORKER_TIMEOUT_S"


class Gauge(View):
    """Gauge representation of opencensus view.

    This class is used to collect process metrics from the reporter agent.
    Cpp metrics should be collected in a different way.
    """

    def __init__(self, name, description, unit, tags: List[str]):
        self._measure = measure_module.MeasureInt(name, description, unit)
        tags = [tag_key_module.TagKey(tag) for tag in tags]
        self._view = View(
            name, description, tags, self.measure, aggregation.LastValueAggregation()
        )

    @property
    def measure(self):
        return self._measure

    @property
    def view(self):
        return self._view

    @property
    def name(self):
        return self.measure.name


Record = namedtuple("Record", ["gauge", "value", "tags"])


class WorkerProxyExportState:
    def __init__(self):
        self._last_reported_time = time.monotonic()
        # view_name
        # -> set of tag_vals tuple that this worker genereated.
        # NOTE: We assume a tuple of tag_vals correspond to a
        # single worker because it always contain WorkerId.
        self._view_to_owned_tag_vals = defaultdict(set)

    @property
    def last_reported_time(self):
        return self._last_reported_time

    @property
    def view_to_owned_tag_vals(self):
        return self._view_to_owned_tag_vals

    def update_last_reported_time(self):
        self._last_reported_time = time.monotonic()

    def put_tag_vals(self, view_name: str, tag_vals: Tuple[str]):
        self._view_to_owned_tag_vals[view_name].add(tag_vals)


class ProxyedMetric:
    def __init__(self, name: str, desc: str, unit: str, columns: List[str]):
        self._name = name
        self._desc = desc
        self._unit = unit
        self._columns = columns
        # tuple of label values -> data (OpenCesnsus Aggregation data)
        self._data = {}
    
    @property
    def name(self):
        return self._name
    
    @property
    def desc(self):
        return self._desc
    
    @property
    def unit(self):
        return self._unit
    
    @property
    def columns(self):
        return self._columns

    @property
    def data(self):
        return self._data

    def record(self, metric: Metric):
        timeseries = metric.timeseries

        if len(timeseries) == 0:
            return

        # Create the aggregation and fill it in the our stats
        for series in timeseries:
            labels = tuple(val.value for val in series.label_values)

            # Aggregate points.
            for point in series.points:
                if point.HasField("int64_value"):
                    data = CountAggregationData(point.int64_value)
                elif point.HasField("double_value"):
                    data = LastValueAggregationData(ValueDouble, point.double_value)
                elif point.HasField("distribution_value"):
                    dist_value = point.distribution_value
                    counts_per_bucket = [
                        bucket.count for bucket in dist_value.buckets
                    ]
                    bucket_bounds = dist_value.bucket_options.explicit.bounds
                    data = DistributionAggregationData(
                        dist_value.sum / dist_value.count,
                        dist_value.count,
                        dist_value.sum_of_squared_deviation,
                        counts_per_bucket,
                        bucket_bounds,
                    )
                else:
                    raise ValueError("Summary is not supported")
                self._data[labels] = data


class Component:
    def __init__(self, id: str):
        self.id = id
        self._last_reported_time = time.monotonic()
        # metrics_name (str) -> metric (ProxyedMetric)
        self._metrics = defaultdict(dict)

    @property
    def metrics(self) -> Dict[str, Dict[List[str], Any]]:
        return self._metrics

    @property
    def last_reported_time(self):
        return self._last_reported_time

    def record(self, metrics: List[Metric]):
        self._update_last_reported_time()
        # Walk the protobufs and convert them to ViewData
        for metric in metrics:
            descriptor = metric.metric_descriptor
            name = descriptor.name
            columns = [label_key.key for label_key in descriptor.label_keys]

            if name not in self._metrics:
                self._metrics[name] = ProxyedMetric(
                    name,
                    descriptor.description,
                    descriptor.unit,
                    columns
                )
            self._metrics[name].record(metric)

    def _update_last_reported_time(self):
        self._last_reported_time = time.monotonic()

class OpenCensusProxyCollector:
    def __init__(self, namespace: str):
        self._namespace = namespace
        # component_id -> Components
        # For workers, they contain worker ids.
        # For other components (raylet, GCS),
        # they contain the global key "CORE"
        self._components = {}

    def record(self, metrics: List[Metric], worker_id_hex: str = None):
        key = "CORE" if not worker_id_hex else worker_id_hex
        if key not in self._components:
            self._components[key] = Component(key)
        self._components[key].record(metrics)

    def clean_stale_components(self, timeout: int):
        stale_components = []
        stale_component_ids = []
        for id, component in self._components.items():
            elapsed = time.monotonic() - component.last_reported_time
            if  elapsed > timeout:
                stale_component_ids.append(id)
                logger.info(
                    "Metrics from a worker ({}) is cleaned up due to "
                    "timeout. Time since last report {}s".format(
                        id, elapsed
                    )
                )
        for id in stale_component_ids:
            stale_components.append(self._components.pop(id))
        return stale_components

    # TODO: add start and end timestamp
    def to_metric(self, metric_name, metric_description, label_keys, metric_units, tag_values, agg_data, metrics_map):
        """to_metric translate the data that OpenCensus create
        to Prometheus format, using Prometheus Metric object
        :type desc: dict
        :param desc: The map that describes view definition
        :type tag_values: tuple of :class:
            `~opencensus.tags.tag_value.TagValue`
        :param object of opencensus.tags.tag_value.TagValue:
            TagValue object used as label values
        :type agg_data: object of :class:
            `~opencensus.stats.aggregation_data.AggregationData`
        :param object of opencensus.stats.aggregation_data.AggregationData:
            Aggregated data that needs to be converted as Prometheus samples
        :rtype: :class:`~prometheus_client.core.CounterMetricFamily` or
                :class:`~prometheus_client.core.HistogramMetricFamily` or
                :class:`~prometheus_client.core.UnknownMetricFamily` or
                :class:`~prometheus_client.core.GaugeMetricFamily`
        :returns: A Prometheus metric object
        """
        metric_name = f"{self._namespace}_{metric_name}"
        assert len(tag_values) == len(label_keys), (tag_values, label_keys)
        # Prometheus requires that all tag values be strings hence
        # the need to cast none to the empty string before exporting. See
        # https://github.com/census-instrumentation/opencensus-python/issues/480
        tag_values = [tv if tv else "" for tv in tag_values]

        if isinstance(agg_data, CountAggregationData):
            metric = metrics_map.get(metric_name)
            if not metric:
                metric = CounterMetricFamily(
                    name=metric_name,
                    documentation=metric_description,
                    unit=metric_units,
                    labels=label_keys,
                )
                metrics_map[metric_name] = metric
            metric.add_metric(labels=tag_values, value=agg_data.count_data)
            return metric

        elif isinstance(agg_data, DistributionAggregationData):

            assert agg_data.bounds == sorted(agg_data.bounds)
            # buckets are a list of buckets. Each bucket is another list with
            # a pair of bucket name and value, or a triple of bucket name,
            # value, and exemplar. buckets need to be in order.
            buckets = []
            cum_count = 0  # Prometheus buckets expect cumulative count.
            for ii, bound in enumerate(agg_data.bounds):
                cum_count += agg_data.counts_per_bucket[ii]
                bucket = [str(bound), cum_count]
                buckets.append(bucket)
            # Prometheus requires buckets to be sorted, and +Inf present.
            # In OpenCensus we don't have +Inf in the bucket bonds so need to
            # append it here.
            buckets.append(["+Inf", agg_data.count_data])
            metric = metrics_map.get(metric_name)
            if not metric:
                metric = HistogramMetricFamily(
                    name=metric_name,
                    documentation=metric_description,
                    labels=label_keys,
                )
                metrics_map[metric_name] = metric
            metric.add_metric(
                labels=tag_values,
                buckets=buckets,
                sum_value=agg_data.sum,
            )
            return metric

        elif isinstance(agg_data, LastValueAggregationData):
            metric = metrics_map.get(metric_name)
            if not metric:
                metric = GaugeMetricFamily(
                    name=metric_name,
                    documentation=metric_description,
                    labels=label_keys,
                )
                metrics_map[metric_name] = metric
            metric.add_metric(labels=tag_values, value=agg_data.value)
            return metric

        else:
            raise ValueError(f"unsupported aggregation type {type(agg_data)}")

    def collect(self):  # pragma: NO COVER
        """Collect fetches the statistics from OpenCensus
        and delivers them as Prometheus Metrics.
        Collect is invoked every time a prometheus.Gatherer is run
        for example when the HTTP endpoint is invoked by Prometheus.
        """
        # Make a shallow copy of self._view_name_to_data_map, to avoid seeing
        # concurrent modifications when iterating through the dictionary.
        metrics_map = {}
        for component in self._components.values():
            for metric in component.metrics.values():
                for label_values, data in metric.data.items():
                    self.to_metric(
                        metric.name,
                        metric.desc,
                        metric.columns,
                        metric.unit,
                        label_values,
                        data,
                        metrics_map
                    )

        for metric in metrics_map.values():
            yield metric


class MetricsAgent:
    def __init__(
        self,
        view_manager: ViewManager,
        stats_recorder: StatsRecorder,
        stats_exporter: StatsExporter = None,
    ):
        """A class to record and export metrics.

        The class exports metrics in 2 different ways.
        - Directly record and export metrics using OpenCensus.
        - Proxy metrics from other core components
            (e.g., raylet, GCS, core workers).

        This class is thread-safe.
        """
        # Lock required because gRPC server uses
        # multiple threads to process requests.
        self._lock = threading.Lock()

        #
        # Opencensus components to record metrics.
        #

        # Managing views to export metrics
        # If the stats_exporter is None, we disable all metrics export.
        self.view_manager = view_manager
        # A class that's used to record metrics
        # emitted from the current process.
        self.stats_recorder = stats_recorder
        # A class to export metrics.
        self.stats_exporter = stats_exporter

        if self.stats_exporter is None:
            # If the exporter is not given,
            # we disable metrics collection.
            self.view_manager = None
        else:
            self.view_manager.register_exporter(stats_exporter)

        # After the timeout, the worker is marked as dead.
        # The timeout is reset every time worker reports
        # new metrics (it happens every 2 seconds by default).
        # This value must be longer than the Prometheus
        # scraping interval (10s by default in Ray).
        self.worker_timeout_s = int(os.getenv(RAY_WORKER_TIMEOUT_S, 120))
        self.c = OpenCensusProxyCollector(self.stats_exporter.options.namespace)

    def _get_mutable_view_data(self, view_name: str) -> ViewData:
        """Return the current view data for a given view name."""
        assert self._lock.locked()
        return self.view_manager.measure_to_view_map._measure_to_view_data_list_map[
            view_name
        ][-1]

    def record_and_export(self, records: List[Record], global_tags=None):
        """Directly record and export stats from the same process."""
        global_tags = global_tags or {}
        with self._lock:
            if not self.view_manager:
                return

            for record in records:
                gauge = record.gauge
                value = record.value
                tags = record.tags
                self._record_gauge(gauge, value, {**tags, **global_tags})

    def _record_gauge(self, gauge: Gauge, value: float, tags: dict):
        view_data = self.view_manager.get_view(gauge.name)
        if not view_data:
            self.view_manager.register_view(gauge.view)
            # Reobtain the view.
        view = self.view_manager.get_view(gauge.name).view
        measurement_map = self.stats_recorder.new_measurement_map()
        tag_map = tag_map_module.TagMap()
        for key, tag_val in tags.items():
            tag_key = tag_key_module.TagKey(key)
            tag_value = tag_value_module.TagValue(tag_val)
            tag_map.insert(tag_key, tag_value)
        measurement_map.measure_float_put(view.measure, value)
        # NOTE: When we record this metric, timestamp will be renewed.
        measurement_map.record(tag_map)

    def proxy_export_metrics(self, metrics: List[Metric], worker_id_hex: str = None):
        """Proxy export metrics specified by a Opencensus Protobuf.

        This API is used to export metrics emitted from
        core components.

        Args:
            metrics: A list of protobuf Metric defined from OpenCensus.
            worker_id_hex: The worker ID it proxies metrics export. None
                if the metric is not from a worker (i.e., raylet, GCS).
        """
        with self._lock:
            if not self.view_manager:
                return

            self._proxy_export_metrics(metrics, worker_id_hex)

    def _proxy_export_metrics(self, metrics: List[Metric], worker_id_hex: str = None):
        assert self._lock.locked()
        self.c.record(metrics, worker_id_hex)

    def clean_all_dead_worker_metrics(self):
        """Clean dead worker's metrics.

        Worker metrics are cleaned up if there was no metrics
        report for more than `worker_timeout_s`.

        This method has to be periodically called by a caller.
        """
        with self._lock:
            if not self.view_manager:
                return
            
            self.c.clean_stale_components(self.worker_timeout_s)


class PrometheusServiceDiscoveryWriter(threading.Thread):
    """A class to support Prometheus service discovery.

    It supports file-based service discovery. Checkout
    https://prometheus.io/docs/guides/file-sd/ for more details.

    Args:
        gcs_address: Gcs address for this cluster.
        temp_dir: Temporary directory used by
            Ray to store logs and metadata.
    """

    def __init__(self, gcs_address, temp_dir):
        gcs_client_options = ray._raylet.GcsClientOptions.from_gcs_address(gcs_address)
        self.gcs_address = gcs_address

        ray._private.state.state._initialize_global_state(gcs_client_options)
        self.temp_dir = temp_dir
        self.default_service_discovery_flush_period = 5
        super().__init__()

    def get_file_discovery_content(self):
        """Return the content for Prometheus service discovery."""
        nodes = ray.nodes()
        metrics_export_addresses = [
            "{}:{}".format(node["NodeManagerAddress"], node["MetricsExportPort"])
            for node in nodes
            if node["alive"] is True
        ]
        gcs_client = GcsClient(address=self.gcs_address)
        autoscaler_addr = gcs_client.internal_kv_get(b"AutoscalerMetricsAddress", None)
        if autoscaler_addr:
            metrics_export_addresses.append(autoscaler_addr.decode("utf-8"))
        dashboard_addr = gcs_client.internal_kv_get(b"DashboardMetricsAddress", None)
        if dashboard_addr:
            metrics_export_addresses.append(dashboard_addr.decode("utf-8"))
        return json.dumps(
            [{"labels": {"job": "ray"}, "targets": metrics_export_addresses}]
        )

    def write(self):
        # Write a file based on https://prometheus.io/docs/guides/file-sd/
        # Write should be atomic. Otherwise, Prometheus raises an error that
        # json file format is invalid because it reads a file when
        # file is re-written. Note that Prometheus still works although we
        # have this error.
        temp_file_name = self.get_temp_file_name()
        with open(temp_file_name, "w") as json_file:
            json_file.write(self.get_file_discovery_content())
        # NOTE: os.replace is atomic on both Linux and Windows, so we won't
        # have race condition reading this file.
        os.replace(temp_file_name, self.get_target_file_name())

    def get_target_file_name(self):
        return os.path.join(
            self.temp_dir, ray._private.ray_constants.PROMETHEUS_SERVICE_DISCOVERY_FILE
        )

    def get_temp_file_name(self):
        return os.path.join(
            self.temp_dir,
            "{}_{}".format(
                "tmp", ray._private.ray_constants.PROMETHEUS_SERVICE_DISCOVERY_FILE
            ),
        )

    def run(self):
        while True:
            # This thread won't be broken by exceptions.
            try:
                self.write()
            except Exception as e:
                logger.warning(
                    "Writing a service discovery file, {},"
                    "failed.".format(self.get_target_file_name())
                )
                logger.warning(traceback.format_exc())
                logger.warning(f"Error message: {e}")
            time.sleep(self.default_service_discovery_flush_period)
