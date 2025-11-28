"""Utilities for fetching and parsing Prometheus metrics.

This module provides functions for fetching metrics from Prometheus endpoints
and parsing them into structured data. It's used by both test utilities and
Ray Serve components.
"""

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple, Union

import requests

from ray._private.worker import RayContext

try:
    from prometheus_client.core import Metric
    from prometheus_client.parser import Sample, text_string_to_metric_families
except (ImportError, ModuleNotFoundError):
    Metric = None
    Sample = None

    def text_string_to_metric_families(*args, **kwargs):
        raise ModuleNotFoundError("`prometheus_client` not found")


logger = logging.getLogger(__name__)


@dataclass
class PrometheusTimeseries:
    """A collection of timeseries from multiple addresses. Each timeseries is a
    collection of samples with the same metric name and labels. Concretely:
    - components_dict: a dictionary of addresses to the Component labels
    - metric_descriptors: a dictionary of metric names to the Metric object
    - metric_samples: the latest value of each label
    """

    components_dict: Dict[str, Set[str]] = field(default_factory=dict)
    metric_descriptors: Dict[str, Metric] = field(default_factory=dict)
    metric_samples: Dict[frozenset, Sample] = field(default_factory=dict)

    def flush(self):
        self.components_dict.clear()
        self.metric_descriptors.clear()
        self.metric_samples.clear()


def fetch_from_prom_server(
    address: str,
    query: str,
    *,
    time: Union[str, int, float] = None,
    timeout: float = None,
) -> Dict[str, Any]:
    """Query Prometheus server using PromQL query expression.

    Args:
        address: Hostname:Port of the prometheus server
        query: PromQL query expression.
        time: RFC3339 or Unix timestamp to use for the query. If None, current time is used.
        timeout: Query timeout in seconds. If None, uses Prometheus server default.

    Returns:
        Dict[str, Any]: Response data from Prometheus query API containing metrics.

    Raises:
        requests.exceptions.RequestException: If query fails or server is unreachable.
    """
    # Build query params
    params = {"query": query}
    if time is not None:
        params["time"] = time
    if timeout is not None:
        params["timeout"] = str(timeout)

    response = requests.get(
        f"http://{address}/api/v1/query",
        params=params,
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


def raw_metrics(info: RayContext) -> Dict[str, List[Any]]:
    """Return prometheus metrics from a RayContext

    Args:
        info: Ray context returned from ray.init()

    Returns:
        Dict from metric name to a list of samples for the metrics
    """
    metrics_page = "localhost:{}".format(info.address_info["metrics_export_port"])
    print("Fetch metrics from", metrics_page)
    return fetch_prometheus_metrics([metrics_page])


def raw_metric_timeseries(
    info: RayContext, result: PrometheusTimeseries
) -> Dict[str, List[Any]]:
    """Return prometheus timeseries from a RayContext"""
    metrics_page = "localhost:{}".format(info.address_info["metrics_export_port"])
    print("Fetch metrics from", metrics_page)
    return fetch_prometheus_metric_timeseries([metrics_page], result)


def fetch_from_prom_exporter(prom_addresses: List[str]) -> Iterator[Tuple[str, str]]:
    """Fetch raw prometheus metrics text from multiple exporter addresses.

    Args:
        prom_addresses: List of addresses (host:port) of Prometheus exporters to fetch
            the `/metrics` exposition text from.

    Yields:
        Tuple[str, str]: Iterator of (address, metrics_text) for each successful fetch.
    """
    for address in prom_addresses:
        try:
            response = requests.get(f"http://{address}/metrics")
            yield address, response.text
        except requests.exceptions.ConnectionError:
            continue


def fetch_prometheus(prom_addresses: List[str]) -> Tuple[str, str, str]:
    """Fetch and parse prometheus metrics from multiple addresses.

    Args:
        prom_addresses: List of addresses to fetch metrics from.

    Returns:
        Tuple of (components_dict, metric_descriptors, metric_samples)
    """
    components_dict = {}
    metric_descriptors = {}
    metric_samples = []

    for address in prom_addresses:
        if address not in components_dict:
            components_dict[address] = set()

    for address, response in fetch_from_prom_exporter(prom_addresses):
        for metric in text_string_to_metric_families(response):
            for sample in metric.samples:
                metric_descriptors[sample.name] = metric
                metric_samples.append(sample)
                if "Component" in sample.labels:
                    components_dict[address].add(sample.labels["Component"])
    return components_dict, metric_descriptors, metric_samples


def fetch_prometheus_timeseries(
    prom_addresses: List[str],
    result: PrometheusTimeseries,
) -> PrometheusTimeseries:
    """Fetch prometheus metrics and update a timeseries collection.

    Args:
        prom_addresses: List of addresses to fetch metrics from.
        result: PrometheusTimeseries object to update.

    Returns:
        Updated PrometheusTimeseries object.
    """
    components_dict, metric_descriptors, metric_samples = fetch_prometheus(
        prom_addresses
    )
    for address, components in components_dict.items():
        if address not in result.components_dict:
            result.components_dict[address] = set()
        result.components_dict[address].update(components)
    result.metric_descriptors.update(metric_descriptors)
    for sample in metric_samples:
        # update sample to the latest value
        result.metric_samples[
            frozenset(list(sample.labels.items()) + [("_metric_name_", sample.name)])
        ] = sample
    return result


def fetch_prometheus_metrics(prom_addresses: List[str]) -> Dict[str, List[Any]]:
    """Return prometheus metrics from the given addresses.

    Args:
        prom_addresses: List of metrics_agent addresses to collect metrics from.

    Returns:
        Dict mapping from metric name to list of samples for the metric.
    """
    _, _, samples = fetch_prometheus(prom_addresses)
    samples_by_name = defaultdict(list)
    for sample in samples:
        samples_by_name[sample.name].append(sample)
    return samples_by_name


def fetch_prometheus_metric_timeseries(
    prom_addresses: List[str], result: PrometheusTimeseries
) -> Dict[str, List[Any]]:
    samples = fetch_prometheus_timeseries(
        prom_addresses, result
    ).metric_samples.values()
    samples_by_name = defaultdict(list)
    for sample in samples:
        samples_by_name[sample.name].append(sample)
    return samples_by_name


def parse_prometheus_metrics_text(metrics_text: str) -> Dict[str, List[Sample]]:
    """Parse prometheus metrics text format into structured data.

    Args:
        metrics_text: Raw prometheus metrics text.

    Returns:
        Dict mapping from metric name to list of samples.
    """
    try:
        # Parse metrics and create a lookup by metric name
        metric_samples_by_name = {}
        for metric in text_string_to_metric_families(metrics_text):
            for sample in metric.samples:
                if sample.name not in metric_samples_by_name:
                    metric_samples_by_name[sample.name] = []
                metric_samples_by_name[sample.name].append(sample)
        return metric_samples_by_name
    except ImportError:
        logger.error("prometheus_client not available for parsing metrics")
        return {}
    except Exception as e:
        logger.error(f"Error parsing prometheus metrics: {e}")
        return {}


def filter_samples_by_label(
    samples: List[Sample], label_name: str, label_value: str
) -> List[Sample]:
    """Filter samples by a specific label value.

    Args:
        samples: List of prometheus samples to filter.
        label_name: Name of the label to filter by.
        label_value: Value the label must have.

    Returns:
        Filtered list of samples.
    """
    return [
        sample for sample in samples if sample.labels.get(label_name) == label_value
    ]


def extract_metric_values(
    metric_samples_by_name: Dict[str, List[Sample]],
    metric_names: List[str],
    filter_func: Optional[callable] = None,
) -> Dict[str, Any]:
    """Extract values for specific metrics from parsed prometheus data.

    Args:
        metric_samples_by_name: Parsed prometheus metrics data.
        metric_names: List of metric names to extract.
        filter_func: Optional function to filter samples.

    Returns:
        Dict mapping metric names to their values or filtered samples.
    """
    metrics_result = {}

    for metric_name in metric_names:
        if metric_name in metric_samples_by_name:
            samples = metric_samples_by_name[metric_name]
            if samples:
                if filter_func:
                    metrics_result[metric_name] = filter_func(samples)
                else:
                    # Return all samples if no filter
                    metrics_result[metric_name] = samples
            else:
                metrics_result[metric_name] = []
        else:
            logger.warning(f"Metric {metric_name} not found in exporter response")
            metrics_result[metric_name] = []

    return metrics_result
