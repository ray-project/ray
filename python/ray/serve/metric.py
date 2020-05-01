import time

import numpy as np
import pandas as pd

import ray


@ray.remote(num_cpus=0)
class MetricMonitor:
    def __init__(self, gc_window_seconds=3600):
        """Metric monitor scrapes metrics from ray serve actors
        and allow windowed query operations.

        Args:
            gc_window_seconds(int): How long will we keep the metric data in
                memory. Data older than the gc_window will be deleted.
        """
        #: Mapping actor ID (hex) -> actor handle
        self.actor_handles = dict()

        self.data_entries = []

        self.gc_window_seconds = gc_window_seconds
        self.latest_gc_time = time.time()

    def add_target(self, target_handle):
        hex_id = target_handle._actor_id.hex()
        self.actor_handles[hex_id] = target_handle

    def remove_target(self, target_handle):
        hex_id = target_handle._actor_id.hex()
        self.actor_handles.pop(hex_id)

    def scrape(self):
        # If expected gc time has passed, we will perform metric value GC.
        expected_gc_time = self.latest_gc_time + self.gc_window_seconds
        if expected_gc_time < time.time():
            self._perform_gc()
            self.latest_gc_time = time.time()

        curr_time = time.time()
        result = [
            handle.get_metrics.remote()
            for handle in self.actor_handles.values()
        ]
        # TODO(simon): handle the possibility that an actor_handle is removed
        for handle_result in ray.get(result):
            for metric_name, metric_info in handle_result.items():
                data_entry = {
                    "retrieved_at": curr_time,
                    "name": metric_name,
                    "type": metric_info["type"],
                }

                if metric_info["type"] == "counter":
                    data_entry["value"] = metric_info["value"]
                    self.data_entries.append(data_entry)

                elif metric_info["type"] == "list":
                    for metric_value in metric_info["value"]:
                        new_entry = data_entry.copy()
                        new_entry["value"] = metric_value
                        self.data_entries.append(new_entry)

    def _perform_gc(self):
        curr_time = time.time()
        earliest_time_allowed = curr_time - self.gc_window_seconds

        # If we don"t have any data at hand, no need to gc.
        if len(self.data_entries) == 0:
            return

        df = pd.DataFrame(self.data_entries)
        df = df[df["retrieved_at"] >= earliest_time_allowed]
        self.data_entries = df.to_dict(orient="record")

    def _get_dataframe(self):
        return pd.DataFrame(self.data_entries)

    def collect(self,
                percentiles=[50, 90, 95],
                agg_windows_seconds=[10, 60, 300, 600, 3600]):
        """Collect and perform aggregation on all metrics.

        Args:
            percentiles(List[int]): The percentiles for aggregation operations.
                Default is 50th, 90th, 95th percentile.
            agg_windows_seconds(List[int]): The aggregation windows in seconds.
                The longest aggregation window must be shorter or equal to the
                gc_window_seconds.
        """
        result = {}
        df = pd.DataFrame(self.data_entries)

        if len(df) == 0:  # no metric to report
            return {}

        # Retrieve the {metric_name -> metric_type} mapping
        metric_types = df[["name",
                           "type"]].set_index("name").squeeze().to_dict()

        for metric_name, metric_type in metric_types.items():
            if metric_type == "counter":
                result[metric_name] = df.loc[df["name"] == metric_name,
                                             "value"].tolist()[-1]
            if metric_type == "list":
                result.update(
                    self._aggregate(metric_name, percentiles,
                                    agg_windows_seconds))
        return result

    def _aggregate(self, metric_name, percentiles, agg_windows_seconds):
        """Perform aggregation over a metric.

        Note:
            This metric must have type `list`.
        """
        assert max(agg_windows_seconds) <= self.gc_window_seconds, (
            "Aggregation window exceeds gc window. You should set a longer gc "
            "window or shorter aggregation window.")

        curr_time = time.time()
        df = pd.DataFrame(self.data_entries)
        filtered_df = df[df["name"] == metric_name]
        if len(filtered_df) == 0:
            return dict()

        data_types = filtered_df["type"].unique().tolist()
        assert data_types == [
            "list"
        ], ("Can't aggreagte over non-list type. {} has type {}".format(
            metric_name, data_types))

        aggregated_metric = {}
        for window in agg_windows_seconds:
            earliest_time = curr_time - window
            windowed_df = filtered_df[
                filtered_df["retrieved_at"] > earliest_time]
            percentile_values = np.percentile(windowed_df["value"],
                                              percentiles)
            for percentile, value in zip(percentiles, percentile_values):
                result_key = "{name}_{perc}th_perc_{window}_window".format(
                    name=metric_name, perc=percentile, window=window)
                aggregated_metric[result_key] = value

        return aggregated_metric


@ray.remote(num_cpus=0)
def start_metric_monitor_loop(monitor_handle, duration_s=5):
    while True:
        time.sleep(duration_s)
        try:
            ray.get(monitor_handle.scrape.remote())
        except ray.exceptions.RayActorError:
            pass
