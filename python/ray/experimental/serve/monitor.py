from collections import defaultdict
import time

import ray
from ray.experimental.serve.utils import logger

@ray.remote
class MetricMonitor:
    """A simple metric aggregator that pulls metrics from registered actors.

    Example:
    >>> monitor = MetricMonitor.remote()
    >>> start_monitor_loop.remote(monitor)
    >>> monitor.add_target.remote(some_other_actor)
    >>> ray.get(monitor.get_stats.remote())
    {"some_other_actor.metric_name": [1.1, 1.0, 0.9, ...]}

    Note:
        The monitored actor is expected to implement the following interface:
        ```
        def health_check(self):
            return {
                "metric_name": metric_value_list,
                "metric_name": metric_value_list
            }
        ```
    """
    def __init__(self):
        self.backends_to_watch = dict()
        self.stats = defaultdict(list)
    
    def scrape(self):
        if not len(self.backends_to_watch):
            return

        aggregated_result = ray.get([handle.health_check.remote() for handle in self.backends_to_watch.values()])
        for actor_result in aggregated_result:
            for metric_name, metric_values in actor_result.items():
                if isinstance(metric_values, list):
                    self.stats[metric_name].extend(metric_values)
                else:
                    self.stats[metric_name].append(metric_values)
        
    def add_target(self, handle):
        # HACK: we use _actor_id because two actor handles pointing 
        # to the same actor are not equal in python.
        self.backends_to_watch[handle._actor_id] = handle

    def remove_target(self, handle):
        self.backends_to_watch.pop(handle._actor_id)

    def get_stats(self):
        return self.stats

@ray.remote(num_cpus=0)
def start_monitor_loop(monitor_actor_handle, interval_s=2):
    while True:
        ray.get(monitor_actor_handle.scrape.remote())
        time.sleep(interval_s)
        
