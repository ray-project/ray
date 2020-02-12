import argparse
import json
import logging
import random
import redis
import time

# TODO(sang): Add this module to requirements
from prometheus_client import start_http_server, Gauge

import ray

from ray.dashboard.closed_source.ingest_server \
    import NODE_INFO_CHANNEL, RAY_INFO_CHANNEL

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


if __name__ == '__main__':
    redis_client = redis.StrictRedis(host="127.0.0.1", port=6379)
    p = redis_client.pubsub(ignore_subscribe_messages=True)
    p.psubscribe(RAY_INFO_CHANNEL)
    p.psubscribe(NODE_INFO_CHANNEL)
    metrics_cpu_usage = Gauge("ray_metrics_cpu_usage", "CPU usage of pid residing in a host.", labelnames=("pid", "host", "ip"))
    metrics_mem_usage = Gauge("ray_metrics_mem_usage", "Memory usage of pid residing in a host.", labelnames=("pid", "host", "ip"))

    # Start up the server to expose the metrics.
    host, port = '127.0.0.1', 8000
    logger.info('Server listening on port {} at address {}'.format(host, port))
    start_http_server(port)

    # Read data from Redis.
    for x in p.listen():
        data = x["data"]
        channel = ray.utils.decode(x["channel"])
        data = json.loads(ray.utils.decode(data))
        print(channel)
        print(data)

        if channel == NODE_INFO_CHANNEL:
            clients = data['clients']
            for client in clients:
                host = client['hostname']
                ip = client['ip']
                workers = client['workers']

                for worker in workers:
                    pid = worker['pid']
                    cpu_usage = worker['cpu_percent']
                    mem_usage = worker['memory_info']['rss']
                    metrics_cpu_usage.labels(pid=pid, host=host, ip=ip).set(cpu_usage)
                    metrics_mem_usage.labels(pid=pid, host=host, ip=ip).set(mem_usage)
