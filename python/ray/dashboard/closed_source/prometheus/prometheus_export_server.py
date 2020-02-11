from prometheus_client import start_http_server, Gauge
import random
import time
import json
import redis
import ray
from ray.dashboard.closed_source.ingest_server import NODE_INFO_CHANNEL, RAY_INFO_CHANNEL

if __name__ == '__main__':
    redis_client = redis.StrictRedis(host="127.0.0.1", port=6379)
    p = redis_client.pubsub(ignore_subscribe_messages=True)
    p.psubscribe(RAY_INFO_CHANNEL)
    p.psubscribe(NODE_INFO_CHANNEL)
    g = Gauge("mamam", "CPU usage of pid", labelnames=("pid", "host", "ip"))
    # Start up the server to expose the metrics.
    start_http_server(8000)
    # Generate some requests.
    for x in p.listen():
        data = x["data"]
        channel = ray.utils.decode(x["channel"])
        data = json.loads(ray.utils.decode(data))
        # print(channel)
        # print(data)

        if channel == NODE_INFO_CHANNEL:
            clients = data['clients']
            for client in clients:
                print(client)
                host = client['hostname']
                ip = client['ip']
                workers = client['workers']

                for worker in workers:
                    pid = worker['pid']
                    cpu_usage = worker['cpu_percent']
                    g.labels(pid=pid, host=host, ip=ip).set(cpu_usage)
