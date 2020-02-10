
import redis
import ray
import json
from ray.dashboard.closed_source.anyscale_server import NODE_INFO_CHANNEL, RAY_INFO_CHANNEL

"""Test script to see if Redis subscription works as expected

TODO(sang):Remove this file.
"""

redis_client = redis.StrictRedis(host="127.0.0.1", port=6379)
p = redis_client.pubsub(ignore_subscribe_messages=True)

p.psubscribe(NODE_INFO_CHANNEL)
print("NodeStats: subscribed to {}".format(NODE_INFO_CHANNEL))

p.psubscribe(RAY_INFO_CHANNEL)
print("NodeStats: subscribed to {}".format(RAY_INFO_CHANNEL))

for x in p.listen():
    channel = ray.utils.decode(x["channel"])
    data = x["data"]
    data = json.loads(ray.utils.decode(data))
    print(data)
    print(channel)