import asyncio

import ray
from ray import serve

ray.util.connect("localhost:61234")

ray.get_runtime_context().node_id
