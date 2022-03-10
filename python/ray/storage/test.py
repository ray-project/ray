import ray
import json
import numpy as np

ray.init(
    storage="/tmp/qux",
    object_store_memory=100e6,
    _system_config={
        "object_spilling_config": json.dumps(
            {"type": "ray_storage", "params": {}},
        )
    },
)

fs = ray.storage.get_filesystem()
print(fs)


@ray.remote
def f():
    fs = ray.storage.get_filesystem()
    print(fs)


ray.get(f.remote())

refs = []
for _ in range(10):
    refs.append(ray.put(np.zeros(1024 * 1024 * 20)))
print("DONE")
