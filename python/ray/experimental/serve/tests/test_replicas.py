import time

import ray
from ray.experimental import serve

def test_scaling(serve_instance):
    class WhoAmI:
        def __call__(self,_):
            return getattr(self, "_ray_serve_replica_id", "unknown")
    
    # Setup
    serve.create_endpoint("scaling", "/scaling")
    serve.create_backend(WhoAmI, "scaling-backend:v1")
    serve.link("scaling", "scaling-backend:v1")

    # Test basic function
    handle = serve.get_handle("scaling")
    replica_id = ray.get(handle.remote())
    assert replica_id == 0

    # Test scaling up
    serve.set_replica("scaling-backend:v1", 2)
    time.sleep(0.5) # wait for the second replica to come up
    replica_ids = ray.get([handle.remote() for _ in range(10)])
    assert set(replica_ids) == {0, 1}

    # Test scaling down to 0
    serve.set_replica("scaling-backend:v1", 0)
    time.sleep(0.5) # wait for the two replicas to be removed
    blocked_return = handle.remote()
    ready, not_ready = ray.wait([blocked_return], 1, timeout=0.1)
    assert len(not_ready) == 1, print(ray.get(ready))