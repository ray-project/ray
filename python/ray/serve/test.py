import ray
from ray import serve

def f():
    pass

client = serve.start(http_port=8003, detached=True)
client.create_backend("backend", f)
client.create_endpoint("endpoint", backend="backend")

client.shutdown()

def check_dead():
    for actor_name in [
            client._controller_name,
    ]:
        try:
            print("getting", actor_name)
            ray.get_actor(actor_name)
            return False
        except ValueError:
            pass
    return True

while not check_dead():
    import time;time.sleep(0.1)
