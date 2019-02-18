from ray.serve.frontend import HTTPFrontendActor
import ray
import requests

ray.init(redis_address="172.31.46.235:54599")

a = HTTPFrontendActor.remote()
a.start.remote()

resp = requests.get("http://0.0.0.0:8000/hi")

