import ray
from ray import serve


@serve.deployment(ray_actor_options={"num_cpus": 0})
class TelemetryReceiver:
    def __init__(self):
        self.storage = ray.get_actor(name="storage", namespace="serve")

    async def __call__(self, request) -> bool:
        report = await request.json()
        ray.get(self.storage.store_report.remote(report))
        return True


app = TelemetryReceiver.bind()
