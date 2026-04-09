import asyncio
import time

from ray import serve


@serve.deployment
class SimulatedMLInference:
    """Deployment simulating ML model loading and inference."""

    def __init__(self):
        time.sleep(120)

    async def __call__(self, request):
        _ = await asyncio.wait_for(request.json(), timeout=2)
        time.sleep(0.015)
        return {"ok": True}


app = SimulatedMLInference.bind()
