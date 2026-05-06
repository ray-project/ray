import asyncio
import time

from ray import serve


@serve.deployment
class SimulatedMLInference:
    """Deployment simulating ML model loading and inference."""

    def __init__(self):
        time.sleep(120)

    async def __call__(self, request):
        _ = await asyncio.wait_for(request.json(), timeout=60)
        time.sleep(0.015)
        return {"ok": True}


# Unique deployment name while sharing the same class.
app_1 = SimulatedMLInference.options(name="SimulatedMLInference_app_1").bind()
app_2 = SimulatedMLInference.options(name="SimulatedMLInference_app_2").bind()
app_3 = SimulatedMLInference.options(name="SimulatedMLInference_app_3").bind()
app_4 = SimulatedMLInference.options(name="SimulatedMLInference_app_4").bind()
app_5 = SimulatedMLInference.options(name="SimulatedMLInference_app_5").bind()
app_6 = SimulatedMLInference.options(name="SimulatedMLInference_app_6").bind()
app_7 = SimulatedMLInference.options(name="SimulatedMLInference_app_7").bind()
app_8 = SimulatedMLInference.options(name="SimulatedMLInference_app_8").bind()
