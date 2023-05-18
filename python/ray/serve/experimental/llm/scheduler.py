import ray
from typing import List
from ray.serve.experimental.llm.inference_worker import InferenceWorker
from ray.serve.experimental.llm.types import SamplingParams

class InfernceScheduler:
    def __init__(self, inference_worker: InferenceWorker):
        self._inference_worker = inference_worker

    async def process_request(self, input_text: str, params: SamplingParams) -> str:
        pass

    async def add_request()