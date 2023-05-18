import ray
from typing import List
from ray.serve.experimental.llm.worker import InferenceWorker
from ray.serve.experimental.llm.types import SamplingParams


class InferenceScheduler:
    def __init__(self, inference_worker: InferenceWorker):
        self._inference_worker = inference_worker
        self._request_queue = []

    async def process_request(self, input_text: str, params: SamplingParams) -> str:
        pass

    async def add_request(self):
        pass

    async def next(self):
        pass
