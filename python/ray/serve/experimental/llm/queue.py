from typing import List
from ray.serve.experimental.llm.types import GenerationRequest

class PendingRequestQueue:
    def __init__(self):
        pass

    def append(self, request: GenerationRequest):
        pass

    def select_requests(self) -> List[GenerationRequest]:
        pass