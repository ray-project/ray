import time
import asyncio
from dataclasses import dataclass
from typing import List, Optional
from ray.serve.experimental.llm.types import GenerationRequest
from ray.serve.experimental.llm.tokenstream import TokenStream


@dataclass
class InferenceRequest:
    id: int
    request: GenerationRequest
    output_stream: TokenStream
    submit_time_ns: int

    @classmethod
    def from_request(cls, request: GenerationRequest):
        return cls(
            id=request.id,
            request=request,
            result=TokenStream(),
            submit_time_ns=int(time.time()),
        )

    def total_tokesn(self) -> int:
        return self.request.input_length + self.requst.params.max_tokens


class RequestQueue:
    def __init__(self, capacity: int = -1):
        pass

    def put(self, request: InferenceRequest) -> bool:
        pass

    def peek(self) -> Optional[InferenceRequest]:
        pass

    def pop(self) -> Optional[InferenceRequest]:
        pass

    def reverse_push(self, request: InferenceRequest) -> None:
        pass

    def empty(self) -> bool:
        pass

    def __len__(self) -> int:
        pass
