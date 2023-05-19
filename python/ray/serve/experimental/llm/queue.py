import time
import asyncio
from collections import deque
from dataclasses import dataclass
from threading import Lock
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
    def __init__(self):
        self._queue = deque()
        self._lock = Lock()

    def put(self, request: InferenceRequest) -> bool:
        with self._lock:
            self._queue.append(request)
            return True

    def peek(self) -> Optional[InferenceRequest]:
        with self._lock:
            if len(self._queue) == 0:
                return None
            return self._queue[0]

    def pop(self) -> Optional[InferenceRequest]:
        with self._lock:
            if len(self._queue) == 0:
                return None
            return self._queue.popleft()

    def reverse_push(self, request: InferenceRequest) -> None:
        with self._lock:
            self._queue.appendleft(request)
            return True

    def empty(self) -> bool:
        with self._lock:
            return len(self._queue) == 0

    def __len__(self) -> int:
        with self._lock:
            return len(self._queue)
