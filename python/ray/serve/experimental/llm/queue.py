import time
import asyncio
from collections import deque
from dataclasses import dataclass
from threading import RLock, Condition
from typing import List, Optional
from ray.serve.experimental.llm.types import GenerationRequest
from ray.serve.experimental.llm.tokenstream import FakeTokenStream, Event_ts


@dataclass
class InferenceRequest:
    id: int
    request: GenerationRequest
    output_stream: FakeTokenStream
    submit_time_ns: int

    @classmethod
    def from_request(cls, request: GenerationRequest, loop: asyncio.AbstractEventLoop, event: Event_ts):
        return cls(
            id=request.id,
            request=request,
            output_stream=FakeTokenStream(loop=loop, event=event),
            submit_time_ns=int(time.time()),
        )

    def total_tokens(self) -> int:
        return self.request.input_length + self.request.sampling_params.max_new_tokens


class RequestQueue:
    def __init__(self):
        self._queue = deque()
        self._lock = RLock()
        self._cv = Condition(self._lock)

    def push(self, request: InferenceRequest) -> bool:
        with self._cv:
            self._queue.append(request)
            self._cv.notify_all()
            return True

    def peek(self) -> Optional[InferenceRequest]:
        with self._lock:
            if len(self._queue) == 0:
                return None
            return self._queue[0]

    def pop(self) -> Optional[InferenceRequest]:
        with self._lock:
            while len(self._queue) == 0:
                return None
            return self._queue.popleft()

    def wait(self, timeout=None):
        start = time.time()
        with self._cv:
            while len(self._queue) == 0:
                self._cv.wait(timeout)
                if timeout is not None and time.time() - start >= timeout:
                    return

    def reverse_push(self, request: InferenceRequest) -> None:
        with self._cv:
            self._queue.appendleft(request)
            self._cv.notify_all()

    def empty(self) -> bool:
        with self._lock:
            return len(self._queue) == 0

    def __len__(self) -> int:
        with self._lock:
            return len(self._queue)
