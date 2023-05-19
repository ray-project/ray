import time

import ray
import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import List, Tuple
from threading import Thread
from ray.serve.experimental.llm.worker import InferenceWorker
from ray.serve.experimental.llm.types import (
    SamplingParams,
    GenerationRequest,
    Generation,
)
from ray.serve.experimental.llm.tokenstream import TokenStream
from ray.serve.experimental.llm.queue import PendingRequestQueue


_request_id = 0


def get_request_id() -> int:
    global _request_id
    _request_id += 1
    return _request_id


@dataclass
class PendingRequest:
    """A request that has been submitted to the scheduler but not yet processed."""

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


class InferenceScheduler:
    def __init__(self, inference_worker: InferenceWorker):
        self._inference_worker = inference_worker
        self._request_queue = PendingRequestQueue()
        self._executor_loop = asyncio.new_event_loop()
        self._thread = Thread(target=self._run_executor_loop)
        self._thread.start()

    def process_request(self, input_text: str, params: SamplingParams) -> TokenStream:
        request = GenerationRequest(
            id=get_request_id(), input_text=input_text, params=params
        )
        return self._add_request(request)

    def _add_request(self, request: GenerationRequest) -> TokenStream:
        pending_request = PendingRequest.from_request(request)
        self._request_queue.append(pending_request)
        return pending_request.output_stream

    def _run_executor_loop(self):
        asyncio.set_event_loop(self._executor_loop)
        self._executor_loop.run_until_complete(self._schedule_request())

    async def _schedule_request(self):
        while True:
            await self._process_next_batch()

    async def _select_requests(
        in_process_requests: List[PendingRequest],
    ) -> List[PendingRequest]:
        pass

    async def _process_next_batch(self):
        requests = await self._select_requests([])
        current_batch_id, requests = self._process_new_batch(requests)

        while current_batch_id is not None:
            additional_requests = await self._select_requests(requests)
            additional_batch_id, additional_requests = self._process_new_batch(
                additional_requests
            )

            generations, current_batch_id = self._inference_worker.generate_next_token(
                [current_batch_id, additional_batch_id]
            )
            requests = self._process_generations(
                generations, requests + additional_requests
            )
            current_batch_id = self._inference_worker.filter_requests(
                current_batch_id, [r.id for r in requests]
            )

    def _process_new_batch(
        self, requests: List[PendingRequest]
    ) -> Tuple[int, List[PendingRequest]]:
        generations, batch_id = self._inference_worker.process_new_batch(requests)
        requests = self._process_generations(generations, requests)
        batch_id = self._inference_worker.filter_requests(
            batch_id, [r.id for r in requests]
        )
        return batch_id, requests

    def _generate_next_token(
        self, batch_ids: List[int], requests: List[PendingRequest]
    ) -> List[Generation]:
        generations, batch_id = self._inference_worker.generate_next_token(
            batch_ids,
        )
        requests = self._process_generations(generations, requests)
        batch_id = self._inference_worker.filter_requests(
            batch_id, [r.id for r in requests]
        )
        return batch_id, requests

    def _process_generations(
        self, generations: List[Generation], requests: List[PendingRequest]
    ) -> List[PendingRequest]:
        unfinished_requests = []
        for i, generation in enumerate(generations):
            assert (
                requests[i].id == generation.request_id
            ), f"expect request id {requests[i].id} but got {generation.request_id}"
            requests[i].output_stream.append(generation.token_text)
            if generation.is_finished:
                requests[i].output_stream.end()
            else:
                unfinished_requests.append(requests[i])
        return unfinished_requests
