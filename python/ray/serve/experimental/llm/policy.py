import asyncio
from abc import ABC, abstractmethod
from collections import namedtuple
from typing import List
from ray.serve.experimental.llm.queue import InferenceRequest, RequestQueue


class RequestSelectionPolicy(ABC):
    @abstractmethod
    def select_new_requests(
        self, in_process_requests: List[InferenceRequest], queue: RequestQueue
    ) -> List[InferenceRequest]:
        raise NotImplementedError

    # TODO: we might also interested in other events, such as when a request is
    # finished, or when a token is generated.


Quota = namedtuple("Quota", ["min_num_requests", "token_budget"])


class QuotaBasedRequestSelectionPolicy(RequestSelectionPolicy):
    def __init__(
        self,
        max_batch_total_tokens: int = 32000,
        waiting_served_ratio: float = 1.2,
        max_waiting_tokens: int = 20,
    ):
        self.max_batch_total_tokens = max_batch_total_tokens
        self.waiting_served_ratio = waiting_served_ratio
        self.max_waiting_tokens = max_waiting_tokens

    def select_new_requests(
        self, in_process_requests: List[InferenceRequest], queue: RequestQueue
    ) -> List[InferenceRequest]:
        min_num_requests, token_budget = self.calculate_quota(in_process_requests)

        if min_num_requests and len(queue) < min_num_requests:
            return []

        results = []
        while not queue.empty():
            request = queue.peek()
            if request.total_tokens() >= token_budget:
                break
            results.append(request)
            queue.pop()
            token_budget -= request.total_tokens()

        if min_num_requests and len(results) < min_num_requests:
            for request in results:
                queue.reverse_push(request)
            return []
        return results

    def select_new_requests_asyncio_queue(
        self, in_process_requests: List[InferenceRequest], queue: asyncio.Queue
    ) -> List[InferenceRequest]:
        min_num_requests, token_budget = self.calculate_quota(in_process_requests)

        if min_num_requests and queue.qsize() < min_num_requests:
            return []

        hypothetical_results = []
        while len(hypothetical_results) < queue.qsize():
            request = queue._queue[0]
            if request.total_tokens() >= token_budget:
                break
            hypothetical_results.append(request)
            token_budget -= request.total_tokens()

        results = []
        if min_num_requests and len(hypothetical_results) < min_num_requests:
            results = []
        else:
            results = []
            for _ in hypothetical_results:
                results.append(queue.get_nowait())

        return results

    def calculate_quota(self, in_process_requests: List[InferenceRequest]) -> Quota:
        if not in_process_requests:
            return Quota(
                min_num_requests=None, token_budget=self.max_batch_total_tokens
            )

        batch_size = len(in_process_requests)

        # calculate minmal_new_requests to be served
        if len(in_process_requests) >= self.max_waiting_tokens:
            min_num_requests = 0
        else:
            min_num_requests = int(batch_size * self.waiting_served_ratio)

        # calculate token budget
        # TODO: we might want consider padding as well.
        # TODO: can we calculate the token budget based on the model?
        token_budget = max(
            0,
            self.max_batch_total_tokens
            - sum([r.total_tokens() for r in in_process_requests]),
        )
        return min_num_requests, token_budget


class StaticBatchPolicy(RequestSelectionPolicy):
    def __init__(
        self,
        batch_size: int,
    ):
        self.batch_size = batch_size

    def select_new_requests(
        self, in_process_requests: List[InferenceRequest], queue: RequestQueue
    ) -> List[InferenceRequest]:
        if in_process_requests:
            return []
        if len(queue) < self.batch_size:
            return []

        results = []
        while not queue.empty() and len(results) < self.batch_size:
            request = queue.peek()
            results.append(request)
            queue.pop()

        return results
