import asyncio
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Tuple, Optional
from threading import Thread, Lock
from transformers import AutoTokenizer
from dataclasses import dataclass
from ray.serve.experimental.llm.worker import InferenceWorker
from ray.serve.experimental.llm.types import (
    SamplingParams,
    GenerationRequest,
    Generation,
)
from ray.serve.experimental.llm.tokenstream import FakeTokenStream
from ray.serve.experimental.llm.queue import RequestQueue, InferenceRequest
from ray.serve.experimental.llm.policy import RequestSelectionPolicy

logger = logging.getLogger(__name__)

_request_id = 0


def get_request_id() -> int:
    # TODO: more robust request id generation.
    global _request_id
    _request_id += 1
    return _request_id


class Tokenizer(ABC):
    @abstractmethod
    def get_input_length(self, input_text: str, max_length: int) -> int:
        raise NotImplementedError("")


class NaiveTokenizer(Tokenizer):
    def get_input_length(self, input_text: str, max_length: int) -> int:
        return min(input_text.count(" ") + 1, max_length)

    # TODO: add model specific tokenizer


class TransfomerTokenizer(Tokenizer):
    def __init__(self, pad_token_id=50256, *args, **kwargs):
        self._tokenizer = AutoTokenizer.from_pretrained(*args, **kwargs)
        self._tokenizer.pad_token_id = pad_token_id

    def get_input_length(self, input_text: str, max_length: int) -> int:
        return self._tokenizer(
            text=input_text,
            return_tensors="pt",
            padding=True,
            return_token_type_ids=False,
            truncation=True,
            max_length=max_length,
        )["input_ids"].shape[1]


@dataclass
class Stats:
    num_requests_processed: int = 0
    num_active_requests: int = 0
    num_finished_requests: int = 0
    num_tokens_generated: int = 0
    num_input_tokens: int = 0
    num_iterations: int = 0
    last_report_time: float = 0.0
    start_time: float = 0.0

    def report_stats(self):
        if time.time() - self.last_report_time < 1:
            return False
        self.last_report_time = time.time()
        print(f"scheduler stats: {self}")
        elapsed = self.last_report_time - self.start_time
        token_s = (self.num_input_tokens + self.num_tokens_generated) / elapsed
        print(f"elapsed: {elapsed}, generated_tokens/s: {token_s}")
        return True

    def request_selected(self, requests: List[InferenceRequest]):
        self.num_active_requests += len(requests)
        self.num_requests_processed += len(requests)
        self.num_input_tokens += sum([r.request.input_length for r in requests])

    def request_finished(self):
        self.num_active_requests -= 1
        self.num_finished_requests += 1

    def token_generated(self, num):
        self.num_tokens_generated += num

    def iteration_finished(self):
        self.num_iterations += 1

    def start(self):
        self.start_time = time.time()


class InferenceScheduler:
    def __init__(
        self,
        tokenizer: Tokenizer,
        inference_worker_loader,
        request_selection_policy: RequestSelectionPolicy,
        request_queue: RequestQueue,
        loop: asyncio.AbstractEventLoop,
        inline: bool = False,
    ):
        self._tokenizer = tokenizer
        self._request_selection_policy = request_selection_policy
        self._inference_worker_loader = inference_worker_loader
        self._request_queue = request_queue
        self._loop = loop
        self._lock = Lock()
        self._stop = False
        self._stats = Stats()
        if not inline:
            self._thread = Thread(target=self._run_scheduling_loop)
            self._thread.start()

    def stop(self):
        with self._lock:
            self._stop = True
        self._thread.join()

    def is_stopped(self) -> bool:
        with self._lock:
            return self._stop

    def process_request(
        self, input_text: str, params: SamplingParams, max_length: int = 1024
    ) -> FakeTokenStream:
        request = GenerationRequest(
            id=get_request_id(),
            input_text=input_text,
            max_length=max_length,
            input_length=self._tokenizer.get_input_length(input_text, max_length),
            sampling_params=params,
        )
        return self._add_request(request)

    def _add_request(self, request: GenerationRequest) -> FakeTokenStream:
        pending_request = InferenceRequest.from_request(request, self._loop)
        self._request_queue.push(pending_request)
        return pending_request.output_stream

    def _run_scheduling_loop(self):
        """Schedule requests to be processed by the inference worker."""
        # start work the in the scheduling loop to avoid GPU memory leak.
        self._inference_worker = self._inference_worker_loader()
        self._stats.start()

        # The main schedule loop:
        #
        # 0. start with empty in-process requests.
        #
        # 1. select new requests to process, based
        # on the current in-process requests. send them to the inference worker.
        #
        # 2. for both new and in-process requests, combine them
        # and generate the next token. filter out finished requests.
        #
        # 3. goto step 1.
        batch_id = None
        in_process_requests = []
        while not self.is_stopped():
            # select new requests to process.
            new_requests = self._select_new_requests(in_process_requests)
            new_batch_id, new_unfinished_requests = self._process_new_requests(
                new_requests
            )

            # combine new batch with existing batch to generate next token.
            batch_id, in_process_requests = self._generate_next_token(
                [batch_id, new_batch_id], in_process_requests + new_unfinished_requests
            )
            self._stats.iteration_finished()
            self._report_stats()

    def _report_stats(self):
        if self._stats.report_stats():
            self._inference_worker.report_stats()

    def _select_new_requests(
        self,
        in_process_requests: List[InferenceRequest],
    ) -> List[InferenceRequest]:
        while (
            len(in_process_requests) == 0
            and self._request_queue.empty()
            and not self.is_stopped()
        ):
            # if there is no in-process requests and no new requests in the queue,
            # wait for new requests to arrive in the queue.
            self._request_queue.wait(1)

        requests = self._request_selection_policy.select_new_requests(
            in_process_requests, self._request_queue
        )
        self._stats.request_selected(requests)
        return requests

    def _process_new_requests(
        self, requests: List[InferenceRequest]
    ) -> Tuple[int, List[InferenceRequest]]:
        if len(requests) == 0:
            return None, []
        generations, batch_id = self._inference_worker.process_new_batch(
            [r.request for r in requests]
        )
        requests, need_filter = self._process_generation_result(generations, requests)

        if need_filter:
            batch_id = self._inference_worker.filter_requests(
                batch_id, [r.id for r in requests]
            )
        return batch_id, requests

    def _generate_next_token(
        self, batch_ids: List[int], requests: List[InferenceRequest]
    ) -> Tuple[Optional[int], List[Generation]]:
        generations, batch_id = self._inference_worker.generate_next_token(
            batch_ids,
        )
        requests, need_filter = self._process_generation_result(generations, requests)

        if batch_id is not None:
            if need_filter:
                batch_id = self._inference_worker.filter_requests(
                    batch_id, [r.id for r in requests]
                )
        else:
            assert len(requests) == 0, "expect no requests left"
        return batch_id, requests

    def _process_generation_result(
        self, generations: List[Generation], requests: List[InferenceRequest]
    ) -> Tuple[List[InferenceRequest], bool]:
        some_request_finished = False
        unfinished_requests = []
        self._stats.token_generated(len(generations))
        for i, generation in enumerate(generations):
            assert (
                requests[i].id == generation.request_id
            ), f"expect request id {requests[i].id} but got {generation.request_id}"
            requests[i].output_stream.put(generation.token_text)
            if generation.stopped:
                self._stats.request_finished()
                requests[i].output_stream.put(generation.generated_text.text)
                requests[i].output_stream.end()
                some_request_finished = True
            else:
                unfinished_requests.append(requests[i])
        return unfinished_requests, some_request_finished
