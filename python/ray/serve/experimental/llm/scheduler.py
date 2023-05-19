import asyncio
import logging
from abc import ABC, abstractmethod
from typing import List, Tuple, Optional
from threading import Thread
from transformers import AutoTokenizer
from ray.serve.experimental.llm.worker import InferenceWorker
from ray.serve.experimental.llm.types import (
    SamplingParams,
    GenerationRequest,
    Generation,
)
from ray.serve.experimental.llm.tokenstream import TokenStream
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


class InferenceScheduler:
    def __init__(
        self,
        tokenizer: Tokenizer,
        inference_worker: InferenceWorker,
        request_selection_policy: RequestSelectionPolicy,
        request_queue: RequestQueue,
        loop: asyncio.AbstractEventLoop,
    ):
        self._tokenizer = tokenizer
        self._inference_worker = inference_worker
        self._request_selection_policy = request_selection_policy
        self._request_queue = request_queue
        self._loop = loop
        self._thread = Thread(target=self._run_scheduling_loop)
        self._thread.start()

    def process_request(
        self, input_text: str, params: SamplingParams, max_length: int = 1024
    ) -> TokenStream:
        request = GenerationRequest(
            id=get_request_id(),
            input_text=input_text,
            max_length=max_length,
            input_length=self._tokenizer.get_input_length(input_text, max_length),
            sampling_params=params,
        )
        return self._add_request(request)

    def _add_request(self, request: GenerationRequest) -> TokenStream:
        pending_request = InferenceRequest.from_request(request, self._loop)
        self._request_queue.push(pending_request)
        return pending_request.output_stream

    def _run_scheduling_loop(self):
        """Schedule requests to be processed by the inference worker."""

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
        while True:
            # select new requests to process.
            logger.debug("select new requests to process")
            new_requests = self._select_new_requests(in_process_requests)
            logger.debug(f"requests selected {[r.id for r in new_requests]}")
            new_batch_id, new_unfinished_requests = self._process_new_requests(
                new_requests
            )
            logger.debug(f"request proccessed {[r.id for r in new_requests]}")
            # combine new batch with existing batch to generate next token.
            batch_id, in_process_requests = self._generate_next_token(
                [batch_id, new_batch_id], in_process_requests + new_unfinished_requests
            )
            logger.debug(f"token generated")

    def _select_new_requests(
        self,
        in_process_requests: List[InferenceRequest],
    ) -> List[InferenceRequest]:
        if len(in_process_requests) == 0 and self._request_queue.empty():
            # if there is no in-process requests and no new requests in the queue,
            # wait for new requests to arrive in the queue.
            self._request_queue.wait()

        return self._request_selection_policy.select_new_requests(
            in_process_requests, self._request_queue
        )

    def _process_new_requests(
        self, requests: List[InferenceRequest]
    ) -> Tuple[int, List[InferenceRequest]]:
        if len(requests) == 0:
            return None, []
        generations, batch_id = self._inference_worker.process_new_batch(
            [r.request for r in requests]
        )
        requests = self._process_generation_result(generations, requests)
        batch_id = self._inference_worker.filter_requests(
            batch_id, [r.id for r in requests]
        )
        return batch_id, requests

    def _generate_next_token(
        self, batch_ids: List[int], requests: List[InferenceRequest]
    ) -> Tuple[Optional[int], List[Generation]]:
        logger.debug(f"generating tokesn for batch {batch_ids}")
        generations, batch_id = self._inference_worker.generate_next_token(
            batch_ids,
        )
        requests = self._process_generation_result(generations, requests)

        if batch_id is not None:
            batch_id = self._inference_worker.filter_requests(
                batch_id, [r.id for r in requests]
            )
        else:
            assert len(requests) == 0, "expect no requests left"
        return batch_id, requests

    def _process_generation_result(
        self, generations: List[Generation], requests: List[InferenceRequest]
    ) -> List[InferenceRequest]:
        unfinished_requests = []
        for i, generation in enumerate(generations):
            assert (
                requests[i].id == generation.request_id
            ), f"expect request id {requests[i].id} but got {generation.request_id}"
            logger.debug(f"processing generation {generation}")
            requests[i].output_stream.put(generation)
            if generation.stopped:
                logger.info(f" {requests[i].id} finished")
                requests[i].output_stream.end()
            else:
                unfinished_requests.append(requests[i])
        return unfinished_requests
