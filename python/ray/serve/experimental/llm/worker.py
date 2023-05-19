from typing import List, Callable, Tuple, Optional

from ray.serve.experimental.llm.models.model import Model
from ray.serve.experimental.llm.types import Generation, GenerationRequest


class InferenceWorker:
    def __init__(self, model_loader: Callable[[], Model]):
        self._model = model_loader()
        self._batch_state_cache = dict()

    def process_new_batch(
        self, requests: List[GenerationRequest]
    ) -> Tuple[List[Generation], int]:
        batch_state = self._model.create_batch(requests)
        generations, batch_state = self._model.generate_token(batch_state)
        self._batch_state_cache[batch_state.id] = batch_state
        return generations, batch_state.id

    def generate_next_token(self, batch_ids: List[int]) -> Tuple[List[Generation], int]:
        if len(batch_ids) == 0:
            raise ValueError("Must provide at least one batch")
        batch_states = []
        for batch_id in batch_ids:
            batch_state = self._batch_state_cache.pop(batch_id)
            if batch_state is None:
                raise ValueError(f"Batch ID {batch_id} not found in cache.")
            batch_states.append(batch_state)

        if len(batch_states) > 1:
            batch_state = self._model.concatenate_batches(batch_states)
        else:
            batch_state = batch_states[0]

        generations, batch_state = self._model.generate_token(batch_state)
        self._batch_state_cache[batch_state.id] = batch_state
        return generations, batch_state.id

    def filter_requests(self, batch_id: int, request_ids: List[int]) -> Optional[int]:
        batch_state = self._batch_state_cache.pop(batch_id)

        if len(request_ids) == 0:
            return None

        filtered = batch_state.filter(request_ids)
        if len(filtered):
            self._batch_state_cache[filtered.id] = filtered
            return filtered.id

        return None
