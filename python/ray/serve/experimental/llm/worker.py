from collections import defaultdict
from typing import List, Callable, Dict, Tuple, Optional

from ray.serve.experimental.llm.models.model import Model
from ray.serve.experimental.llm.types import Generation, GenerationRequest


class InferenceWorker:
    def __init__(self, model_loader: Callable[[], Model]):
        self._model = model_loader()
        self._batch_state_cache = dict()
        self._batch_counter = 0

    def get_next_batch_id(self) -> int:
        batch_id = self._batch_counter
        self._batch_counter += 1
        return batch_id

    async def process_new_batch(self, requests: List[GenerationRequest]) -> Tuple[List[Generation], int]:
        batch_state = self._model.create_batch_state(requests)
        batch_id = self.get_next_batch_id()
        generations, batch_state = self._model.generate_token(batch_state)
        self._batch_state_cache[batch_id] = batch_state
        return generations, batch_id

    async def generate_next_token(self, batch_ids: List[int]) -> Tuple[List[Generation], int]:
        if len(batch_ids) == 0:
            raise ValueError("Must provide at least one batch")
        batch_states = []
        for batch_id in batch_ids:
            batch_state = self._batch_state_cache.pop(batch_id)
            if batch_state is None:
                raise ValueError(f"Batch ID {batch_id} not found in cache.")
            batch_states.append(batch_state)

        if len(batch_states) > 1:
            batch_state = self._model.concatenate_batch_stats(batch_states)
        else:
            batch_state = batch_states[0]

        generations, batch_state = self._model.generate_token(batch_state)
        new_batch_id = self.get_next_batch_id()
        self._batch_state_cache[new_batch_id] = batch_state
        return generations, new_batch_id

    async def delete_finished_requests(self, batch_id: int, request_ids: List[int]) -> Optional[int]:
        batch_state = self._batch_state_cache.pop(batch_id)
        filtered = batch_state.filter(request_ids)
        if len(filtered):
            next_batch_id = self.get_next_batch_id()
            self._batch_state_cache[next_batch_id] = filtered
            return next_batch_id
        return None
        