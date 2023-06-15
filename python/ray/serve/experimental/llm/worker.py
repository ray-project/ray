import torch
import gc
from typing import List, Callable, Tuple, Optional

from ray.serve.experimental.llm.models.model import Model
from ray.serve.experimental.llm.types import Generation, GenerationRequest


class InferenceWorker:
    def __init__(self, model_loader: Callable[[], Model]):
        self._model = model_loader()
        self._batch_state_cache = dict()
        if self._model.device.type == "cuda":
            self._inference_mode_raii_guard = torch._C._InferenceMode(True)

    def process_new_batch(
        self, requests: List[GenerationRequest]
    ) -> Tuple[List[Generation], int]:
        batch_state = self._model.create_batch(requests)
        generations, batch_state = self._model.generate_token(batch_state)
        self._batch_state_cache[batch_state.id] = batch_state
        return generations, batch_state.id

    def generate_next_token(
        self, batch_ids: List[int]
    ) -> Tuple[List[Generation], Optional[int]]:
        if len(batch_ids) == 0:
            raise ValueError("Must provide at least one batch")
        batch_states = []
        for batch_id in batch_ids:
            if batch_id is None:
                continue
            batch_state = self._batch_state_cache.pop(batch_id, None)
            if batch_state is None:
                raise ValueError(f"Batch ID {batch_id} not found in cache.")
            batch_states.append(batch_state)

        if len(batch_states) == 0:
            return [], None

        if len(batch_states) > 1:
            batch_state = self._model.concatenate_batches(batch_states)
        else:
            batch_state = batch_states[0]

        generations, batch_state = self._model.generate_token(batch_state)

        if batch_state:
            self._batch_state_cache[batch_state.id] = batch_state
            return generations, batch_state.id
        return generations, None

    def filter_requests(self, batch_id: int, request_ids: List[int]) -> Optional[int]:
        batch_state = self._batch_state_cache.pop(batch_id)

        if len(request_ids) == 0:
            return None

        filtered = batch_state.filter(request_ids)
        if len(filtered):
            self._batch_state_cache[filtered.id] = filtered
            return filtered.id

        return None

    def report_stats(self):
        #print(f"worker stats: {[(id, cache.stats()) for id, cache in self._batch_state_cache.items()]}")
        if self._model.device.type == "cuda":
            #gc.collect()
            print(f"memory allocated: {torch.cuda.memory_allocated(self._model.device) / 2 ** 30}")
            print(f"memory reserved: {torch.cuda.memory_reserved(self._model.device) / 2 ** 30}")
            #self.check_cuda_objects()
            #if torch.cuda.memory_allocated(self._model.device) / 2 ** 30 > 30:
            #    self.debug_objects()

    def check_cuda_objects(self):
        from collections import defaultdict
        if self._model.device.type != "cuda":
            return
        d = defaultdict(int)

        for obj in gc.get_objects():
            try:
                if torch.is_tensor(obj) or (hasattr(obj, 'data') and torch.is_tensor(obj.data)):
                    t = tuple(obj.size()) + (obj.dtype, obj.device) 
                    d[t] += 1
            except:
                pass

        for count, obj_signature in sorted([(count, sig) for sig, count in d.items()], key=lambda x: x[0], reverse=True):
            print(count, obj_signature)

    def debug_objects(self):
        objs = gc.get_objects()
        tensors = [obj for obj in objs if torch.is_tensor(obj)]
        leaked_tensors = [t for t in tensors if t.size() == torch.Size([20, 1, 1024])]
        if len(leaked_tensors) >= 1000:
            import pdb;pdb.set_trace()
