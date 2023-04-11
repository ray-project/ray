from dataclasses import dataclass
import torch
import numpy as np
import tree # pip install dm-tree

from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.evaluation.postprocessing import Postprocessing

@dataclass
class BufferItem:
    # TODO (Kourosh): These names have to match those in the SampleBatch and 
    # PostProcessing.
    obs: dict # keys (shape): input_ids (T,), attention_mask (T,)
    actions: dict # keys: sequence (T,), response_mask (T,), logits (T, VS), attention_mask (T, )
    infos: dict 
    rewards: float # scalar (python float)
    value_targets: float # scalar (python float)
    advantages: float # scalar (python float)

class Buffer:
    """This buffer should work for both torch and numpy types in the buffer items.
    
    Its job is to collect simple BufferItems but then upon calling 
    convert_to_sample_batch, figure out the padding required to create blocks for 
    tensors inside a SampleBatch.
    """
    
    def __init__(self):
        self._buffer = []
        self._framework = None

    def append(self, item: BufferItem):
        if self._framework is None:
            self._framework = torch if isinstance(item.obs["input_ids"], torch.Tensor) else np
        else:
            if self._framework == torch:
                assert isinstance(item.obs["input_ids"], torch.Tensor), "The buffer items should be of the same framework."
            else:
                assert isinstance(item.obs["input_ids"], np.ndarray), "The buffer items should be of the same framework."


        # under the same key, the values should be of the same length
        for k in (SampleBatch.ACTIONS, SampleBatch.OBS):
            flattened = tree.flatten(getattr(item, k))
            for i in range(len(flattened) - 1):
                if not flattened[i].shape[0] == flattened[i+1].shape[0]:
                    raise ValueError("The values under the same key should be of the same length.")
    
        self._buffer.append(item)
    
    def convert_to_sample_batch(self, padding_type: str = "right") -> SampleBatch:
        assert padding_type in ("left", "right"), "The padding should be either 'left' or 'right'."
        keys = BufferItem.__dataclass_fields__.keys()

        sample_batch_dict = {}
        for key in keys:
            values = []
            for item in self._buffer: 
                val = getattr(item, key)

                if isinstance(val, float):
                    val = torch.tensor(val) if self._framework == torch else np.array(val)
                elif isinstance(val, dict):
                    val = NestedDict(val)
        
                values.append(val)

            # some values may not have the same sequence length, so we need to pad them
            if key in (SampleBatch.ACTIONS, SampleBatch.OBS):
                # we should first obtain the max length for each value. Remember that each value is possibly a nested dict where the values are tensors.

                # TODO (Kourosh): This is not optimal since we are flattening the whole 
                # tree structure, while all we need is the DFS traversal of the tree 
                # and obtaining the first leave.

                # Each v is a nested dict where the leave values can be iterated easily
                max_length = max(next(iter(v.values())).shape[0] for v in values)
    
                for item in values:
                    for nested_key, val in item.items():
                        if val.shape[0] < max_length:
                            padding = self._framework.zeros(
                                (max_length - val.shape[0], *val.shape[1:]), 
                                dtype=val.dtype
                            )

                            if padding_type == "left":
                                if self._framework == torch:
                                    item[nested_key] = torch.cat((padding, val), 0)
                                else:
                                    item[nested_key] = np.concatenate((padding, val), 0)
                            else:
                                if self._framework == torch:
                                    item[nested_key] = torch.cat((val, padding), 0)
                                else:
                                    item[nested_key] = np.concatenate((val, padding), 0)
            
            values = tree.map_structure(lambda *x: self._framework.stack(x,0), *values)
            sample_batch_dict[key] = values.asdict() if isinstance(values, NestedDict) else values

        return SampleBatch(sample_batch_dict)
    

if __name__ == "__main__":

    foo = Buffer()
    foo.append(
        BufferItem(**{
            SampleBatch.OBS: {
                "input_ids": torch.tensor([1, 2, 3]), 
                "attention_mask": torch.tensor([1, 1, 1])
            },
            SampleBatch.ACTIONS: {
                "sequence": torch.tensor([1, 2, 3, 4]), 
                "logits": torch.tensor([[0.5, 0.5] for _ in range(4)]), 
                "attention_mask": torch.tensor([1, 1, 1, 1])
            },
            SampleBatch.REWARDS: 1.0,
            Postprocessing.VALUE_TARGETS: 1.0,
            Postprocessing.ADVANTAGES: 1.0,
        })   
    )

    foo.append(
        BufferItem(**{
            SampleBatch.OBS: {
                "input_ids": torch.tensor([4, 5, 6]), 
                "attention_mask": torch.tensor([1, 1, 1])
            },
            SampleBatch.ACTIONS: {
                "sequence": torch.tensor([4, 5, 6, 7]), 
                "logits": torch.tensor([[0.5, 0.5] for _ in range(4)]), 
                "attention_mask": torch.tensor([1, 1, 1, 1])
            },
            SampleBatch.REWARDS: 1.0,
            Postprocessing.VALUE_TARGETS: 1.0,
            Postprocessing.ADVANTAGES: 1.0,
        })  
    )

    # action sequence length is different from the previous two
    foo.append(
        BufferItem(**{
            SampleBatch.OBS: {
                "input_ids": torch.tensor([4, 5, 6]), 
                "attention_mask": torch.tensor([1, 1, 1])
            },
            SampleBatch.ACTIONS: {
                "sequence": torch.tensor([4, 5, 6]), 
                "logits": torch.tensor([[0.5, 0.5] for _ in range(3)]), 
                "attention_mask": torch.tensor([1, 1, 1])
            },
            SampleBatch.REWARDS: 1.0,
            Postprocessing.VALUE_TARGETS: 1.0,
            Postprocessing.ADVANTAGES: 1.0,
        })  
    )
    
    sb = foo.convert_to_sample_batch()



    # numpy version
    foo = Buffer()
    foo.append(
        BufferItem(**{
            SampleBatch.OBS: {
                "input_ids": np.array([1, 2, 3]), 
                "attention_mask": np.array([1, 1, 1])
            },
            SampleBatch.ACTIONS: {
                "sequence": np.array([1, 2, 3, 4]), 
                "logits": np.array([[0.5, 0.5] for _ in range(4)]), 
                "attention_mask": np.array([1, 1, 1, 1])
            },
            SampleBatch.REWARDS: 1.0,
            Postprocessing.VALUE_TARGETS: 1.0,
            Postprocessing.ADVANTAGES: 1.0,
        })   
    )

    foo.append(
        BufferItem(**{
            SampleBatch.OBS: {
                "input_ids": np.array([4, 5, 6]),
                "attention_mask": np.array([1, 1, 1])
            },
            SampleBatch.ACTIONS: {
                "sequence": np.array([4, 5, 6, 7]),
                "logits": np.array([[0.5, 0.5] for _ in range(4)]),
                "attention_mask": np.array([1, 1, 1, 1])
            },
            SampleBatch.REWARDS: 1.0,
            Postprocessing.VALUE_TARGETS: 1.0,
            Postprocessing.ADVANTAGES: 1.0,
        })
    )

    # action sequence length is different from the previous two
    foo.append(
        BufferItem(**{
            SampleBatch.OBS: {
                "input_ids": np.array([4, 5, 6]),
                "attention_mask": np.array([1, 1, 1]),
            },
            SampleBatch.ACTIONS: {
                "sequence": np.array([4, 5, 6]),
                "logits": np.array([[0.5, 0.5] for _ in range(3)]),
                "attention_mask": np.array([1, 1, 1])
            },
            SampleBatch.REWARDS: 1.0,
            Postprocessing.VALUE_TARGETS: 1.0,
            Postprocessing.ADVANTAGES: 1.0,
        })
    )
    
    sb = foo.convert_to_sample_batch()


    breakpoint()

            