from dataclasses import dataclass
from typing import Dict, List, Tuple, Union
import torch
from ray import cloudpickle as pickle
import pyarrow as pa

# (dtype, shape, offset)
FEATURE_TYPE = Tuple[torch.dtype, torch.Size, int]
TORCH_BYTE_ELEMENT_TYPE = torch.uint8

def _create_binary_array_from_buffer(buffer: bytes) -> pa.BinaryArray:
    """zero-copy create a binary array from a buffer"""
    data_buffer = pa.py_buffer(buffer)
    return pa.Array.from_buffers(
        pa.binary(),
        1,
        [
            None,
            pa.array([0, data_buffer.size], type=pa.int32()).buffers()[1],
            data_buffer,
        ],
    )


@dataclass
class Metadata:
    features: Dict[str, List[FEATURE_TYPE]]
    total_buffer_size: int

@dataclass
class OneRowBatch:
    buffer: torch.Tensor
    metadata: Metadata

    @classmethod
    def from_batch(cls, batch: Dict[str, Union[List[torch.Tensor], torch.Tensor]]) -> 'OneRowBatch':
        features: Dict[str, List[FEATURE_TYPE]] = {}
        flattenned_binary_tensors = []
        total_buffer_size = 0
        for name, tensors in batch.items():
            features[name] = []
            if not isinstance(tensors, list):
                tensors = [tensors]
            for tensor in tensors:
                flattened_tensor = tensor.flatten().contiguous().view(TORCH_BYTE_ELEMENT_TYPE)
                flattenned_binary_tensors.append(flattened_tensor)
                features[name].append((tensor.dtype, tensor.shape, total_buffer_size))
                total_buffer_size += flattened_tensor.shape[0]
        buffer = torch.empty(total_buffer_size, dtype=TORCH_BYTE_ELEMENT_TYPE)
        cur_offset = 0
        for flattened_tensor in flattenned_binary_tensors:
            buffer[cur_offset:cur_offset + flattened_tensor.shape[0]] = flattened_tensor
            cur_offset += flattened_tensor.shape[0]
        
        return OneRowBatch(
            buffer=buffer,
            metadata=Metadata(
                features=features,
                total_buffer_size=total_buffer_size,
            ),
        )

    def to_one_row_table(self) -> pa.Table:
        buffer_array = _create_binary_array_from_buffer(self.buffer.numpy().data)
        metadata_array = _create_binary_array_from_buffer(pickle.dumps(self.metadata))
        return pa.Table.from_arrays(
            arrays=[buffer_array, metadata_array],
            names=["_buffer", "_metadata"],
        )

    @classmethod
    def from_one_row_table(cls, table: pa.Table) -> 'OneRowBatch':
        return OneRowBatch(
            buffer=torch.frombuffer(
                table["_buffer"].chunks[0].buffers()[2],
                dtype=TORCH_BYTE_ELEMENT_TYPE
            ),
            metadata=pickle.loads(table["_metadata"].chunks[0].buffers()[2]),
        )

    def to_batch(self, pin_memory: bool = False) -> Dict[str, List[torch.Tensor]]:
        batch = {}
        storage_buffer = self.buffer.untyped_storage()
        offsets = []
        for name, features in self.metadata.features.items():
            for _, _, offset in features:
                offsets.append(offset)
        offsets.append(self.metadata.total_buffer_size)

        offset_id = 0
        for name, features in self.metadata.features.items():
            batch[name] = []
            for dtype, shape, _ in features:
                tensor = torch.tensor(
                    storage_buffer[offsets[offset_id]:offsets[offset_id + 1]],
                    dtype=dtype,
                    device=storage_buffer.device
                ).view(shape)
                # TODO(xgui): We should be able to move the whole buffer into GPU memory at once,
                # as long as we care about the alignment of the tensors.
                if pin_memory:
                    tensor = tensor.pin_memory()
                batch[name].append(tensor)
                offset_id += 1
        return batch
