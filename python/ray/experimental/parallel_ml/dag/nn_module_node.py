from typing import Any

import torch
from ray.dag import DAGNode


class NNModuleNode(DAGNode):
    def get_module(self) -> torch.nn.Module:
        pass

    def get_input_tensor_shape(self) -> Any:
        pass

    def get_input_tensor_dtype(self) -> torch.dtype:
        pass

    def get_data_loader(self) -> torch.utils.data.DataLoader:
        pass
