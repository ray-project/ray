from ray.util.sgd.v2.backends.tensorflow import TensorflowConfig
from ray.util.sgd.v2.backends.torch import TorchConfig

BACKEND_NAME_TO_CONFIG_CLS = {
    "tensorflow": TensorflowConfig,
    "torch": TorchConfig
}

RESULT_FILE_JSON = "results.json"
