class TorchNotAvailableClass:
    def __init__(self):
        raise ValueError("`torch` is not installed. Please install torch to "
                         "use this backend.")

def torch_not_available_fn():
    raise ValueError("`torch` is not installed. Please install torch to "
                     "use this backend.")

try:
    import torch # noqa: F401
    TORCH_INSTALLED = True
except ImportError:
    TORCH_INSTALLED = False

if TORCH_INSTALLED:
    from ray.train.torch.torch import TorchConfig, TorchBackend, get_device, \
        prepare, prepare_data_loader
else:
    TorchConfig = TorchBackend = TorchNotAvailableClass
    get_device = prepare = prepare_data_loader = torch_not_available_fn



__all__ = [
    "TorchConfig", "TorchBackend", "get_device", "prepare", "prepare_data_loader"
]

