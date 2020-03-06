from ray.util.sgd.torch import TorchTrainer
from ray.util.sgd.tf import TFTrainer

__all__ = ["TorchTrainer", "TFTrainer"]


def PyTorchTrainer(**kwargs):
    raise DeprecationWarning("ray.util.sgd.pytorch.PyTorchTrainer has been "
                             "renamed to ray.util.sgd.torch.TorchTrainer")
