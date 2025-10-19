from ray.rllib.utils.framework import try_import_torch, try_import_tf
from ray.util.annotations import DeveloperAPI

torch, _ = try_import_torch()
_, tf, _ = try_import_tf()


@DeveloperAPI
def single_value_to_cpu(value):
    if torch and isinstance(value, torch.Tensor):
        return value.detach().cpu().item()
    elif tf and tf.is_tensor(value):
        return value.numpy()
    return value
