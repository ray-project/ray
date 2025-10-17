from ray.rllib.utils.framework import try_import_torch, try_import_tf

torch, _ = try_import_torch()
_, tf, _ = try_import_tf()


def single_value_to_cpu(value):
    if torch and isinstance(value, torch.Tensor):
        return value.detach().cpu().item()
    elif tf and tf.is_tensor(value):
        return value.numpy()
    return value


def list_of_values_to_cpu(values):
    if torch and isinstance(values[0], torch.Tensor):
        return [value.detach().cpu() for value in values]
    elif tf and isinstance(values[0], tf.Tensor):
        return [value.numpy() for value in values]
    return values
