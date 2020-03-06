from ray.rllib.utils import try_import_tf, try_import_torch

tf = try_import_tf()
torch, nn = try_import_torch()


def explained_variance(y, pred, framework="tf"):
    if framework == "tf":
        _, y_var = tf.nn.moments(y, axes=[0])
        _, diff_var = tf.nn.moments(y - pred, axes=[0])
        return tf.maximum(-1.0, 1 - (diff_var / y_var))
    else:
        y_var = torch.var(y, dim=[0])
        diff_var = torch.var(y - pred, dim=[0])
        min_ = torch.Tensor([-1.0])
        return torch.max(
            min_.to(
                device=torch.device("cuda")
            ) if torch.cuda.is_available() else min_,
            1 - (diff_var / y_var)
        )
