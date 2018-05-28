from ray.rllib.v2.policy import Policy


class ImperativePolicy(Policy):
    """Policy def for imperative frameworks, e.g., PyTorch.
    
    You must implement compute/apply grads which return/take numpy arrays."""
    pass
