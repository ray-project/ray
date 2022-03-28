from ray.rllib.utils.deprecation import Deprecated


@Deprecated(
    new="Sub-class from Trainer (or another Trainer sub-class) directly! "
    "See e.g. ray.rllib.agents.dqn.dqn.py for an example.",
    error=True,
)
def build_trainer(*args, **kwargs):
    pass  # deprecated w/ error
