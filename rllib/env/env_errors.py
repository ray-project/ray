"""Error classes for RLlib environment operations."""

from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class StepFailedRecreateEnvError(Exception):
    """An exception that signals that the environment step failed and the environment needs to be reset.

    This exception may be raised by the environment's `step` method.
    It is then caught by the `EnvRunner` and the environment is reset.
    This can be useful if your environment is unstable, regularely crashing in a certain way.
    For example, if you connect to an external simulator that you have little control over.
    You can detect such crashes in your step method and throw this error to not log the error.
    Use this with caution, as it may lead to infinite loops of resetting the environment.
    """

    pass
