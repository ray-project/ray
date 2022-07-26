from ray.tune._structure_refactor import warn_structure_refactor
from ray.tune.trainable.function_trainable import (  # noqa: F401, F403
    FunctionTrainable as FunctionRunner,
    FuncCheckpointUtil,
    wrap_function,
)

warn_structure_refactor(
    "ray.tune.function_runner.FunctionRunner",
    "ray.tune.trainable.function_trainable.FunctionTrainable",
)
