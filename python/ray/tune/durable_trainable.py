from typing import Callable, Type, Union

import logging

from ray.tune.trainable import Trainable

from ray.util.annotations import Deprecated

logger = logging.getLogger(__name__)


# Deprecated: Remove in Ray > 1.13
@Deprecated
class DurableTrainable(Trainable):
    _sync_function_tpl = None

    def __init__(self, *args, **kwargs):
        raise DeprecationWarning(
            "DeprecationWarning: The `DurableTrainable` class is being "
            "deprecated. Instead, all Trainables are durable by default "
            "if you provide an `upload_dir`. You'll likely only need to "
            "remove the call to `tune.durable()` or directly inherit from "
            "`Trainable` instead of `DurableTrainable` for class "
            "trainables to make your code forward-compatible."
        )


# Deprecated: Remove in Ray > 1.13
@Deprecated
def durable(trainable: Union[str, Type[Trainable], Callable]):
    raise DeprecationWarning(
        "DeprecationWarning: `tune.durable()` is being deprecated."
        "Instead, all Trainables are durable by default if "
        "you provide an `upload_dir`. You'll likely only need to remove "
        "the call to `tune.durable()` to make your code "
        "forward-compatible."
    )
