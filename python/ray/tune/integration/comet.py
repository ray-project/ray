from ray.air.integrations.comet import CometLoggerCallback as _CometLoggerCallback
from typing import List

import logging

from ray.util.annotations import Deprecated

logger = logging.getLogger(__name__)

callback_deprecation_message = (
    "`ray.tune.integration.comet.CometLoggerCallback` "
    "is deprecated and will be removed in "
    "the future. Please use `ray.air.integrations.comet.CometLoggerCallback` "
    "instead."
)


@Deprecated(message=callback_deprecation_message)
class CometLoggerCallback(_CometLoggerCallback):
    def __init__(
        self,
        online: bool = True,
        tags: List[str] = None,
        save_checkpoints: bool = False,
        **experiment_kwargs
    ):
        logging.warning(callback_deprecation_message)
        super().__init__(online, tags, save_checkpoints, **experiment_kwargs)
