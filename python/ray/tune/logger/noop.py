from ray.tune.logger.logger import Logger
from ray.util.annotations import Deprecated, PublicAPI


@Deprecated(message="`NoopLogger` will be removed in Ray 2.7.")
@PublicAPI
class NoopLogger(Logger):
    def on_result(self, result):
        pass
