from ray.tune.logger.logger import Logger
from ray.util.annotations import PublicAPI


@PublicAPI
class NoopLogger(Logger):
    def on_result(self, result):
        pass
