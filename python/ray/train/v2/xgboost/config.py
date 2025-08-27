from contextlib import contextmanager

from ray.train.v2._internal.execution.train_fn_utils import get_train_fn_utils
from ray.train.xgboost.config import XGBoostConfig as XGBoostConfigV1


class XGBoostConfig(XGBoostConfigV1):
    @property
    def train_func_context(self):
        parent_context = super(XGBoostConfig, self).train_func_context

        @contextmanager
        def collective_communication_context():
            if get_train_fn_utils().is_distributed():
                with parent_context():
                    yield
            else:
                yield

        return collective_communication_context
