from contextlib import contextmanager

from ray.train.v2._internal.execution.train_fn_utils import get_train_fn_utils


def _create_xgboost_config_class():
    """Create XGBoostConfig class with delayed import to avoid circular imports."""
    # Import here to avoid circular import when v2 is enabled
    from ray.train.xgboost.config import XGBoostConfig as XGBoostConfigV1

    class XGBoostConfig(XGBoostConfigV1):
        @property
        def train_func_context(self):
            if get_train_fn_utils().is_running_in_distributed_mode():
                return super().train_func_context
            else:

                @contextmanager
                def no_op_context():
                    yield

                return no_op_context

    return XGBoostConfig


# Create the class at module level
XGBoostConfig = _create_xgboost_config_class()
