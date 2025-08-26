from dataclasses import dataclass

from ray.train._internal.xgboost.xgboost_config_impl import XGBoostConfigImpl
from ray.train.utils import _copy_doc


@dataclass
@_copy_doc(XGBoostConfigImpl)
class XGBoostConfig(XGBoostConfigImpl):
    pass
