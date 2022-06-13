from ray.data import Dataset
from ray.air.config import ScalingConfig
from ray.tune.sample import Categorical

_SCALING_CONFIG_KEY = "scaling_config"


def process_dataset_param(config_dict: dict):
    """Going through config dict (params space) and fully execute any Dataset
    if necessary.
    """
    for k, v in config_dict.items():
        if isinstance(v, dict):
            process_dataset_param(v)
        elif isinstance(v, Dataset):
            config_dict[k] = v.fully_executed()
        # TODO(xwjiang): Consider CV config for beta.
        # elif isinstance(v, int):
        #     # CV settings
        #     pass
        elif isinstance(v, list) or isinstance(v, Categorical):
            _list = v if isinstance(v, list) else v.categories
            if len(_list) == 0:
                return
            if isinstance(_list[0], Dataset):
                if isinstance(v, list):
                    config_dict[k] = [_item.fully_executed() for _item in _list]
                else:  # Categorical
                    config_dict[k].categories = [
                        _item.fully_executed() for _item in _list
                    ]


def process_scaling_config(params_space: dict):
    """Convert ``params_space["scaling_config"]`` back to a dict so that
    it can be used to generate search space.
    """
    scaling_config = params_space.get(_SCALING_CONFIG_KEY, None)
    if not isinstance(scaling_config, ScalingConfig):
        return
    params_space[_SCALING_CONFIG_KEY] = scaling_config.__dict__.copy()
