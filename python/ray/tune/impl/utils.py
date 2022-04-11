from typing import Optional

from ray.data import Dataset
from ray.tune.sample import Categorical


def execute_dataset(config_dict: dict, config_dict_key: Optional[str] = None) -> Optional[bool]:
    """Going through config dict (params space) and fully execute any Dataset
    if necessary.

    For the cv mode, the dataset needs to be like:
    "datasets": {
        "num_folds": 3,
        "train_and_validation": ds,
    }

    Args:
        config_dict_key: the name of the config dict.
    Returns:
        True if the user specifies CV mode.
    """
    is_cv = False
    is_recurring_cv = False
    for k, v in config_dict.items():
        if isinstance(v, dict):
            is_recurring_cv = execute_dataset(v, config_dict_key=k) and is_recurring_cv
        elif isinstance(v, Dataset):
            config_dict[k] = v.fully_executed()
        elif k == "num_folds":
            # Let's support not tunable fold number for now.
            assert isinstance(v, int), "Expecting an integer for `num_folds`."
            assert config_dict_key == "datasets", "Expecting `num_folds` to be specified directly under `datasets` entry."
            is_cv = True
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
    if is_cv:
        assert "train_and_validation" in config_dict.keys(), "Expecting \"train_and_validation\" key to be supplied together with num_folds."

    return is_cv or is_recurring_cv

