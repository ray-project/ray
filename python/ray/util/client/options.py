from typing import Any
from typing import Dict
from typing import Optional

options = {
    "num_returns": (int, lambda x: x >= 0,
                    "The keyword 'num_returns' only accepts 0 "
                    "or a positive integer"),
    "num_cpus": (),
    "num_gpus": (),
    "resources": (),
    "accelerator_type": (),
    "max_calls": (int, lambda x: x >= 0,
                  "The keyword 'max_calls' only accepts 0 "
                  "or a positive integer"),
    "max_restarts": (int, lambda x: x >= -1,
                     "The keyword 'max_restarts' only accepts -1, 0 "
                     "or a positive integer"),
    "max_task_retries": (int, lambda x: x >= -1,
                         "The keyword 'max_task_retries' only accepts -1, 0 "
                         "or a positive integer"),
    "max_retries": (int, lambda x: x >= -1,
                    "The keyword 'max_retries' only accepts 0, -1 "
                    "or a positive integer"),
    "max_concurrency": (),
    "name": (),
    "lifetime": (),
    "memory": (),
    "object_store_memory": (),
    "placement_group": (),
    "placement_group_bundle_index": (),
    "placement_group_capture_child_tasks": (),
    "runtime_env": (),
    "override_environment_variables": (),
}


def validate_options(
        kwargs_dict: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if kwargs_dict is None:
        return None
    if len(kwargs_dict) == 0:
        return None
    out = {}
    for k, v in kwargs_dict.items():
        if k not in options.keys():
            raise TypeError(f"Invalid option passed to remote(): {k}")
        validator = options[k]
        if len(validator) != 0:
            if v is not None:
                if not isinstance(v, validator[0]):
                    raise ValueError(validator[2])
                if not validator[1](v):
                    raise ValueError(validator[2])
        out[k] = v
    return out
