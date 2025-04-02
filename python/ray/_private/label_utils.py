import re
import yaml
from typing import Dict

import ray._private.ray_constants as ray_constants

# Regex patterns used to validate that labels conform to Kubernetes label syntax rules.
# Regex for optional prefix (DNS subdomain)
LABEL_PREFIX_REGEX = (
    r"^([a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*)$"
)
# Regex for mandatory name (label key without prefix) or value
LABEL_REGEX = r"^[a-zA-Z0-9]([a-zA-Z0-9_.-]*[a-zA-Z0-9])?$"


def parse_node_labels_string(
    labels_str: str, cli_logger, cf, command_arg="--labels"
) -> Dict[str, str]:
    try:
        labels = {}
        if labels_str == "":
            return labels
        # Labels argument should consist of a string of key=value pairs
        # separated by commas. Labels follow Kubernetes label syntax.
        label_pairs = labels_str.split(",")
        for pair in label_pairs:
            # Split each pair by `=`
            key_value = pair.split("=")
            if len(key_value) != 2:
                raise ValueError("Label value is not a key-value pair.")
            key = key_value[0].strip()
            value = key_value[1].strip()
            labels[key] = value
    except Exception as e:
        cli_logger.abort(
            "`{}` is not a valid string of key-value pairs, detail error:{}"
            "Valid values look like this: `{}`",
            cf.bold(f"{command_arg}={labels_str}"),
            str(e),
            cf.bold(f'{command_arg}="key1=val1,key2=val2"'),
        )
    return labels


def parse_node_labels_from_yaml_file(
    path: str, cli_logger, cf, command_arg="--labels-file"
) -> Dict[str, str]:
    try:
        if path == "":
            return {}
        with open(path, "r") as file:
            # Expects valid YAML content
            labels = yaml.safe_load(file)
            if not isinstance(labels, dict):
                raise ValueError(
                    "The format after deserialization is not a key-value pair map"
                )
            for key, value in labels.items():
                if not isinstance(key, str):
                    raise ValueError("The key is not string type.")
                if not isinstance(value, str):
                    raise ValueError(f'The value of the "{key}" is not string type')
    except Exception as e:
        cli_logger.abort(
            "The file at `{}` is not a valid YAML file, detail error:{}"
            "Valid values look like this: `{}`",
            cf.bold(f"{command_arg}={path}"),
            str(e),
            cf.bold(f"{command_arg}='gpu_type: A100\nregion: us'"),
        )
    return labels


def validate_node_labels(labels: Dict[str, str]):
    if labels is None:
        return
    for key in labels.keys():
        if key.startswith(ray_constants.RAY_DEFAULT_LABEL_KEYS_PREFIX):
            raise ValueError(
                f"Custom label keys `{key}` cannot start with the prefix "
                f"`{ray_constants.RAY_DEFAULT_LABEL_KEYS_PREFIX}`. "
                f"This is reserved for Ray defined labels."
            )
        if "/" in key:
            prefix, name = key.rsplit("/")
            if len(prefix) > 253 or not re.match(LABEL_PREFIX_REGEX, prefix):
                raise ValueError(
                    f"Invalid label key prefix `{prefix}`. Prefix must be a series of DNS labels "
                    f"separated by dots (.),not longer than 253 characters in total."
                )
        else:
            name = key
        if len(name) > 63 or not re.match(LABEL_REGEX, name):
            raise ValueError(
                f"Invalid label key name `{name}`. Name must be 63 chars or less beginning and ending "
                f"with an alphanumeric character ([a-z0-9A-Z]) with dashes (-), underscores (_),"
                f"dots (.), and alphanumerics between."
            )
        value = labels.get(key)
        if value is None or value == "":
            return
        if len(value) > 63 or not re.match(LABEL_REGEX, value):
            raise ValueError(
                f"Invalid label key value `{value}`. Value must be 63 chars or less beginning and ending "
                f"with an alphanumeric character ([a-z0-9A-Z]) with dashes (-), underscores (_),"
                f"dots (.), and alphanumerics between."
            )
