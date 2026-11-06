import json
from typing import Dict

import yaml

import ray._private.ray_constants as ray_constants
from ray._common.label_utils import (
    LABEL_OPERATORS as LABEL_OPERATORS,
    LABEL_PREFIX_REGEX as LABEL_PREFIX_REGEX,
    LABEL_REGEX as LABEL_REGEX,
    LABEL_SELECTOR_REGEX as LABEL_SELECTOR_REGEX,
    OPERATOR_PATTERN as OPERATOR_PATTERN,
    validate_fallback_strategy as validate_fallback_strategy,
    validate_label_key as validate_label_key,
    validate_label_selector as validate_label_selector,
    validate_label_selector_value as validate_label_selector_value,
    validate_label_value as validate_label_value,
    validate_node_label_syntax as validate_node_label_syntax,
)


def parse_node_labels_json(labels_json: str) -> Dict[str, str]:
    labels = json.loads(labels_json)
    if not isinstance(labels, dict):
        raise ValueError("The format after deserialization is not a key-value pair map")
    for key, value in labels.items():
        if not isinstance(key, str):
            raise ValueError("The key is not string type.")
        if not isinstance(value, str):
            raise ValueError(f'The value of the "{key}" is not string type')

    # Validate parsed custom node labels don't begin with ray.io prefix
    validate_node_labels(labels)

    return labels


def parse_node_labels_string(labels_str: str) -> Dict[str, str]:
    labels = {}

    # Remove surrounding quotes if they exist
    if len(labels_str) > 1 and labels_str.startswith('"') and labels_str.endswith('"'):
        labels_str = labels_str[1:-1]

    if labels_str == "":
        return labels

    # Labels argument should consist of a string of key=value pairs
    # separated by commas. Labels follow Kubernetes label syntax.
    label_pairs = labels_str.split(",")
    for pair in label_pairs:
        # Split each pair by `=`
        key_value = pair.split("=")
        if len(key_value) != 2:
            raise ValueError("Label string is not a key-value pair.")
        key = key_value[0].strip()
        value = key_value[1].strip()
        labels[key] = value

    # Validate parsed node labels follow expected Kubernetes label syntax
    validate_node_label_syntax(labels)

    return labels


def parse_node_labels_from_yaml_file(path: str) -> Dict[str, str]:
    if path == "":
        return {}
    with open(path, "r") as file:
        # Expects valid YAML content
        labels = yaml.safe_load(file)
        if not isinstance(labels, dict):
            raise ValueError(
                "The format after deserialization is not a key-value pair map."
            )
        for key, value in labels.items():
            if not isinstance(key, str):
                raise ValueError("The key is not string type.")
            if not isinstance(value, str):
                raise ValueError(f'The value of "{key}" is not string type.')

    # Validate parsed node labels follow expected Kubernetes label syntax
    validate_node_label_syntax(labels)

    return labels


# TODO (ryanaoleary@): This function will be removed after the migration to the label
# selector API from NodeLabelSchedulingPolicy is complete.
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
