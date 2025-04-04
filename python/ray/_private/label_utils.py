import re
import yaml
from typing import Dict


# Regex patterns used to validate that labels conform to Kubernetes label syntax rules.
# https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set

# Regex for mandatory name (DNS label) or value
# Examples:
#   Valid matches: "a", "label-name", "a-._b", "123", "this_is_a_valid_label"
#   Invalid matches: "-abc", "abc-", "my@label", "a" * 64
LABEL_REGEX = re.compile(r"[a-zA-Z0-9]([a-zA-Z0-9_.-]*[a-zA-Z0-9]){0,62}")

# Regex for optional prefix (DNS subdomain)
# Examples:
#   Valid matches: "abc", "sub.domain.example", "my-label", "123.456.789"
#   Invalid matches: "-abc", "prefix_", "sub..domain", sub.$$.example
LABEL_PREFIX_REGEX = rf"^({LABEL_REGEX.pattern}?(\.{LABEL_REGEX.pattern}?)*)$"

# Supported operators for label selector conditions. Not (!) conditions are handled separately.
LABEL_OPERATORS = {"in"}
# Create a pattern string dynamically based on the LABEL_OPERATORS
OPERATOR_PATTERN = "|".join([re.escape(operator) for operator in LABEL_OPERATORS])

# Regex to match valid label selector operators and values
# Examples:
#   Valid matches: "spot", "!GPU", "213521", "in(A123, B456, C789)", "!in(spot, on-demand)", "valid-value"
#   Invalid matches: "-spot", "spot_", "in()", "in(spot,", "in(H100, TPU!GPU)", "!!!in(H100, TPU)"
LABEL_SELECTOR_REGEX = re.compile(
    rf"^!?(?:{OPERATOR_PATTERN})?\({LABEL_REGEX.pattern}(?:, ?{LABEL_REGEX.pattern})*\)$|^!?{LABEL_REGEX.pattern}$"
)


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
            raise ValueError("Label value is not a key-value pair.")
        key = key_value[0].strip()
        value = key_value[1].strip()
        labels[key] = value

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

    return labels


def validate_node_labels(labels: Dict[str, str]):
    if labels is None:
        return
    for key in labels.keys():
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
