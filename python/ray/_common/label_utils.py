"""Label syntax and selector validation shared across Ray libraries."""

import re
from typing import Any, Dict, List, Optional

# Regex patterns used to validate that labels conform to Kubernetes label syntax rules.
# https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set

# Regex for mandatory name (DNS label) or value
# Examples:
#   Valid matches: "a", "label-name", "a-._b", "123", "this_is_a_valid_label"
#   Invalid matches: "-abc", "abc-", "my@label"
LABEL_REGEX = re.compile(r"([a-zA-Z0-9]([a-zA-Z0-9_.-]{0,61}[a-zA-Z0-9])?)")

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


def validate_label_key(key: str) -> Optional[str]:
    if "/" in key:
        prefix, name = key.rsplit("/", 1)
        if len(prefix) > 253 or not re.fullmatch(LABEL_PREFIX_REGEX, prefix):
            return str(
                f"Invalid label key prefix `{prefix}`. Prefix must be a series of DNS labels "
                f"separated by dots (.), not longer than 253 characters in total."
            )
    else:
        name = key
    if len(name) > 63 or not re.fullmatch(LABEL_REGEX, name):
        return str(
            f"Invalid label key name `{name}`. Name must be 63 chars or less beginning and ending "
            f"with an alphanumeric character ([a-z0-9A-Z]) with dashes (-), underscores (_),"
            f"dots (.), and alphanumerics between."
        )
    return None


def validate_label_value(value: str):
    if value == "":
        return
    if len(value) > 63 or not re.fullmatch(LABEL_REGEX, value):
        raise ValueError(
            f"Invalid label key value `{value}`. Value must be 63 chars or less beginning and ending "
            f"with an alphanumeric character ([a-z0-9A-Z]) with dashes (-), underscores (_),"
            f"dots (.), and alphanumerics between."
        )


def validate_label_selector(label_selector: Optional[Dict[str, str]]) -> Optional[str]:
    if label_selector is None:
        return None

    for key, value in label_selector.items():
        possible_error_message = validate_label_key(key)
        if possible_error_message:
            return possible_error_message
        if value is not None:
            possible_error_message = validate_label_selector_value(value)
            if possible_error_message:
                return possible_error_message

    return None


def validate_label_selector_value(selector: str) -> Optional[str]:
    if selector == "":
        return None
    if not re.fullmatch(LABEL_SELECTOR_REGEX, selector):
        return str(
            f"Invalid label selector value `{selector}`. The label selector value should contain optional operators and a label value. Supported operators are: ! and {LABEL_OPERATORS}. "
            f"Value must be 63 chars or less beginning and ending "
            f"with an alphanumeric character ([a-z0-9A-Z]) with dashes (-), underscores (_),"
            f"dots (.), and alphanumerics between."
        )

    return None


# TODO (ryanaoleary@): This function will replace `validate_node_labels` after
# the migration from NodeLabelSchedulingPolicy to the Label Selector API is complete.
def validate_node_label_syntax(labels: Dict[str, str]):
    if labels is None:
        return
    for key, value in labels.items():
        possible_error_message = validate_label_key(key)
        if possible_error_message:
            raise ValueError(possible_error_message)
        if value is not None:
            validate_label_value(value)


def validate_fallback_strategy(
    fallback_strategy: Optional[List[Dict[str, Any]]]
) -> Optional[str]:
    if fallback_strategy is None:
        return None

    # Supported options for `fallback_strategy` scheduling.
    supported_options = {"label_selector"}

    for strategy in fallback_strategy:
        if not isinstance(strategy, dict):
            return "Each element in fallback_strategy must be a dictionary."

        if not strategy:
            return "Empty dictionary found in `fallback_strategy`."

        # Validate `fallback_strategy` only contains supported options.
        for option in strategy:
            if option not in supported_options:
                return (
                    f"Unsupported option found: '{option}'. "
                    f"Only {list(supported_options)} is currently supported."
                )

        # Validate the 'label_selector' dictionary.
        label_selector = strategy.get("label_selector")
        if label_selector:
            if not isinstance(label_selector, dict):
                return 'The value of "label_selector" must be a dictionary.'

            error_message = validate_label_selector(label_selector)
            if error_message:
                return error_message

    return None
