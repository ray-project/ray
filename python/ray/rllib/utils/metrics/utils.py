import re


def to_snake_case(class_name: str) -> str:
    """Converts class name to snake case.

    This is used to unify metrics names when using class names within.
    Args:
        class_name: A string defining a class name usually in camel
            case.

    Returns:
        The class name in snake case.
    """
    # Insert _ between a lower- or digit-char and an upper-char
    name = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", class_name)
    # Insert _ between an upper-char followed by upper+lower (to split
    # "ABCReward" into "abc_reward")
    name = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", "_", name)
    return name.lower()
