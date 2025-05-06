import re


def to_snake_case(class_name: str) -> str:
    """Converts class name to snake case.

    This is used to unify metrics names when using class names within.
    Args:
        class_name: A string defining a class name (usually in camel
        case).

    Returns:
        The class name in snake case.
    """
    return re.sub(r"(?<!^)(?=[A-Z])", "_", class_name).lower()
