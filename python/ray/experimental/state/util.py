from typing import List, Optional, Union


def convert_string_to_type(
    val: Optional[Union[str, int, float, bool]], convert_type: Union[int, float, bool]
) -> Union[int, float, bool]:
    """Convert the given value to a convert type.

    If the given val is None, it will just return None without the conversion.

    It supports,
        str -> int/float/bool
        int -> int
        bool -> bool
        float -> float
    """
    if val is None:
        return None
    elif type(val) is convert_type:
        return val
    elif convert_type is int:
        try:
            val = int(val)
        except ValueError:
            raise ValueError(
                f"Failed to convert a value {val} of type {type(val)} to {convert_type}"
            )
    elif convert_type is float:
        try:
            val = float(val)
        except ValueError:
            raise ValueError(
                f"Failed to convert a value {val} of type {type(val)} to {convert_type}"
            )
    elif convert_type is bool:
        # Without this, "False" will become True.
        if val == "False" or val == "false" or val == "0":
            val = False
        elif val == "True" or val == "true" or val == "1":
            val = True
        else:
            raise ValueError(
                f"Failed to convert a value {val} of type {type(val)} to {convert_type}"
            )
    else:
        assert False, f"Unsupported convert type {convert_type}"
    return val


class HumanReadable:
    """A class that consumes a configuration object C,
    and a generic object X, and creates human-readable keys to
    augment X with respect to configuration C.

    Configuration object:

    {
        "key" : function (value) -> string
    }

    The configuration object would contain the keys we want
    to make "human-readable." For each key, we would specify
    a conversion function that will output a string representing
    a human readable version of that key.

    We require that "key" maps to values of primitive types (int, str, float... etc)."""

    def __init__(self, config: dict) -> None:
        self.config = config

    def prepare(self, obj: dict):
        pass

    def format_list(self, objects: List[dict]):
        for obj in objects:
            if type(obj) is dict:
                self.format(obj)

    def format(self, obj: dict):
        """Runs a DFS on task_state, expanding on iterable keys, and
        augmenting the task_state with additional, human readable keys."""
        for key in obj:
            if key in self.config:
                human_readable_str = self.config[key](obj[key])
                obj[key] = human_readable_str
                continue
            if type(obj[key]) is list:
                self.format_list(obj[key])
                continue
            if type(obj[key]) is dict:
                self.format(obj[key])
                continue
