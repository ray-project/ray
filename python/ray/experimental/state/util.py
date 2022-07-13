from typing import Optional, Union


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
