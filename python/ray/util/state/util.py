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


def record_deprecated_state_api_import():
    import warnings
    from ray._private.usage.usage_lib import TagKey, record_extra_usage_tag

    warnings.warn(
        "Ray state API is no longer experimental. Please import from `ray.util.state`. "
        "instead. Importing from `ray.experimental` will be deprecated in "
        "future releases. ",
        DeprecationWarning,
    )

    record_extra_usage_tag(TagKey.EXPERIMENTAL_STATE_API_IMPORT, "1")
