# flake8: noqa
from pkg_resources import packaging

# Pydantic is a dependency of `ray["default"]` but not the minimal installation,
# so handle the case where it isn't installed.
try:
    import pydantic

    PYDANTIC_INSTALLED = True
except ImportError:
    pydantic = None
    PYDANTIC_INSTALLED = False


if not PYDANTIC_INSTALLED:
    IS_PYDANTIC_2 = False
    BaseModel = None
    Extra = None
    Field = None
    NonNegativeFloat = None
    NonNegativeInt = None
    PositiveFloat = None
    PositiveInt = None
    ValidationError = None
    root_validator = None
    validator = None
# In pydantic <1.9.0, __version__ attribute is missing, issue ref:
# https://github.com/pydantic/pydantic/issues/2572, so we need to check
# the existence prior to comparison.
elif hasattr(pydantic, "__version__") and packaging.version.parse(
    pydantic.__version__
) < packaging.version.parse("2.0"):
    IS_PYDANTIC_2 = False
    from pydantic import (
        BaseModel,
        Extra,
        Field,
        NonNegativeFloat,
        NonNegativeInt,
        PositiveFloat,
        PositiveInt,
        ValidationError,
        root_validator,
        validator,
    )
else:
    IS_PYDANTIC_2 = True
    from pydantic.v1 import (
        BaseModel,
        Extra,
        Field,
        NonNegativeFloat,
        NonNegativeInt,
        PositiveFloat,
        PositiveInt,
        ValidationError,
        root_validator,
        validator,
    )
