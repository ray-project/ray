from enum import Enum
from pydantic import BaseModel, Field, validator
from typing import Tuple, Dict


class SupportedLanguage(str, Enum):
    python36 = "python_3.6"
    python37 = "python_3.7"
    python38 = "python_3.8"
    python39 = "python_3.9"
    python310 = "python_3.10"


class AppConfig(BaseModel):
    init_args: Tuple = Field(
        default=None,
        description=("The application's init_args. Only works with Python 3 "
                     "applications.")
    )
    init_kwargs: Dict = Field(
        default=None,
        description=("The application's init_args. Only works with Python 3 "
                     "applications.")
    )
    import_path: str = Field(
        default=None,
        description=("The application's full import path. Should be of the "
                     "form \"module.submodule_1...submodule_n."
                     "MyClassOrFunction.\" This is equivalent to "
                     "\"from module.submodule_1...submodule_n import "
                     "MyClassOrFunction\". Only works with Python 3 "
                     "applications.")
    )
    language: SupportedLanguage = Field(
        ...,
        description="The application's coding language."
    )

    @validator("language")
    def language_supports_specified_attributes(cls, v, values):
        required_attributes = {
            SupportedLanguage.python36: {"import_path"},
            SupportedLanguage.python37: {"import_path"},
            SupportedLanguage.python38: {"import_path"},
            SupportedLanguage.python39: {"import_path"},
            SupportedLanguage.python310: {"import_path"},
        }

        optional_attributes = {
            SupportedLanguage.python36: {"init_args", "init_kwargs"},
            SupportedLanguage.python37: {"init_args", "init_kwargs"},
            SupportedLanguage.python38: {"init_args", "init_kwargs"},
            SupportedLanguage.python39: {"init_args", "init_kwargs"},
            SupportedLanguage.python310: {"init_args", "init_kwargs"},
        }

        for attribute in required_attributes[v]:
            if attribute not in values or values[attribute] is None:
                raise ValueError(f"{attribute} must be defined in the "
                                 f"{v.value} language.")
        
        supported_attributes = required_attributes[v].union(optional_attributes[v])
        for attribute, value in values.items():
            if attribute not in supported_attributes and value is not None:
                raise ValueError(f"Got {value} as {attribute}. However, "
                                 f"{attribute} is not supported in the "
                                 f"{v.value} language.")
