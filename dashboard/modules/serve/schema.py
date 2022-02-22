from enum import Enum


class SupportedLanguage(str, Enum):
    python36 = "python_3.6"
    python37 = "python_3.7"
    python38 = "python_3.8"
    python39 = "python_3.9"
    python310 = "python_3.10"
