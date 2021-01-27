from pydantic.dataclasses import dataclass


@dataclass
class CondaEnv:
    name: str
