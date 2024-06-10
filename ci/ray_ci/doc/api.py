import re

from enum import Enum
from dataclasses import dataclass
from typing import Optional, List


_SPHINX_AUTOSUMMARY_HEADER = ".. autosummary::"
_SPHINX_AUTOCLASS_HEADER = ".. autoclass::"


class AnnotationType(Enum):
    PUBLIC_API = "PublicAPI"
    DEVELOPER_API = "DeveloperAPI"
    DEPRECATED = "Deprecated"
    UNKNOWN = "Unknown"


class CodeType(Enum):
    CLASS = "Class"
    FUNCTION = "Function"


@dataclass
class API:
    name: str
    annotation_type: AnnotationType
    code_type: CodeType

    def from_autosummary(doc: str, current_module: Optional[str] = None) -> List["API"]:
        """
        Parse API from the following autosummary sphinx block.

        .. autosummary::
            :option_01
            :option_02

            api_01
            api_02
        """
        apis = []
        lines = doc.splitlines()
        if not lines:
            return apis

        if lines[0].strip() != _SPHINX_AUTOSUMMARY_HEADER:
            return apis

        for line in lines:
            if line == _SPHINX_AUTOSUMMARY_HEADER:
                continue
            if line.strip().startswith(":"):
                # option lines
                continue
            if not line.strip():
                # empty lines
                continue
            if not re.match(r"\s", line):
                # end of autosummary, \s means empty space, this line is checking if
                # the line is not empty and not starting with empty space
                break
            api_name = (
                f"{current_module}.{line.strip()}" if current_module else line.strip()
            )
            apis.append(
                API(
                    name=api_name,
                    annotation_type=AnnotationType.PUBLIC_API,
                    code_type=CodeType.FUNCTION,
                )
            )

        return apis

    def from_autoclass(
        doc: str, current_module: Optional[str] = None
    ) -> Optional["API"]:
        """
        Parse API from the following autoclass sphinx block.

        .. autoclass:: api_01
        """
        doc = doc.strip()
        if not doc.startswith(_SPHINX_AUTOCLASS_HEADER):
            return None
        api_name = doc[len(_SPHINX_AUTOCLASS_HEADER) :].strip()

        return API(
            name=f"{current_module}.{api_name}" if current_module else api_name,
            annotation_type=AnnotationType.PUBLIC_API,
            code_type=CodeType.CLASS,
        )
