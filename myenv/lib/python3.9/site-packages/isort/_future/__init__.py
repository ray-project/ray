import sys

if sys.version_info.major <= 3 and sys.version_info.minor <= 6:
    from . import _dataclasses as dataclasses

else:
    import dataclasses  # type: ignore

dataclass = dataclasses.dataclass  # type: ignore
field = dataclasses.field  # type: ignore

__all__ = ["dataclasses", "dataclass", "field"]
