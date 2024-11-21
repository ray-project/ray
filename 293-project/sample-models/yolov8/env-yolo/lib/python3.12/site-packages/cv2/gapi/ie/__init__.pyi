__all__: list[str] = []

import cv2.typing
import typing as _typing


from cv2.gapi.ie import detail as detail


# Enumerations
TraitAs_TENSOR: int
TRAIT_AS_TENSOR: int
TraitAs_IMAGE: int
TRAIT_AS_IMAGE: int
TraitAs = int
"""One of [TraitAs_TENSOR, TRAIT_AS_TENSOR, TraitAs_IMAGE, TRAIT_AS_IMAGE]"""

Sync: int
SYNC: int
Async: int
ASYNC: int
InferMode = int
"""One of [Sync, SYNC, Async, ASYNC]"""



# Classes
class PyParams:
    # Functions
    @_typing.overload
    def __init__(self) -> None: ...
    @_typing.overload
    def __init__(self, tag: str, model: str, weights: str, device: str) -> None: ...
    @_typing.overload
    def __init__(self, tag: str, model: str, device: str) -> None: ...

    def constInput(self, layer_name: str, data: cv2.typing.MatLike, hint: TraitAs = ...) -> PyParams: ...

    def cfgNumRequests(self, nireq: int) -> PyParams: ...

    def cfgBatchSize(self, size: int) -> PyParams: ...



# Functions
@_typing.overload
def params(tag: str, model: str, weights: str, device: str) -> PyParams: ...
@_typing.overload
def params(tag: str, model: str, device: str) -> PyParams: ...


