__all__: list[str] = []

import cv2.gapi.onnx.ep
import cv2.typing
import typing as _typing


from cv2.gapi.onnx import ep as ep


# Enumerations
TraitAs_TENSOR: int
TRAIT_AS_TENSOR: int
TraitAs_IMAGE: int
TRAIT_AS_IMAGE: int
TraitAs = int
"""One of [TraitAs_TENSOR, TRAIT_AS_TENSOR, TraitAs_IMAGE, TRAIT_AS_IMAGE]"""



# Classes
class PyParams:
    # Functions
    @_typing.overload
    def __init__(self) -> None: ...
    @_typing.overload
    def __init__(self, tag: str, model_path: str) -> None: ...

    def cfgMeanStd(self, layer_name: str, m: cv2.typing.Scalar, s: cv2.typing.Scalar) -> PyParams: ...

    def cfgNormalize(self, layer_name: str, flag: bool) -> PyParams: ...

    @_typing.overload
    def cfgAddExecutionProvider(self, ep: cv2.gapi.onnx.ep.OpenVINO) -> PyParams: ...
    @_typing.overload
    def cfgAddExecutionProvider(self, ep: cv2.gapi.onnx.ep.DirectML) -> PyParams: ...
    @_typing.overload
    def cfgAddExecutionProvider(self, ep: cv2.gapi.onnx.ep.CoreML) -> PyParams: ...
    @_typing.overload
    def cfgAddExecutionProvider(self, ep: cv2.gapi.onnx.ep.CUDA) -> PyParams: ...
    @_typing.overload
    def cfgAddExecutionProvider(self, ep: cv2.gapi.onnx.ep.TensorRT) -> PyParams: ...

    def cfgDisableMemPattern(self) -> PyParams: ...



# Functions
def params(tag: str, model_path: str) -> PyParams: ...


