__all__ = [
    "IntPointer",
    "MatLike",
    "MatShape",
    "Size",
    "Size2f",
    "Scalar",
    "Point",
    "Point2i",
    "Point2f",
    "Point2d",
    "Point3i",
    "Point3f",
    "Point3d",
    "Range",
    "Rect",
    "Rect2i",
    "Rect2f",
    "Rect2d",
    "Moments",
    "RotatedRect",
    "TermCriteria",
    "Vec2i",
    "Vec2f",
    "Vec2d",
    "Vec3i",
    "Vec3f",
    "Vec3d",
    "Vec4i",
    "Vec4f",
    "Vec4d",
    "Vec6f",
    "FeatureDetector",
    "DescriptorExtractor",
    "FeatureExtractor",
    "GProtoArg",
    "GProtoInputArgs",
    "GProtoOutputArgs",
    "GRunArg",
    "GOptRunArg",
    "GMetaArg",
    "Prim",
    "Matx33f",
    "Matx33d",
    "Matx44f",
    "Matx44d",
    "GTypeInfo",
    "ExtractArgsCallback",
    "ExtractMetaCallback",
    "LayerId",
    "IndexParams",
    "SearchParams",
    "map_string_and_string",
    "map_string_and_int",
    "map_string_and_vector_size_t",
    "map_string_and_vector_float",
    "map_int_and_double",
]

import cv2.dnn
import cv2.gapi.wip.draw
import cv2.mat_wrapper
import numpy
import typing as _typing
import cv2


if _typing.TYPE_CHECKING:
    NumPyArrayNumeric = numpy.ndarray[_typing.Any, numpy.dtype[numpy.integer[_typing.Any] | numpy.floating[_typing.Any]]]
else:
    NumPyArrayNumeric = numpy.ndarray


if _typing.TYPE_CHECKING:
    NumPyArrayFloat32 = numpy.ndarray[_typing.Any, numpy.dtype[numpy.float32]]
else:
    NumPyArrayFloat32 = numpy.ndarray


if _typing.TYPE_CHECKING:
    NumPyArrayFloat64 = numpy.ndarray[_typing.Any, numpy.dtype[numpy.float64]]
else:
    NumPyArrayFloat64 = numpy.ndarray


if _typing.TYPE_CHECKING:
    TermCriteria_Type = cv2.TermCriteria_Type
else:
    TermCriteria_Type = int


IntPointer = int
"""Represents an arbitrary pointer"""
MatLike = _typing.Union[cv2.mat_wrapper.Mat, NumPyArrayNumeric]
MatShape = _typing.Sequence[int]
Size = _typing.Sequence[int]
"""Required length is 2"""
Size2f = _typing.Sequence[float]
"""Required length is 2"""
Scalar = _typing.Sequence[float]
"""Required length is at most 4"""
Point = _typing.Sequence[int]
"""Required length is 2"""
Point2i = Point
Point2f = _typing.Sequence[float]
"""Required length is 2"""
Point2d = _typing.Sequence[float]
"""Required length is 2"""
Point3i = _typing.Sequence[int]
"""Required length is 3"""
Point3f = _typing.Sequence[float]
"""Required length is 3"""
Point3d = _typing.Sequence[float]
"""Required length is 3"""
Range = _typing.Sequence[int]
"""Required length is 2"""
Rect = _typing.Sequence[int]
"""Required length is 4"""
Rect2i = _typing.Sequence[int]
"""Required length is 4"""
Rect2f = _typing.Sequence[float]
"""Required length is 4"""
Rect2d = _typing.Sequence[float]
"""Required length is 4"""
Moments = _typing.Dict[str, float]
RotatedRect = _typing.Tuple[Point2f, Size2f, float]
"""Any type providing sequence protocol is supported"""
TermCriteria = _typing.Tuple[TermCriteria_Type, int, float]
"""Any type providing sequence protocol is supported"""
Vec2i = _typing.Sequence[int]
"""Required length is 2"""
Vec2f = _typing.Sequence[float]
"""Required length is 2"""
Vec2d = _typing.Sequence[float]
"""Required length is 2"""
Vec3i = _typing.Sequence[int]
"""Required length is 3"""
Vec3f = _typing.Sequence[float]
"""Required length is 3"""
Vec3d = _typing.Sequence[float]
"""Required length is 3"""
Vec4i = _typing.Sequence[int]
"""Required length is 4"""
Vec4f = _typing.Sequence[float]
"""Required length is 4"""
Vec4d = _typing.Sequence[float]
"""Required length is 4"""
Vec6f = _typing.Sequence[float]
"""Required length is 6"""
FeatureDetector = cv2.Feature2D
DescriptorExtractor = cv2.Feature2D
FeatureExtractor = cv2.Feature2D
GProtoArg = _typing.Union[Scalar, cv2.GMat, cv2.GOpaqueT, cv2.GArrayT]
GProtoInputArgs = _typing.Sequence[GProtoArg]
GProtoOutputArgs = _typing.Sequence[GProtoArg]
GRunArg = _typing.Union[MatLike, Scalar, cv2.GOpaqueT, cv2.GArrayT, _typing.Sequence[_typing.Any], None]
GOptRunArg = _typing.Optional[GRunArg]
GMetaArg = _typing.Union[cv2.GMat, Scalar, cv2.GOpaqueT, cv2.GArrayT]
Prim = _typing.Union[cv2.gapi.wip.draw.Text, cv2.gapi.wip.draw.Circle, cv2.gapi.wip.draw.Image, cv2.gapi.wip.draw.Line, cv2.gapi.wip.draw.Rect, cv2.gapi.wip.draw.Mosaic, cv2.gapi.wip.draw.Poly]
Matx33f = NumPyArrayFloat32
"""NDArray(shape=(3, 3), dtype=numpy.float32)"""
Matx33d = NumPyArrayFloat64
"""NDArray(shape=(3, 3), dtype=numpy.float64)"""
Matx44f = NumPyArrayFloat32
"""NDArray(shape=(4, 4), dtype=numpy.float32)"""
Matx44d = NumPyArrayFloat64
"""NDArray(shape=(4, 4), dtype=numpy.float64)"""
GTypeInfo = _typing.Union[cv2.GMat, Scalar, cv2.GOpaqueT, cv2.GArrayT]
ExtractArgsCallback = _typing.Callable[[_typing.Sequence[GTypeInfo]], _typing.Sequence[GRunArg]]
ExtractMetaCallback = _typing.Callable[[_typing.Sequence[GTypeInfo]], _typing.Sequence[GMetaArg]]
LayerId = cv2.dnn.DictValue
IndexParams = _typing.Dict[str, _typing.Union[bool, int, float, str]]
SearchParams = _typing.Dict[str, _typing.Union[bool, int, float, str]]
map_string_and_string = _typing.Dict[str, str]
map_string_and_int = _typing.Dict[str, int]
map_string_and_vector_size_t = _typing.Dict[str, _typing.Sequence[int]]
map_string_and_vector_float = _typing.Dict[str, _typing.Sequence[float]]
map_int_and_double = _typing.Dict[int, float]
