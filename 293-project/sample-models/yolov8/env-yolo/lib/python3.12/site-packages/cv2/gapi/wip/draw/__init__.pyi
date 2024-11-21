__all__: list[str] = []

import cv2
import cv2.typing
import typing as _typing


# Classes
class Text:
    text: str
    org: cv2.typing.Point
    ff: int
    fs: float
    color: cv2.typing.Scalar
    thick: int
    lt: int
    bottom_left_origin: bool

    # Functions
    @_typing.overload
    def __init__(self, text_: str, org_: cv2.typing.Point, ff_: int, fs_: float, color_: cv2.typing.Scalar, thick_: int = ..., lt_: int = ..., bottom_left_origin_: bool = ...) -> None: ...
    @_typing.overload
    def __init__(self) -> None: ...


class Rect:
    rect: cv2.typing.Rect
    color: cv2.typing.Scalar
    thick: int
    lt: int
    shift: int

    # Functions
    @_typing.overload
    def __init__(self) -> None: ...
    @_typing.overload
    def __init__(self, rect_: cv2.typing.Rect2i, color_: cv2.typing.Scalar, thick_: int = ..., lt_: int = ..., shift_: int = ...) -> None: ...


class Circle:
    center: cv2.typing.Point
    radius: int
    color: cv2.typing.Scalar
    thick: int
    lt: int
    shift: int

    # Functions
    @_typing.overload
    def __init__(self, center_: cv2.typing.Point, radius_: int, color_: cv2.typing.Scalar, thick_: int = ..., lt_: int = ..., shift_: int = ...) -> None: ...
    @_typing.overload
    def __init__(self) -> None: ...


class Line:
    pt1: cv2.typing.Point
    pt2: cv2.typing.Point
    color: cv2.typing.Scalar
    thick: int
    lt: int
    shift: int

    # Functions
    @_typing.overload
    def __init__(self, pt1_: cv2.typing.Point, pt2_: cv2.typing.Point, color_: cv2.typing.Scalar, thick_: int = ..., lt_: int = ..., shift_: int = ...) -> None: ...
    @_typing.overload
    def __init__(self) -> None: ...


class Mosaic:
    mos: cv2.typing.Rect
    cellSz: int
    decim: int

    # Functions
    @_typing.overload
    def __init__(self) -> None: ...
    @_typing.overload
    def __init__(self, mos_: cv2.typing.Rect2i, cellSz_: int, decim_: int) -> None: ...


class Image:
    org: cv2.typing.Point
    img: cv2.typing.MatLike
    alpha: cv2.typing.MatLike

    # Functions
    @_typing.overload
    def __init__(self, org_: cv2.typing.Point, img_: cv2.typing.MatLike, alpha_: cv2.typing.MatLike) -> None: ...
    @_typing.overload
    def __init__(self) -> None: ...


class Poly:
    points: _typing.Sequence[cv2.typing.Point]
    color: cv2.typing.Scalar
    thick: int
    lt: int
    shift: int

    # Functions
    @_typing.overload
    def __init__(self, points_: _typing.Sequence[cv2.typing.Point], color_: cv2.typing.Scalar, thick_: int = ..., lt_: int = ..., shift_: int = ...) -> None: ...
    @_typing.overload
    def __init__(self) -> None: ...



# Functions
@_typing.overload
def render(bgr: cv2.typing.MatLike, prims: _typing.Sequence[cv2.typing.Prim], args: _typing.Sequence[cv2.GCompileArg] = ...) -> None: ...
@_typing.overload
def render(y_plane: cv2.typing.MatLike, uv_plane: cv2.typing.MatLike, prims: _typing.Sequence[cv2.typing.Prim], args: _typing.Sequence[cv2.GCompileArg] = ...) -> None: ...

def render3ch(src: cv2.GMat, prims: cv2.GArrayT) -> cv2.GMat: ...

def renderNV12(y: cv2.GMat, uv: cv2.GMat, prims: cv2.GArrayT) -> tuple[cv2.GMat, cv2.GMat]: ...


