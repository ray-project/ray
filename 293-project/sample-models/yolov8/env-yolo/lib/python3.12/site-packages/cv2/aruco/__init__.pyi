__all__: list[str] = []

import cv2
import cv2.typing
import typing as _typing


# Enumerations
CORNER_REFINE_NONE: int
CORNER_REFINE_SUBPIX: int
CORNER_REFINE_CONTOUR: int
CORNER_REFINE_APRILTAG: int
CornerRefineMethod = int
"""One of [CORNER_REFINE_NONE, CORNER_REFINE_SUBPIX, CORNER_REFINE_CONTOUR, CORNER_REFINE_APRILTAG]"""

DICT_4X4_50: int
DICT_4X4_100: int
DICT_4X4_250: int
DICT_4X4_1000: int
DICT_5X5_50: int
DICT_5X5_100: int
DICT_5X5_250: int
DICT_5X5_1000: int
DICT_6X6_50: int
DICT_6X6_100: int
DICT_6X6_250: int
DICT_6X6_1000: int
DICT_7X7_50: int
DICT_7X7_100: int
DICT_7X7_250: int
DICT_7X7_1000: int
DICT_ARUCO_ORIGINAL: int
DICT_APRILTAG_16h5: int
DICT_APRILTAG_16H5: int
DICT_APRILTAG_25h9: int
DICT_APRILTAG_25H9: int
DICT_APRILTAG_36h10: int
DICT_APRILTAG_36H10: int
DICT_APRILTAG_36h11: int
DICT_APRILTAG_36H11: int
DICT_ARUCO_MIP_36h12: int
DICT_ARUCO_MIP_36H12: int
PredefinedDictionaryType = int
"""One of [DICT_4X4_50, DICT_4X4_100, DICT_4X4_250, DICT_4X4_1000, DICT_5X5_50, DICT_5X5_100, DICT_5X5_250, DICT_5X5_1000, DICT_6X6_50, DICT_6X6_100, DICT_6X6_250, DICT_6X6_1000, DICT_7X7_50, DICT_7X7_100, DICT_7X7_250, DICT_7X7_1000, DICT_ARUCO_ORIGINAL, DICT_APRILTAG_16h5, DICT_APRILTAG_16H5, DICT_APRILTAG_25h9, DICT_APRILTAG_25H9, DICT_APRILTAG_36h10, DICT_APRILTAG_36H10, DICT_APRILTAG_36h11, DICT_APRILTAG_36H11, DICT_ARUCO_MIP_36h12, DICT_ARUCO_MIP_36H12]"""



# Classes
class Board:
    # Functions
    @_typing.overload
    def __init__(self, objPoints: _typing.Sequence[cv2.typing.MatLike], dictionary: Dictionary, ids: cv2.typing.MatLike) -> None: ...
    @_typing.overload
    def __init__(self, objPoints: _typing.Sequence[cv2.UMat], dictionary: Dictionary, ids: cv2.UMat) -> None: ...

    def getDictionary(self) -> Dictionary: ...

    def getObjPoints(self) -> _typing.Sequence[_typing.Sequence[cv2.typing.Point3f]]: ...

    def getIds(self) -> _typing.Sequence[int]: ...

    def getRightBottomCorner(self) -> cv2.typing.Point3f: ...

    @_typing.overload
    def matchImagePoints(self, detectedCorners: _typing.Sequence[cv2.typing.MatLike], detectedIds: cv2.typing.MatLike, objPoints: cv2.typing.MatLike | None = ..., imgPoints: cv2.typing.MatLike | None = ...) -> tuple[cv2.typing.MatLike, cv2.typing.MatLike]: ...
    @_typing.overload
    def matchImagePoints(self, detectedCorners: _typing.Sequence[cv2.UMat], detectedIds: cv2.UMat, objPoints: cv2.UMat | None = ..., imgPoints: cv2.UMat | None = ...) -> tuple[cv2.UMat, cv2.UMat]: ...

    @_typing.overload
    def generateImage(self, outSize: cv2.typing.Size, img: cv2.typing.MatLike | None = ..., marginSize: int = ..., borderBits: int = ...) -> cv2.typing.MatLike: ...
    @_typing.overload
    def generateImage(self, outSize: cv2.typing.Size, img: cv2.UMat | None = ..., marginSize: int = ..., borderBits: int = ...) -> cv2.UMat: ...


class GridBoard(Board):
    # Functions
    @_typing.overload
    def __init__(self, size: cv2.typing.Size, markerLength: float, markerSeparation: float, dictionary: Dictionary, ids: cv2.typing.MatLike | None = ...) -> None: ...
    @_typing.overload
    def __init__(self, size: cv2.typing.Size, markerLength: float, markerSeparation: float, dictionary: Dictionary, ids: cv2.UMat | None = ...) -> None: ...

    def getGridSize(self) -> cv2.typing.Size: ...

    def getMarkerLength(self) -> float: ...

    def getMarkerSeparation(self) -> float: ...


class CharucoBoard(Board):
    # Functions
    @_typing.overload
    def __init__(self, size: cv2.typing.Size, squareLength: float, markerLength: float, dictionary: Dictionary, ids: cv2.typing.MatLike | None = ...) -> None: ...
    @_typing.overload
    def __init__(self, size: cv2.typing.Size, squareLength: float, markerLength: float, dictionary: Dictionary, ids: cv2.UMat | None = ...) -> None: ...

    def setLegacyPattern(self, legacyPattern: bool) -> None: ...

    def getLegacyPattern(self) -> bool: ...

    def getChessboardSize(self) -> cv2.typing.Size: ...

    def getSquareLength(self) -> float: ...

    def getMarkerLength(self) -> float: ...

    def getChessboardCorners(self) -> _typing.Sequence[cv2.typing.Point3f]: ...

    @_typing.overload
    def checkCharucoCornersCollinear(self, charucoIds: cv2.typing.MatLike) -> bool: ...
    @_typing.overload
    def checkCharucoCornersCollinear(self, charucoIds: cv2.UMat) -> bool: ...


class DetectorParameters:
    adaptiveThreshWinSizeMin: int
    adaptiveThreshWinSizeMax: int
    adaptiveThreshWinSizeStep: int
    adaptiveThreshConstant: float
    minMarkerPerimeterRate: float
    maxMarkerPerimeterRate: float
    polygonalApproxAccuracyRate: float
    minCornerDistanceRate: float
    minDistanceToBorder: int
    minMarkerDistanceRate: float
    minGroupDistance: float
    cornerRefinementMethod: int
    cornerRefinementWinSize: int
    relativeCornerRefinmentWinSize: float
    cornerRefinementMaxIterations: int
    cornerRefinementMinAccuracy: float
    markerBorderBits: int
    perspectiveRemovePixelPerCell: int
    perspectiveRemoveIgnoredMarginPerCell: float
    maxErroneousBitsInBorderRate: float
    minOtsuStdDev: float
    errorCorrectionRate: float
    aprilTagQuadDecimate: float
    aprilTagQuadSigma: float
    aprilTagMinClusterPixels: int
    aprilTagMaxNmaxima: int
    aprilTagCriticalRad: float
    aprilTagMaxLineFitMse: float
    aprilTagMinWhiteBlackDiff: int
    aprilTagDeglitch: int
    detectInvertedMarker: bool
    useAruco3Detection: bool
    minSideLengthCanonicalImg: int
    minMarkerLengthRatioOriginalImg: float

    # Functions
    def __init__(self) -> None: ...

    def readDetectorParameters(self, fn: cv2.FileNode) -> bool: ...

    def writeDetectorParameters(self, fs: cv2.FileStorage, name: str = ...) -> bool: ...


class RefineParameters:
    minRepDistance: float
    errorCorrectionRate: float
    checkAllOrders: bool

    # Functions
    def __init__(self, minRepDistance: float = ..., errorCorrectionRate: float = ..., checkAllOrders: bool = ...) -> None: ...

    def readRefineParameters(self, fn: cv2.FileNode) -> bool: ...

    def writeRefineParameters(self, fs: cv2.FileStorage, name: str = ...) -> bool: ...


class ArucoDetector(cv2.Algorithm):
    # Functions
    def __init__(self, dictionary: Dictionary = ..., detectorParams: DetectorParameters = ..., refineParams: RefineParameters = ...) -> None: ...

    @_typing.overload
    def detectMarkers(self, image: cv2.typing.MatLike, corners: _typing.Sequence[cv2.typing.MatLike] | None = ..., ids: cv2.typing.MatLike | None = ..., rejectedImgPoints: _typing.Sequence[cv2.typing.MatLike] | None = ...) -> tuple[_typing.Sequence[cv2.typing.MatLike], cv2.typing.MatLike, _typing.Sequence[cv2.typing.MatLike]]: ...
    @_typing.overload
    def detectMarkers(self, image: cv2.UMat, corners: _typing.Sequence[cv2.UMat] | None = ..., ids: cv2.UMat | None = ..., rejectedImgPoints: _typing.Sequence[cv2.UMat] | None = ...) -> tuple[_typing.Sequence[cv2.UMat], cv2.UMat, _typing.Sequence[cv2.UMat]]: ...

    @_typing.overload
    def refineDetectedMarkers(self, image: cv2.typing.MatLike, board: Board, detectedCorners: _typing.Sequence[cv2.typing.MatLike], detectedIds: cv2.typing.MatLike, rejectedCorners: _typing.Sequence[cv2.typing.MatLike], cameraMatrix: cv2.typing.MatLike | None = ..., distCoeffs: cv2.typing.MatLike | None = ..., recoveredIdxs: cv2.typing.MatLike | None = ...) -> tuple[_typing.Sequence[cv2.typing.MatLike], cv2.typing.MatLike, _typing.Sequence[cv2.typing.MatLike], cv2.typing.MatLike]: ...
    @_typing.overload
    def refineDetectedMarkers(self, image: cv2.UMat, board: Board, detectedCorners: _typing.Sequence[cv2.UMat], detectedIds: cv2.UMat, rejectedCorners: _typing.Sequence[cv2.UMat], cameraMatrix: cv2.UMat | None = ..., distCoeffs: cv2.UMat | None = ..., recoveredIdxs: cv2.UMat | None = ...) -> tuple[_typing.Sequence[cv2.UMat], cv2.UMat, _typing.Sequence[cv2.UMat], cv2.UMat]: ...

    def getDictionary(self) -> Dictionary: ...

    def setDictionary(self, dictionary: Dictionary) -> None: ...

    def getDetectorParameters(self) -> DetectorParameters: ...

    def setDetectorParameters(self, detectorParameters: DetectorParameters) -> None: ...

    def getRefineParameters(self) -> RefineParameters: ...

    def setRefineParameters(self, refineParameters: RefineParameters) -> None: ...

    def write(self, fs: cv2.FileStorage, name: str) -> None: ...

    def read(self, fn: cv2.FileNode) -> None: ...


class Dictionary:
    bytesList: cv2.typing.MatLike
    markerSize: int
    maxCorrectionBits: int

    # Functions
    @_typing.overload
    def __init__(self) -> None: ...
    @_typing.overload
    def __init__(self, bytesList: cv2.typing.MatLike, _markerSize: int, maxcorr: int = ...) -> None: ...

    def readDictionary(self, fn: cv2.FileNode) -> bool: ...

    def writeDictionary(self, fs: cv2.FileStorage, name: str = ...) -> None: ...

    def identify(self, onlyBits: cv2.typing.MatLike, maxCorrectionRate: float) -> tuple[bool, int, int]: ...

    @_typing.overload
    def getDistanceToId(self, bits: cv2.typing.MatLike, id: int, allRotations: bool = ...) -> int: ...
    @_typing.overload
    def getDistanceToId(self, bits: cv2.UMat, id: int, allRotations: bool = ...) -> int: ...

    @_typing.overload
    def generateImageMarker(self, id: int, sidePixels: int, _img: cv2.typing.MatLike | None = ..., borderBits: int = ...) -> cv2.typing.MatLike: ...
    @_typing.overload
    def generateImageMarker(self, id: int, sidePixels: int, _img: cv2.UMat | None = ..., borderBits: int = ...) -> cv2.UMat: ...

    @staticmethod
    def getByteListFromBits(bits: cv2.typing.MatLike) -> cv2.typing.MatLike: ...

    @staticmethod
    def getBitsFromByteList(byteList: cv2.typing.MatLike, markerSize: int) -> cv2.typing.MatLike: ...


class CharucoParameters:
    cameraMatrix: cv2.typing.MatLike
    distCoeffs: cv2.typing.MatLike
    minMarkers: int
    tryRefineMarkers: bool

    # Functions
    def __init__(self) -> None: ...


class CharucoDetector(cv2.Algorithm):
    # Functions
    def __init__(self, board: CharucoBoard, charucoParams: CharucoParameters = ..., detectorParams: DetectorParameters = ..., refineParams: RefineParameters = ...) -> None: ...

    def getBoard(self) -> CharucoBoard: ...

    def setBoard(self, board: CharucoBoard) -> None: ...

    def getCharucoParameters(self) -> CharucoParameters: ...

    def setCharucoParameters(self, charucoParameters: CharucoParameters) -> None: ...

    def getDetectorParameters(self) -> DetectorParameters: ...

    def setDetectorParameters(self, detectorParameters: DetectorParameters) -> None: ...

    def getRefineParameters(self) -> RefineParameters: ...

    def setRefineParameters(self, refineParameters: RefineParameters) -> None: ...

    @_typing.overload
    def detectBoard(self, image: cv2.typing.MatLike, charucoCorners: cv2.typing.MatLike | None = ..., charucoIds: cv2.typing.MatLike | None = ..., markerCorners: _typing.Sequence[cv2.typing.MatLike] | None = ..., markerIds: cv2.typing.MatLike | None = ...) -> tuple[cv2.typing.MatLike, cv2.typing.MatLike, _typing.Sequence[cv2.typing.MatLike], cv2.typing.MatLike]: ...
    @_typing.overload
    def detectBoard(self, image: cv2.UMat, charucoCorners: cv2.UMat | None = ..., charucoIds: cv2.UMat | None = ..., markerCorners: _typing.Sequence[cv2.UMat] | None = ..., markerIds: cv2.UMat | None = ...) -> tuple[cv2.UMat, cv2.UMat, _typing.Sequence[cv2.UMat], cv2.UMat]: ...

    @_typing.overload
    def detectDiamonds(self, image: cv2.typing.MatLike, diamondCorners: _typing.Sequence[cv2.typing.MatLike] | None = ..., diamondIds: cv2.typing.MatLike | None = ..., markerCorners: _typing.Sequence[cv2.typing.MatLike] | None = ..., markerIds: cv2.typing.MatLike | None = ...) -> tuple[_typing.Sequence[cv2.typing.MatLike], cv2.typing.MatLike, _typing.Sequence[cv2.typing.MatLike], cv2.typing.MatLike]: ...
    @_typing.overload
    def detectDiamonds(self, image: cv2.UMat, diamondCorners: _typing.Sequence[cv2.UMat] | None = ..., diamondIds: cv2.UMat | None = ..., markerCorners: _typing.Sequence[cv2.UMat] | None = ..., markerIds: cv2.UMat | None = ...) -> tuple[_typing.Sequence[cv2.UMat], cv2.UMat, _typing.Sequence[cv2.UMat], cv2.UMat]: ...



# Functions
@_typing.overload
def drawDetectedCornersCharuco(image: cv2.typing.MatLike, charucoCorners: cv2.typing.MatLike, charucoIds: cv2.typing.MatLike | None = ..., cornerColor: cv2.typing.Scalar = ...) -> cv2.typing.MatLike: ...
@_typing.overload
def drawDetectedCornersCharuco(image: cv2.UMat, charucoCorners: cv2.UMat, charucoIds: cv2.UMat | None = ..., cornerColor: cv2.typing.Scalar = ...) -> cv2.UMat: ...

@_typing.overload
def drawDetectedDiamonds(image: cv2.typing.MatLike, diamondCorners: _typing.Sequence[cv2.typing.MatLike], diamondIds: cv2.typing.MatLike | None = ..., borderColor: cv2.typing.Scalar = ...) -> cv2.typing.MatLike: ...
@_typing.overload
def drawDetectedDiamonds(image: cv2.UMat, diamondCorners: _typing.Sequence[cv2.UMat], diamondIds: cv2.UMat | None = ..., borderColor: cv2.typing.Scalar = ...) -> cv2.UMat: ...

@_typing.overload
def drawDetectedMarkers(image: cv2.typing.MatLike, corners: _typing.Sequence[cv2.typing.MatLike], ids: cv2.typing.MatLike | None = ..., borderColor: cv2.typing.Scalar = ...) -> cv2.typing.MatLike: ...
@_typing.overload
def drawDetectedMarkers(image: cv2.UMat, corners: _typing.Sequence[cv2.UMat], ids: cv2.UMat | None = ..., borderColor: cv2.typing.Scalar = ...) -> cv2.UMat: ...

def extendDictionary(nMarkers: int, markerSize: int, baseDictionary: Dictionary = ..., randomSeed: int = ...) -> Dictionary: ...

@_typing.overload
def generateImageMarker(dictionary: Dictionary, id: int, sidePixels: int, img: cv2.typing.MatLike | None = ..., borderBits: int = ...) -> cv2.typing.MatLike: ...
@_typing.overload
def generateImageMarker(dictionary: Dictionary, id: int, sidePixels: int, img: cv2.UMat | None = ..., borderBits: int = ...) -> cv2.UMat: ...

def getPredefinedDictionary(dict: int) -> Dictionary: ...


