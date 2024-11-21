__all__: list[str] = []

import cv2
import cv2.gapi
import cv2.gapi.ie
import cv2.gapi.onnx
import cv2.gapi.ov
import cv2.typing
import numpy
import typing as _typing


# Enumerations
TEST_CUSTOM: int
TEST_EQ: int
TEST_NE: int
TEST_LE: int
TEST_LT: int
TEST_GE: int
TEST_GT: int
TestOp = int
"""One of [TEST_CUSTOM, TEST_EQ, TEST_NE, TEST_LE, TEST_LT, TEST_GE, TEST_GT]"""

WAVE_CORRECT_HORIZ: int
WAVE_CORRECT_VERT: int
WAVE_CORRECT_AUTO: int
WaveCorrectKind = int
"""One of [WAVE_CORRECT_HORIZ, WAVE_CORRECT_VERT, WAVE_CORRECT_AUTO]"""

OpaqueKind_CV_UNKNOWN: int
OPAQUE_KIND_CV_UNKNOWN: int
OpaqueKind_CV_BOOL: int
OPAQUE_KIND_CV_BOOL: int
OpaqueKind_CV_INT: int
OPAQUE_KIND_CV_INT: int
OpaqueKind_CV_INT64: int
OPAQUE_KIND_CV_INT64: int
OpaqueKind_CV_DOUBLE: int
OPAQUE_KIND_CV_DOUBLE: int
OpaqueKind_CV_FLOAT: int
OPAQUE_KIND_CV_FLOAT: int
OpaqueKind_CV_UINT64: int
OPAQUE_KIND_CV_UINT64: int
OpaqueKind_CV_STRING: int
OPAQUE_KIND_CV_STRING: int
OpaqueKind_CV_POINT: int
OPAQUE_KIND_CV_POINT: int
OpaqueKind_CV_POINT2F: int
OPAQUE_KIND_CV_POINT2F: int
OpaqueKind_CV_POINT3F: int
OPAQUE_KIND_CV_POINT3F: int
OpaqueKind_CV_SIZE: int
OPAQUE_KIND_CV_SIZE: int
OpaqueKind_CV_RECT: int
OPAQUE_KIND_CV_RECT: int
OpaqueKind_CV_SCALAR: int
OPAQUE_KIND_CV_SCALAR: int
OpaqueKind_CV_MAT: int
OPAQUE_KIND_CV_MAT: int
OpaqueKind_CV_DRAW_PRIM: int
OPAQUE_KIND_CV_DRAW_PRIM: int
OpaqueKind = int
"""One of [OpaqueKind_CV_UNKNOWN, OPAQUE_KIND_CV_UNKNOWN, OpaqueKind_CV_BOOL, OPAQUE_KIND_CV_BOOL, OpaqueKind_CV_INT, OPAQUE_KIND_CV_INT, OpaqueKind_CV_INT64, OPAQUE_KIND_CV_INT64, OpaqueKind_CV_DOUBLE, OPAQUE_KIND_CV_DOUBLE, OpaqueKind_CV_FLOAT, OPAQUE_KIND_CV_FLOAT, OpaqueKind_CV_UINT64, OPAQUE_KIND_CV_UINT64, OpaqueKind_CV_STRING, OPAQUE_KIND_CV_STRING, OpaqueKind_CV_POINT, OPAQUE_KIND_CV_POINT, OpaqueKind_CV_POINT2F, OPAQUE_KIND_CV_POINT2F, OpaqueKind_CV_POINT3F, OPAQUE_KIND_CV_POINT3F, OpaqueKind_CV_SIZE, OPAQUE_KIND_CV_SIZE, OpaqueKind_CV_RECT, OPAQUE_KIND_CV_RECT, OpaqueKind_CV_SCALAR, OPAQUE_KIND_CV_SCALAR, OpaqueKind_CV_MAT, OPAQUE_KIND_CV_MAT, OpaqueKind_CV_DRAW_PRIM, OPAQUE_KIND_CV_DRAW_PRIM]"""

ArgKind_OPAQUE_VAL: int
ARG_KIND_OPAQUE_VAL: int
ArgKind_OPAQUE: int
ARG_KIND_OPAQUE: int
ArgKind_GOBJREF: int
ARG_KIND_GOBJREF: int
ArgKind_GMAT: int
ARG_KIND_GMAT: int
ArgKind_GMATP: int
ARG_KIND_GMATP: int
ArgKind_GFRAME: int
ARG_KIND_GFRAME: int
ArgKind_GSCALAR: int
ARG_KIND_GSCALAR: int
ArgKind_GARRAY: int
ARG_KIND_GARRAY: int
ArgKind_GOPAQUE: int
ARG_KIND_GOPAQUE: int
ArgKind = int
"""One of [ArgKind_OPAQUE_VAL, ARG_KIND_OPAQUE_VAL, ArgKind_OPAQUE, ARG_KIND_OPAQUE, ArgKind_GOBJREF, ARG_KIND_GOBJREF, ArgKind_GMAT, ARG_KIND_GMAT, ArgKind_GMATP, ARG_KIND_GMATP, ArgKind_GFRAME, ARG_KIND_GFRAME, ArgKind_GSCALAR, ARG_KIND_GSCALAR, ArgKind_GARRAY, ARG_KIND_GARRAY, ArgKind_GOPAQUE, ARG_KIND_GOPAQUE]"""


Blender_NO: int
BLENDER_NO: int
Blender_FEATHER: int
BLENDER_FEATHER: int
Blender_MULTI_BAND: int
BLENDER_MULTI_BAND: int

ExposureCompensator_NO: int
EXPOSURE_COMPENSATOR_NO: int
ExposureCompensator_GAIN: int
EXPOSURE_COMPENSATOR_GAIN: int
ExposureCompensator_GAIN_BLOCKS: int
EXPOSURE_COMPENSATOR_GAIN_BLOCKS: int
ExposureCompensator_CHANNELS: int
EXPOSURE_COMPENSATOR_CHANNELS: int
ExposureCompensator_CHANNELS_BLOCKS: int
EXPOSURE_COMPENSATOR_CHANNELS_BLOCKS: int

SeamFinder_NO: int
SEAM_FINDER_NO: int
SeamFinder_VORONOI_SEAM: int
SEAM_FINDER_VORONOI_SEAM: int
SeamFinder_DP_SEAM: int
SEAM_FINDER_DP_SEAM: int

DpSeamFinder_COLOR: int
DP_SEAM_FINDER_COLOR: int
DpSeamFinder_COLOR_GRAD: int
DP_SEAM_FINDER_COLOR_GRAD: int
DpSeamFinder_CostFunction = int
"""One of [DpSeamFinder_COLOR, DP_SEAM_FINDER_COLOR, DpSeamFinder_COLOR_GRAD, DP_SEAM_FINDER_COLOR_GRAD]"""

Timelapser_AS_IS: int
TIMELAPSER_AS_IS: int
Timelapser_CROP: int
TIMELAPSER_CROP: int

GraphCutSeamFinderBase_COST_COLOR: int
GRAPH_CUT_SEAM_FINDER_BASE_COST_COLOR: int
GraphCutSeamFinderBase_COST_COLOR_GRAD: int
GRAPH_CUT_SEAM_FINDER_BASE_COST_COLOR_GRAD: int
GraphCutSeamFinderBase_CostType = int
"""One of [GraphCutSeamFinderBase_COST_COLOR, GRAPH_CUT_SEAM_FINDER_BASE_COST_COLOR, GraphCutSeamFinderBase_COST_COLOR_GRAD, GRAPH_CUT_SEAM_FINDER_BASE_COST_COLOR_GRAD]"""

TrackerSamplerCSC_MODE_INIT_POS: int
TRACKER_SAMPLER_CSC_MODE_INIT_POS: int
TrackerSamplerCSC_MODE_INIT_NEG: int
TRACKER_SAMPLER_CSC_MODE_INIT_NEG: int
TrackerSamplerCSC_MODE_TRACK_POS: int
TRACKER_SAMPLER_CSC_MODE_TRACK_POS: int
TrackerSamplerCSC_MODE_TRACK_NEG: int
TRACKER_SAMPLER_CSC_MODE_TRACK_NEG: int
TrackerSamplerCSC_MODE_DETECT: int
TRACKER_SAMPLER_CSC_MODE_DETECT: int
TrackerSamplerCSC_MODE = int
"""One of [TrackerSamplerCSC_MODE_INIT_POS, TRACKER_SAMPLER_CSC_MODE_INIT_POS, TrackerSamplerCSC_MODE_INIT_NEG, TRACKER_SAMPLER_CSC_MODE_INIT_NEG, TrackerSamplerCSC_MODE_TRACK_POS, TRACKER_SAMPLER_CSC_MODE_TRACK_POS, TrackerSamplerCSC_MODE_TRACK_NEG, TRACKER_SAMPLER_CSC_MODE_TRACK_NEG, TrackerSamplerCSC_MODE_DETECT, TRACKER_SAMPLER_CSC_MODE_DETECT]"""


# Classes
class Blender:
    # Functions
    @classmethod
    def createDefault(cls, type: int, try_gpu: bool = ...) -> Blender: ...

    @_typing.overload
    def prepare(self, corners: _typing.Sequence[cv2.typing.Point], sizes: _typing.Sequence[cv2.typing.Size]) -> None: ...
    @_typing.overload
    def prepare(self, dst_roi: cv2.typing.Rect) -> None: ...

    @_typing.overload
    def feed(self, img: cv2.typing.MatLike, mask: cv2.typing.MatLike, tl: cv2.typing.Point) -> None: ...
    @_typing.overload
    def feed(self, img: cv2.UMat, mask: cv2.UMat, tl: cv2.typing.Point) -> None: ...

    @_typing.overload
    def blend(self, dst: cv2.typing.MatLike, dst_mask: cv2.typing.MatLike) -> tuple[cv2.typing.MatLike, cv2.typing.MatLike]: ...
    @_typing.overload
    def blend(self, dst: cv2.UMat, dst_mask: cv2.UMat) -> tuple[cv2.UMat, cv2.UMat]: ...


class FeatherBlender(Blender):
    # Functions
    def __init__(self, sharpness: float = ...) -> None: ...

    def sharpness(self) -> float: ...

    def setSharpness(self, val: float) -> None: ...

    def prepare(self, dst_roi: cv2.typing.Rect) -> None: ...

    @_typing.overload
    def feed(self, img: cv2.typing.MatLike, mask: cv2.typing.MatLike, tl: cv2.typing.Point) -> None: ...
    @_typing.overload
    def feed(self, img: cv2.UMat, mask: cv2.UMat, tl: cv2.typing.Point) -> None: ...

    @_typing.overload
    def blend(self, dst: cv2.typing.MatLike, dst_mask: cv2.typing.MatLike) -> tuple[cv2.typing.MatLike, cv2.typing.MatLike]: ...
    @_typing.overload
    def blend(self, dst: cv2.UMat, dst_mask: cv2.UMat) -> tuple[cv2.UMat, cv2.UMat]: ...

    def createWeightMaps(self, masks: _typing.Sequence[cv2.UMat], corners: _typing.Sequence[cv2.typing.Point], weight_maps: _typing.Sequence[cv2.UMat]) -> tuple[cv2.typing.Rect, _typing.Sequence[cv2.UMat]]: ...


class MultiBandBlender(Blender):
    # Functions
    def __init__(self, try_gpu: int = ..., num_bands: int = ..., weight_type: int = ...) -> None: ...

    def numBands(self) -> int: ...

    def setNumBands(self, val: int) -> None: ...

    def prepare(self, dst_roi: cv2.typing.Rect) -> None: ...

    @_typing.overload
    def feed(self, img: cv2.typing.MatLike, mask: cv2.typing.MatLike, tl: cv2.typing.Point) -> None: ...
    @_typing.overload
    def feed(self, img: cv2.UMat, mask: cv2.UMat, tl: cv2.typing.Point) -> None: ...

    @_typing.overload
    def blend(self, dst: cv2.typing.MatLike, dst_mask: cv2.typing.MatLike) -> tuple[cv2.typing.MatLike, cv2.typing.MatLike]: ...
    @_typing.overload
    def blend(self, dst: cv2.UMat, dst_mask: cv2.UMat) -> tuple[cv2.UMat, cv2.UMat]: ...


class CameraParams:
    focal: float
    aspect: float
    ppx: float
    ppy: float
    R: cv2.typing.MatLike
    t: cv2.typing.MatLike

    # Functions
    def K(self) -> cv2.typing.MatLike: ...


class ExposureCompensator:
    # Functions
    @classmethod
    def createDefault(cls, type: int) -> ExposureCompensator: ...

    def feed(self, corners: _typing.Sequence[cv2.typing.Point], images: _typing.Sequence[cv2.UMat], masks: _typing.Sequence[cv2.UMat]) -> None: ...

    @_typing.overload
    def apply(self, index: int, corner: cv2.typing.Point, image: cv2.typing.MatLike, mask: cv2.typing.MatLike) -> cv2.typing.MatLike: ...
    @_typing.overload
    def apply(self, index: int, corner: cv2.typing.Point, image: cv2.UMat, mask: cv2.UMat) -> cv2.UMat: ...

    def getMatGains(self, arg1: _typing.Sequence[cv2.typing.MatLike] | None = ...) -> _typing.Sequence[cv2.typing.MatLike]: ...

    def setMatGains(self, arg1: _typing.Sequence[cv2.typing.MatLike]) -> None: ...

    def setUpdateGain(self, b: bool) -> None: ...

    def getUpdateGain(self) -> bool: ...


class NoExposureCompensator(ExposureCompensator):
    # Functions
    @_typing.overload
    def apply(self, arg1: int, arg2: cv2.typing.Point, arg3: cv2.typing.MatLike, arg4: cv2.typing.MatLike) -> cv2.typing.MatLike: ...
    @_typing.overload
    def apply(self, arg1: int, arg2: cv2.typing.Point, arg3: cv2.UMat, arg4: cv2.UMat) -> cv2.UMat: ...

    def getMatGains(self, umv: _typing.Sequence[cv2.typing.MatLike] | None = ...) -> _typing.Sequence[cv2.typing.MatLike]: ...

    def setMatGains(self, umv: _typing.Sequence[cv2.typing.MatLike]) -> None: ...


class GainCompensator(ExposureCompensator):
    # Functions
    @_typing.overload
    def __init__(self) -> None: ...
    @_typing.overload
    def __init__(self, nr_feeds: int) -> None: ...

    @_typing.overload
    def apply(self, index: int, corner: cv2.typing.Point, image: cv2.typing.MatLike, mask: cv2.typing.MatLike) -> cv2.typing.MatLike: ...
    @_typing.overload
    def apply(self, index: int, corner: cv2.typing.Point, image: cv2.UMat, mask: cv2.UMat) -> cv2.UMat: ...

    def getMatGains(self, umv: _typing.Sequence[cv2.typing.MatLike] | None = ...) -> _typing.Sequence[cv2.typing.MatLike]: ...

    def setMatGains(self, umv: _typing.Sequence[cv2.typing.MatLike]) -> None: ...

    def setNrFeeds(self, nr_feeds: int) -> None: ...

    def getNrFeeds(self) -> int: ...

    def setSimilarityThreshold(self, similarity_threshold: float) -> None: ...

    def getSimilarityThreshold(self) -> float: ...


class ChannelsCompensator(ExposureCompensator):
    # Functions
    def __init__(self, nr_feeds: int = ...) -> None: ...

    @_typing.overload
    def apply(self, index: int, corner: cv2.typing.Point, image: cv2.typing.MatLike, mask: cv2.typing.MatLike) -> cv2.typing.MatLike: ...
    @_typing.overload
    def apply(self, index: int, corner: cv2.typing.Point, image: cv2.UMat, mask: cv2.UMat) -> cv2.UMat: ...

    def getMatGains(self, umv: _typing.Sequence[cv2.typing.MatLike] | None = ...) -> _typing.Sequence[cv2.typing.MatLike]: ...

    def setMatGains(self, umv: _typing.Sequence[cv2.typing.MatLike]) -> None: ...

    def setNrFeeds(self, nr_feeds: int) -> None: ...

    def getNrFeeds(self) -> int: ...

    def setSimilarityThreshold(self, similarity_threshold: float) -> None: ...

    def getSimilarityThreshold(self) -> float: ...


class BlocksCompensator(ExposureCompensator):
    # Functions
    @_typing.overload
    def apply(self, index: int, corner: cv2.typing.Point, image: cv2.typing.MatLike, mask: cv2.typing.MatLike) -> cv2.typing.MatLike: ...
    @_typing.overload
    def apply(self, index: int, corner: cv2.typing.Point, image: cv2.UMat, mask: cv2.UMat) -> cv2.UMat: ...

    def getMatGains(self, umv: _typing.Sequence[cv2.typing.MatLike] | None = ...) -> _typing.Sequence[cv2.typing.MatLike]: ...

    def setMatGains(self, umv: _typing.Sequence[cv2.typing.MatLike]) -> None: ...

    def setNrFeeds(self, nr_feeds: int) -> None: ...

    def getNrFeeds(self) -> int: ...

    def setSimilarityThreshold(self, similarity_threshold: float) -> None: ...

    def getSimilarityThreshold(self) -> float: ...

    @_typing.overload
    def setBlockSize(self, width: int, height: int) -> None: ...
    @_typing.overload
    def setBlockSize(self, size: cv2.typing.Size) -> None: ...

    def getBlockSize(self) -> cv2.typing.Size: ...

    def setNrGainsFilteringIterations(self, nr_iterations: int) -> None: ...

    def getNrGainsFilteringIterations(self) -> int: ...


class BlocksGainCompensator(BlocksCompensator):
    # Functions
    @_typing.overload
    def __init__(self, bl_width: int = ..., bl_height: int = ...) -> None: ...
    @_typing.overload
    def __init__(self, bl_width: int, bl_height: int, nr_feeds: int) -> None: ...

    @_typing.overload
    def apply(self, index: int, corner: cv2.typing.Point, image: cv2.typing.MatLike, mask: cv2.typing.MatLike) -> cv2.typing.MatLike: ...
    @_typing.overload
    def apply(self, index: int, corner: cv2.typing.Point, image: cv2.UMat, mask: cv2.UMat) -> cv2.UMat: ...

    def getMatGains(self, umv: _typing.Sequence[cv2.typing.MatLike] | None = ...) -> _typing.Sequence[cv2.typing.MatLike]: ...

    def setMatGains(self, umv: _typing.Sequence[cv2.typing.MatLike]) -> None: ...


class BlocksChannelsCompensator(BlocksCompensator):
    # Functions
    def __init__(self, bl_width: int = ..., bl_height: int = ..., nr_feeds: int = ...) -> None: ...


class ImageFeatures:
    img_idx: int
    img_size: cv2.typing.Size
    keypoints: _typing.Sequence[cv2.KeyPoint]
    descriptors: cv2.UMat

    # Functions
    def getKeypoints(self) -> _typing.Sequence[cv2.KeyPoint]: ...


class MatchesInfo:
    src_img_idx: int
    dst_img_idx: int
    matches: _typing.Sequence[cv2.DMatch]
    inliers_mask: numpy.ndarray[_typing.Any, numpy.dtype[numpy.uint8]]
    num_inliers: int
    H: cv2.typing.MatLike
    confidence: float

    # Functions
    def getMatches(self) -> _typing.Sequence[cv2.DMatch]: ...

    def getInliers(self) -> numpy.ndarray[_typing.Any, numpy.dtype[numpy.uint8]]: ...


class FeaturesMatcher:
    # Functions
    def apply(self, features1: ImageFeatures, features2: ImageFeatures) -> MatchesInfo: ...

    def apply2(self, features: _typing.Sequence[ImageFeatures], mask: cv2.UMat | None = ...) -> _typing.Sequence[MatchesInfo]: ...

    def isThreadSafe(self) -> bool: ...

    def collectGarbage(self) -> None: ...


class BestOf2NearestMatcher(FeaturesMatcher):
    # Functions
    def __init__(self, try_use_gpu: bool = ..., match_conf: float = ..., num_matches_thresh1: int = ..., num_matches_thresh2: int = ..., matches_confindece_thresh: float = ...) -> None: ...

    def collectGarbage(self) -> None: ...

    @classmethod
    def create(cls, try_use_gpu: bool = ..., match_conf: float = ..., num_matches_thresh1: int = ..., num_matches_thresh2: int = ..., matches_confindece_thresh: float = ...) -> BestOf2NearestMatcher: ...


class BestOf2NearestRangeMatcher(BestOf2NearestMatcher):
    # Functions
    def __init__(self, range_width: int = ..., try_use_gpu: bool = ..., match_conf: float = ..., num_matches_thresh1: int = ..., num_matches_thresh2: int = ...) -> None: ...


class AffineBestOf2NearestMatcher(BestOf2NearestMatcher):
    # Functions
    def __init__(self, full_affine: bool = ..., try_use_gpu: bool = ..., match_conf: float = ..., num_matches_thresh1: int = ...) -> None: ...


class Estimator:
    # Functions
    def apply(self, features: _typing.Sequence[ImageFeatures], pairwise_matches: _typing.Sequence[MatchesInfo], cameras: _typing.Sequence[CameraParams]) -> tuple[bool, _typing.Sequence[CameraParams]]: ...


class HomographyBasedEstimator(Estimator):
    # Functions
    def __init__(self, is_focals_estimated: bool = ...) -> None: ...


class AffineBasedEstimator(Estimator):
    # Functions
    def __init__(self) -> None: ...


class BundleAdjusterBase(Estimator):
    # Functions
    def refinementMask(self) -> cv2.typing.MatLike: ...

    def setRefinementMask(self, mask: cv2.typing.MatLike) -> None: ...

    def confThresh(self) -> float: ...

    def setConfThresh(self, conf_thresh: float) -> None: ...

    def termCriteria(self) -> cv2.typing.TermCriteria: ...

    def setTermCriteria(self, term_criteria: cv2.typing.TermCriteria) -> None: ...


class NoBundleAdjuster(BundleAdjusterBase):
    # Functions
    def __init__(self) -> None: ...


class BundleAdjusterReproj(BundleAdjusterBase):
    # Functions
    def __init__(self) -> None: ...


class BundleAdjusterRay(BundleAdjusterBase):
    # Functions
    def __init__(self) -> None: ...


class BundleAdjusterAffine(BundleAdjusterBase):
    # Functions
    def __init__(self) -> None: ...


class BundleAdjusterAffinePartial(BundleAdjusterBase):
    # Functions
    def __init__(self) -> None: ...


class SeamFinder:
    # Functions
    def find(self, src: _typing.Sequence[cv2.UMat], corners: _typing.Sequence[cv2.typing.Point], masks: _typing.Sequence[cv2.UMat]) -> _typing.Sequence[cv2.UMat]: ...

    @classmethod
    def createDefault(cls, type: int) -> SeamFinder: ...


class NoSeamFinder(SeamFinder):
    # Functions
    def find(self, arg1: _typing.Sequence[cv2.UMat], arg2: _typing.Sequence[cv2.typing.Point], arg3: _typing.Sequence[cv2.UMat]) -> _typing.Sequence[cv2.UMat]: ...


class PairwiseSeamFinder(SeamFinder):
    # Functions
    def find(self, src: _typing.Sequence[cv2.UMat], corners: _typing.Sequence[cv2.typing.Point], masks: _typing.Sequence[cv2.UMat]) -> _typing.Sequence[cv2.UMat]: ...


class VoronoiSeamFinder(PairwiseSeamFinder):
    # Functions
    def find(self, src: _typing.Sequence[cv2.UMat], corners: _typing.Sequence[cv2.typing.Point], masks: _typing.Sequence[cv2.UMat]) -> _typing.Sequence[cv2.UMat]: ...


class DpSeamFinder(SeamFinder):
    # Functions
    def __init__(self, costFunc: str) -> None: ...

    def setCostFunction(self, val: str) -> None: ...


class GraphCutSeamFinder:
    # Functions
    def __init__(self, cost_type: str, terminal_cost: float = ..., bad_region_penalty: float = ...) -> None: ...

    def find(self, src: _typing.Sequence[cv2.UMat], corners: _typing.Sequence[cv2.typing.Point], masks: _typing.Sequence[cv2.UMat]) -> _typing.Sequence[cv2.UMat]: ...


class Timelapser:
    # Functions
    @classmethod
    def createDefault(cls, type: int) -> Timelapser: ...

    def initialize(self, corners: _typing.Sequence[cv2.typing.Point], sizes: _typing.Sequence[cv2.typing.Size]) -> None: ...

    @_typing.overload
    def process(self, img: cv2.typing.MatLike, mask: cv2.typing.MatLike, tl: cv2.typing.Point) -> None: ...
    @_typing.overload
    def process(self, img: cv2.UMat, mask: cv2.UMat, tl: cv2.typing.Point) -> None: ...

    def getDst(self) -> cv2.UMat: ...


class TimelapserCrop(Timelapser):
    ...

class ProjectorBase:
    ...

class SphericalProjector(ProjectorBase):
    # Functions
    def mapForward(self, x: float, y: float, u: float, v: float) -> None: ...

    def mapBackward(self, u: float, v: float, x: float, y: float) -> None: ...



# Functions
def calibrateRotatingCamera(Hs: _typing.Sequence[cv2.typing.MatLike], K: cv2.typing.MatLike | None = ...) -> tuple[bool, cv2.typing.MatLike]: ...

@_typing.overload
def computeImageFeatures(featuresFinder: cv2.Feature2D, images: _typing.Sequence[cv2.typing.MatLike], masks: _typing.Sequence[cv2.typing.MatLike] | None = ...) -> _typing.Sequence[ImageFeatures]: ...
@_typing.overload
def computeImageFeatures(featuresFinder: cv2.Feature2D, images: _typing.Sequence[cv2.UMat], masks: _typing.Sequence[cv2.UMat] | None = ...) -> _typing.Sequence[ImageFeatures]: ...

@_typing.overload
def computeImageFeatures2(featuresFinder: cv2.Feature2D, image: cv2.typing.MatLike, mask: cv2.typing.MatLike | None = ...) -> ImageFeatures: ...
@_typing.overload
def computeImageFeatures2(featuresFinder: cv2.Feature2D, image: cv2.UMat, mask: cv2.UMat | None = ...) -> ImageFeatures: ...

@_typing.overload
def createLaplacePyr(img: cv2.typing.MatLike, num_levels: int, pyr: _typing.Sequence[cv2.UMat]) -> _typing.Sequence[cv2.UMat]: ...
@_typing.overload
def createLaplacePyr(img: cv2.UMat, num_levels: int, pyr: _typing.Sequence[cv2.UMat]) -> _typing.Sequence[cv2.UMat]: ...

@_typing.overload
def createLaplacePyrGpu(img: cv2.typing.MatLike, num_levels: int, pyr: _typing.Sequence[cv2.UMat]) -> _typing.Sequence[cv2.UMat]: ...
@_typing.overload
def createLaplacePyrGpu(img: cv2.UMat, num_levels: int, pyr: _typing.Sequence[cv2.UMat]) -> _typing.Sequence[cv2.UMat]: ...

@_typing.overload
def createWeightMap(mask: cv2.typing.MatLike, sharpness: float, weight: cv2.typing.MatLike) -> cv2.typing.MatLike: ...
@_typing.overload
def createWeightMap(mask: cv2.UMat, sharpness: float, weight: cv2.UMat) -> cv2.UMat: ...

def focalsFromHomography(H: cv2.typing.MatLike, f0: float, f1: float, f0_ok: bool, f1_ok: bool) -> None: ...

def leaveBiggestComponent(features: _typing.Sequence[ImageFeatures], pairwise_matches: _typing.Sequence[MatchesInfo], conf_threshold: float) -> _typing.Sequence[int]: ...

def matchesGraphAsString(paths: _typing.Sequence[str], pairwise_matches: _typing.Sequence[MatchesInfo], conf_threshold: float) -> str: ...

@_typing.overload
def normalizeUsingWeightMap(weight: cv2.typing.MatLike, src: cv2.typing.MatLike) -> cv2.typing.MatLike: ...
@_typing.overload
def normalizeUsingWeightMap(weight: cv2.UMat, src: cv2.UMat) -> cv2.UMat: ...

def overlapRoi(tl1: cv2.typing.Point, tl2: cv2.typing.Point, sz1: cv2.typing.Size, sz2: cv2.typing.Size, roi: cv2.typing.Rect) -> bool: ...

def restoreImageFromLaplacePyr(pyr: _typing.Sequence[cv2.UMat]) -> _typing.Sequence[cv2.UMat]: ...

def restoreImageFromLaplacePyrGpu(pyr: _typing.Sequence[cv2.UMat]) -> _typing.Sequence[cv2.UMat]: ...

@_typing.overload
def resultRoi(corners: _typing.Sequence[cv2.typing.Point], images: _typing.Sequence[cv2.UMat]) -> cv2.typing.Rect: ...
@_typing.overload
def resultRoi(corners: _typing.Sequence[cv2.typing.Point], sizes: _typing.Sequence[cv2.typing.Size]) -> cv2.typing.Rect: ...

def resultRoiIntersection(corners: _typing.Sequence[cv2.typing.Point], sizes: _typing.Sequence[cv2.typing.Size]) -> cv2.typing.Rect: ...

def resultTl(corners: _typing.Sequence[cv2.typing.Point]) -> cv2.typing.Point: ...

def selectRandomSubset(count: int, size: int, subset: _typing.Sequence[int]) -> None: ...

def stitchingLogLevel() -> int: ...

@_typing.overload
def strip(params: cv2.gapi.ie.PyParams) -> cv2.gapi.GNetParam: ...
@_typing.overload
def strip(params: cv2.gapi.onnx.PyParams) -> cv2.gapi.GNetParam: ...
@_typing.overload
def strip(params: cv2.gapi.ov.PyParams) -> cv2.gapi.GNetParam: ...

def waveCorrect(rmats: _typing.Sequence[cv2.typing.MatLike], kind: WaveCorrectKind) -> _typing.Sequence[cv2.typing.MatLike]: ...


