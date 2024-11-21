__all__: list[str] = []

import cv2
import cv2.typing
import typing as _typing


# Enumerations
CALIB_USE_INTRINSIC_GUESS: int
CALIB_RECOMPUTE_EXTRINSIC: int
CALIB_CHECK_COND: int
CALIB_FIX_SKEW: int
CALIB_FIX_K1: int
CALIB_FIX_K2: int
CALIB_FIX_K3: int
CALIB_FIX_K4: int
CALIB_FIX_INTRINSIC: int
CALIB_FIX_PRINCIPAL_POINT: int
CALIB_ZERO_DISPARITY: int
CALIB_FIX_FOCAL_LENGTH: int



# Functions
@_typing.overload
def calibrate(objectPoints: _typing.Sequence[cv2.typing.MatLike], imagePoints: _typing.Sequence[cv2.typing.MatLike], image_size: cv2.typing.Size, K: cv2.typing.MatLike, D: cv2.typing.MatLike, rvecs: _typing.Sequence[cv2.typing.MatLike] | None = ..., tvecs: _typing.Sequence[cv2.typing.MatLike] | None = ..., flags: int = ..., criteria: cv2.typing.TermCriteria = ...) -> tuple[float, cv2.typing.MatLike, cv2.typing.MatLike, _typing.Sequence[cv2.typing.MatLike], _typing.Sequence[cv2.typing.MatLike]]: ...
@_typing.overload
def calibrate(objectPoints: _typing.Sequence[cv2.UMat], imagePoints: _typing.Sequence[cv2.UMat], image_size: cv2.typing.Size, K: cv2.UMat, D: cv2.UMat, rvecs: _typing.Sequence[cv2.UMat] | None = ..., tvecs: _typing.Sequence[cv2.UMat] | None = ..., flags: int = ..., criteria: cv2.typing.TermCriteria = ...) -> tuple[float, cv2.UMat, cv2.UMat, _typing.Sequence[cv2.UMat], _typing.Sequence[cv2.UMat]]: ...

@_typing.overload
def distortPoints(undistorted: cv2.typing.MatLike, K: cv2.typing.MatLike, D: cv2.typing.MatLike, distorted: cv2.typing.MatLike | None = ..., alpha: float = ...) -> cv2.typing.MatLike: ...
@_typing.overload
def distortPoints(undistorted: cv2.UMat, K: cv2.UMat, D: cv2.UMat, distorted: cv2.UMat | None = ..., alpha: float = ...) -> cv2.UMat: ...

@_typing.overload
def estimateNewCameraMatrixForUndistortRectify(K: cv2.typing.MatLike, D: cv2.typing.MatLike, image_size: cv2.typing.Size, R: cv2.typing.MatLike, P: cv2.typing.MatLike | None = ..., balance: float = ..., new_size: cv2.typing.Size = ..., fov_scale: float = ...) -> cv2.typing.MatLike: ...
@_typing.overload
def estimateNewCameraMatrixForUndistortRectify(K: cv2.UMat, D: cv2.UMat, image_size: cv2.typing.Size, R: cv2.UMat, P: cv2.UMat | None = ..., balance: float = ..., new_size: cv2.typing.Size = ..., fov_scale: float = ...) -> cv2.UMat: ...

@_typing.overload
def initUndistortRectifyMap(K: cv2.typing.MatLike, D: cv2.typing.MatLike, R: cv2.typing.MatLike, P: cv2.typing.MatLike, size: cv2.typing.Size, m1type: int, map1: cv2.typing.MatLike | None = ..., map2: cv2.typing.MatLike | None = ...) -> tuple[cv2.typing.MatLike, cv2.typing.MatLike]: ...
@_typing.overload
def initUndistortRectifyMap(K: cv2.UMat, D: cv2.UMat, R: cv2.UMat, P: cv2.UMat, size: cv2.typing.Size, m1type: int, map1: cv2.UMat | None = ..., map2: cv2.UMat | None = ...) -> tuple[cv2.UMat, cv2.UMat]: ...

@_typing.overload
def projectPoints(objectPoints: cv2.typing.MatLike, rvec: cv2.typing.MatLike, tvec: cv2.typing.MatLike, K: cv2.typing.MatLike, D: cv2.typing.MatLike, imagePoints: cv2.typing.MatLike | None = ..., alpha: float = ..., jacobian: cv2.typing.MatLike | None = ...) -> tuple[cv2.typing.MatLike, cv2.typing.MatLike]: ...
@_typing.overload
def projectPoints(objectPoints: cv2.UMat, rvec: cv2.UMat, tvec: cv2.UMat, K: cv2.UMat, D: cv2.UMat, imagePoints: cv2.UMat | None = ..., alpha: float = ..., jacobian: cv2.UMat | None = ...) -> tuple[cv2.UMat, cv2.UMat]: ...

@_typing.overload
def solvePnP(objectPoints: cv2.typing.MatLike, imagePoints: cv2.typing.MatLike, cameraMatrix: cv2.typing.MatLike, distCoeffs: cv2.typing.MatLike, rvec: cv2.typing.MatLike | None = ..., tvec: cv2.typing.MatLike | None = ..., useExtrinsicGuess: bool = ..., flags: int = ..., criteria: cv2.typing.TermCriteria = ...) -> tuple[bool, cv2.typing.MatLike, cv2.typing.MatLike]: ...
@_typing.overload
def solvePnP(objectPoints: cv2.UMat, imagePoints: cv2.UMat, cameraMatrix: cv2.UMat, distCoeffs: cv2.UMat, rvec: cv2.UMat | None = ..., tvec: cv2.UMat | None = ..., useExtrinsicGuess: bool = ..., flags: int = ..., criteria: cv2.typing.TermCriteria = ...) -> tuple[bool, cv2.UMat, cv2.UMat]: ...

@_typing.overload
def stereoCalibrate(objectPoints: _typing.Sequence[cv2.typing.MatLike], imagePoints1: _typing.Sequence[cv2.typing.MatLike], imagePoints2: _typing.Sequence[cv2.typing.MatLike], K1: cv2.typing.MatLike, D1: cv2.typing.MatLike, K2: cv2.typing.MatLike, D2: cv2.typing.MatLike, imageSize: cv2.typing.Size, R: cv2.typing.MatLike | None = ..., T: cv2.typing.MatLike | None = ..., rvecs: _typing.Sequence[cv2.typing.MatLike] | None = ..., tvecs: _typing.Sequence[cv2.typing.MatLike] | None = ..., flags: int = ..., criteria: cv2.typing.TermCriteria = ...) -> tuple[float, cv2.typing.MatLike, cv2.typing.MatLike, cv2.typing.MatLike, cv2.typing.MatLike, cv2.typing.MatLike, cv2.typing.MatLike, _typing.Sequence[cv2.typing.MatLike], _typing.Sequence[cv2.typing.MatLike]]: ...
@_typing.overload
def stereoCalibrate(objectPoints: _typing.Sequence[cv2.UMat], imagePoints1: _typing.Sequence[cv2.UMat], imagePoints2: _typing.Sequence[cv2.UMat], K1: cv2.UMat, D1: cv2.UMat, K2: cv2.UMat, D2: cv2.UMat, imageSize: cv2.typing.Size, R: cv2.UMat | None = ..., T: cv2.UMat | None = ..., rvecs: _typing.Sequence[cv2.UMat] | None = ..., tvecs: _typing.Sequence[cv2.UMat] | None = ..., flags: int = ..., criteria: cv2.typing.TermCriteria = ...) -> tuple[float, cv2.UMat, cv2.UMat, cv2.UMat, cv2.UMat, cv2.UMat, cv2.UMat, _typing.Sequence[cv2.UMat], _typing.Sequence[cv2.UMat]]: ...
@_typing.overload
def stereoCalibrate(objectPoints: _typing.Sequence[cv2.typing.MatLike], imagePoints1: _typing.Sequence[cv2.typing.MatLike], imagePoints2: _typing.Sequence[cv2.typing.MatLike], K1: cv2.typing.MatLike, D1: cv2.typing.MatLike, K2: cv2.typing.MatLike, D2: cv2.typing.MatLike, imageSize: cv2.typing.Size, R: cv2.typing.MatLike | None = ..., T: cv2.typing.MatLike | None = ..., flags: int = ..., criteria: cv2.typing.TermCriteria = ...) -> tuple[float, cv2.typing.MatLike, cv2.typing.MatLike, cv2.typing.MatLike, cv2.typing.MatLike, cv2.typing.MatLike, cv2.typing.MatLike]: ...
@_typing.overload
def stereoCalibrate(objectPoints: _typing.Sequence[cv2.UMat], imagePoints1: _typing.Sequence[cv2.UMat], imagePoints2: _typing.Sequence[cv2.UMat], K1: cv2.UMat, D1: cv2.UMat, K2: cv2.UMat, D2: cv2.UMat, imageSize: cv2.typing.Size, R: cv2.UMat | None = ..., T: cv2.UMat | None = ..., flags: int = ..., criteria: cv2.typing.TermCriteria = ...) -> tuple[float, cv2.UMat, cv2.UMat, cv2.UMat, cv2.UMat, cv2.UMat, cv2.UMat]: ...

@_typing.overload
def stereoRectify(K1: cv2.typing.MatLike, D1: cv2.typing.MatLike, K2: cv2.typing.MatLike, D2: cv2.typing.MatLike, imageSize: cv2.typing.Size, R: cv2.typing.MatLike, tvec: cv2.typing.MatLike, flags: int, R1: cv2.typing.MatLike | None = ..., R2: cv2.typing.MatLike | None = ..., P1: cv2.typing.MatLike | None = ..., P2: cv2.typing.MatLike | None = ..., Q: cv2.typing.MatLike | None = ..., newImageSize: cv2.typing.Size = ..., balance: float = ..., fov_scale: float = ...) -> tuple[cv2.typing.MatLike, cv2.typing.MatLike, cv2.typing.MatLike, cv2.typing.MatLike, cv2.typing.MatLike]: ...
@_typing.overload
def stereoRectify(K1: cv2.UMat, D1: cv2.UMat, K2: cv2.UMat, D2: cv2.UMat, imageSize: cv2.typing.Size, R: cv2.UMat, tvec: cv2.UMat, flags: int, R1: cv2.UMat | None = ..., R2: cv2.UMat | None = ..., P1: cv2.UMat | None = ..., P2: cv2.UMat | None = ..., Q: cv2.UMat | None = ..., newImageSize: cv2.typing.Size = ..., balance: float = ..., fov_scale: float = ...) -> tuple[cv2.UMat, cv2.UMat, cv2.UMat, cv2.UMat, cv2.UMat]: ...

@_typing.overload
def undistortImage(distorted: cv2.typing.MatLike, K: cv2.typing.MatLike, D: cv2.typing.MatLike, undistorted: cv2.typing.MatLike | None = ..., Knew: cv2.typing.MatLike | None = ..., new_size: cv2.typing.Size = ...) -> cv2.typing.MatLike: ...
@_typing.overload
def undistortImage(distorted: cv2.UMat, K: cv2.UMat, D: cv2.UMat, undistorted: cv2.UMat | None = ..., Knew: cv2.UMat | None = ..., new_size: cv2.typing.Size = ...) -> cv2.UMat: ...

@_typing.overload
def undistortPoints(distorted: cv2.typing.MatLike, K: cv2.typing.MatLike, D: cv2.typing.MatLike, undistorted: cv2.typing.MatLike | None = ..., R: cv2.typing.MatLike | None = ..., P: cv2.typing.MatLike | None = ..., criteria: cv2.typing.TermCriteria = ...) -> cv2.typing.MatLike: ...
@_typing.overload
def undistortPoints(distorted: cv2.UMat, K: cv2.UMat, D: cv2.UMat, undistorted: cv2.UMat | None = ..., R: cv2.UMat | None = ..., P: cv2.UMat | None = ..., criteria: cv2.typing.TermCriteria = ...) -> cv2.UMat: ...


