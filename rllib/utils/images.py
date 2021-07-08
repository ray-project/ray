import numpy as np

try:
    import cv2
    cv2.ocl.setUseOpenCL(False)
except ImportError:
    cv2 = None

if cv2 is None:
    try:
        from skimage import color, io, transform
    except ImportError:
        raise ModuleNotFoundError("Either scikit-image or opencv is required")


def resize(img, height, width):
    if cv2:
        return cv2.resize(img, (width, height), interpolation=cv2.INTER_AREA)
    return transform.resize(img, (height, width))


def rgb2gray(img):
    if cv2:
        return cv2.cvtColor(img, cv2.COLOR_RGB2GRAY)
    return color.rgb2gray(img)


def imread(img_file):
    if cv2:
        return cv2.imread(img_file).astype(np.float32)
    return io.imread(img_file).astype(np.float32)
