__version__ = '0.20.1'
git_version = '3ac97aa9120137381ed1060f37237e44485ac2aa'
from torchvision.extension import _check_cuda_version
if _check_cuda_version() > 0:
    cuda = _check_cuda_version()
