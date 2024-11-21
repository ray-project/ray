from pathlib import Path
from typing import Callable, Optional, Union

from .folder import ImageFolder
from .utils import download_and_extract_archive, verify_str_arg


class Country211(ImageFolder):
    """`The Country211 Data Set <https://github.com/openai/CLIP/blob/main/data/country211.md>`_ from OpenAI.

    This dataset was built by filtering the images from the YFCC100m dataset
    that have GPS coordinate corresponding to a ISO-3166 country code. The
    dataset is balanced by sampling 150 train images, 50 validation images, and
    100 test images for each country.

    Args:
        root (str or ``pathlib.Path``): Root directory of the dataset.
        split (string, optional): The dataset split, supports ``"train"`` (default), ``"valid"`` and ``"test"``.
        transform (callable, optional): A function/transform that takes in a PIL image and returns a transformed
            version. E.g, ``transforms.RandomCrop``.
        target_transform (callable, optional): A function/transform that takes in the target and transforms it.
        download (bool, optional): If True, downloads the dataset from the internet and puts it into
            ``root/country211/``. If dataset is already downloaded, it is not downloaded again.
    """

    _URL = "https://openaipublic.azureedge.net/clip/data/country211.tgz"
    _MD5 = "84988d7644798601126c29e9877aab6a"

    def __init__(
        self,
        root: Union[str, Path],
        split: str = "train",
        transform: Optional[Callable] = None,
        target_transform: Optional[Callable] = None,
        download: bool = False,
    ) -> None:
        self._split = verify_str_arg(split, "split", ("train", "valid", "test"))

        root = Path(root).expanduser()
        self.root = str(root)
        self._base_folder = root / "country211"

        if download:
            self._download()

        if not self._check_exists():
            raise RuntimeError("Dataset not found. You can use download=True to download it")

        super().__init__(str(self._base_folder / self._split), transform=transform, target_transform=target_transform)
        self.root = str(root)

    def _check_exists(self) -> bool:
        return self._base_folder.exists() and self._base_folder.is_dir()

    def _download(self) -> None:
        if self._check_exists():
            return
        download_and_extract_archive(self._URL, download_root=self.root, md5=self._MD5)
