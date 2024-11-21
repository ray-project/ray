import os
from os import path
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from urllib.parse import urljoin

from .folder import default_loader
from .utils import check_integrity, download_and_extract_archive, verify_str_arg
from .vision import VisionDataset


class Places365(VisionDataset):
    r"""`Places365 <http://places2.csail.mit.edu/index.html>`_ classification dataset.

    Args:
        root (str or ``pathlib.Path``): Root directory of the Places365 dataset.
        split (string, optional): The dataset split. Can be one of ``train-standard`` (default), ``train-challenge``,
            ``val``.
        small (bool, optional): If ``True``, uses the small images, i.e. resized to 256 x 256 pixels, instead of the
            high resolution ones.
        download (bool, optional): If ``True``, downloads the dataset components and places them in ``root``. Already
            downloaded archives are not downloaded again.
        transform (callable, optional): A function/transform that takes in a PIL image
            and returns a transformed version. E.g, ``transforms.RandomCrop``
        target_transform (callable, optional): A function/transform that takes in the
            target and transforms it.
        loader (callable, optional): A function to load an image given its path.

     Attributes:
        classes (list): List of the class names.
        class_to_idx (dict): Dict with items (class_name, class_index).
        imgs (list): List of (image path, class_index) tuples
        targets (list): The class_index value for each image in the dataset

    Raises:
        RuntimeError: If ``download is False`` and the meta files, i.e. the devkit, are not present or corrupted.
        RuntimeError: If ``download is True`` and the image archive is already extracted.
    """
    _SPLITS = ("train-standard", "train-challenge", "val")
    _BASE_URL = "http://data.csail.mit.edu/places/places365/"
    # {variant: (archive, md5)}
    _DEVKIT_META = {
        "standard": ("filelist_places365-standard.tar", "35a0585fee1fa656440f3ab298f8479c"),
        "challenge": ("filelist_places365-challenge.tar", "70a8307e459c3de41690a7c76c931734"),
    }
    # (file, md5)
    _CATEGORIES_META = ("categories_places365.txt", "06c963b85866bd0649f97cb43dd16673")
    # {split: (file, md5)}
    _FILE_LIST_META = {
        "train-standard": ("places365_train_standard.txt", "30f37515461640559006b8329efbed1a"),
        "train-challenge": ("places365_train_challenge.txt", "b2931dc997b8c33c27e7329c073a6b57"),
        "val": ("places365_val.txt", "e9f2fd57bfd9d07630173f4e8708e4b1"),
    }
    # {(split, small): (file, md5)}
    _IMAGES_META = {
        ("train-standard", False): ("train_large_places365standard.tar", "67e186b496a84c929568076ed01a8aa1"),
        ("train-challenge", False): ("train_large_places365challenge.tar", "605f18e68e510c82b958664ea134545f"),
        ("val", False): ("val_large.tar", "9b71c4993ad89d2d8bcbdc4aef38042f"),
        ("train-standard", True): ("train_256_places365standard.tar", "53ca1c756c3d1e7809517cc47c5561c5"),
        ("train-challenge", True): ("train_256_places365challenge.tar", "741915038a5e3471ec7332404dfb64ef"),
        ("val", True): ("val_256.tar", "e27b17d8d44f4af9a78502beb927f808"),
    }

    def __init__(
        self,
        root: Union[str, Path],
        split: str = "train-standard",
        small: bool = False,
        download: bool = False,
        transform: Optional[Callable] = None,
        target_transform: Optional[Callable] = None,
        loader: Callable[[str], Any] = default_loader,
    ) -> None:
        super().__init__(root, transform=transform, target_transform=target_transform)

        self.split = self._verify_split(split)
        self.small = small
        self.loader = loader

        self.classes, self.class_to_idx = self.load_categories(download)
        self.imgs, self.targets = self.load_file_list(download)

        if download:
            self.download_images()

    def __getitem__(self, index: int) -> Tuple[Any, Any]:
        file, target = self.imgs[index]
        image = self.loader(file)

        if self.transforms is not None:
            image, target = self.transforms(image, target)

        return image, target

    def __len__(self) -> int:
        return len(self.imgs)

    @property
    def variant(self) -> str:
        return "challenge" if "challenge" in self.split else "standard"

    @property
    def images_dir(self) -> str:
        size = "256" if self.small else "large"
        if self.split.startswith("train"):
            dir = f"data_{size}_{self.variant}"
        else:
            dir = f"{self.split}_{size}"
        return path.join(self.root, dir)

    def load_categories(self, download: bool = True) -> Tuple[List[str], Dict[str, int]]:
        def process(line: str) -> Tuple[str, int]:
            cls, idx = line.split()
            return cls, int(idx)

        file, md5 = self._CATEGORIES_META
        file = path.join(self.root, file)
        if not self._check_integrity(file, md5, download):
            self.download_devkit()

        with open(file) as fh:
            class_to_idx = dict(process(line) for line in fh)

        return sorted(class_to_idx.keys()), class_to_idx

    def load_file_list(self, download: bool = True) -> Tuple[List[Tuple[str, int]], List[int]]:
        def process(line: str, sep="/") -> Tuple[str, int]:
            image, idx = line.split()
            return path.join(self.images_dir, image.lstrip(sep).replace(sep, os.sep)), int(idx)

        file, md5 = self._FILE_LIST_META[self.split]
        file = path.join(self.root, file)
        if not self._check_integrity(file, md5, download):
            self.download_devkit()

        with open(file) as fh:
            images = [process(line) for line in fh]

        _, targets = zip(*images)
        return images, list(targets)

    def download_devkit(self) -> None:
        file, md5 = self._DEVKIT_META[self.variant]
        download_and_extract_archive(urljoin(self._BASE_URL, file), self.root, md5=md5)

    def download_images(self) -> None:
        if path.exists(self.images_dir):
            raise RuntimeError(
                f"The directory {self.images_dir} already exists. If you want to re-download or re-extract the images, "
                f"delete the directory."
            )

        file, md5 = self._IMAGES_META[(self.split, self.small)]
        download_and_extract_archive(urljoin(self._BASE_URL, file), self.root, md5=md5)

        if self.split.startswith("train"):
            os.rename(self.images_dir.rsplit("_", 1)[0], self.images_dir)

    def extra_repr(self) -> str:
        return "\n".join(("Split: {split}", "Small: {small}")).format(**self.__dict__)

    def _verify_split(self, split: str) -> str:
        return verify_str_arg(split, "split", self._SPLITS)

    def _check_integrity(self, file: str, md5: str, download: bool) -> bool:
        integrity = check_integrity(file, md5=md5)
        if not integrity and not download:
            raise RuntimeError(
                f"The file {file} does not exist or is corrupted. You can set download=True to download it."
            )
        return integrity
