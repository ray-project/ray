import json
import pathlib
from typing import Any, Callable, List, Optional, Tuple, Union
from urllib.parse import urlparse

from PIL import Image

from .utils import download_and_extract_archive, verify_str_arg
from .vision import VisionDataset


class CLEVRClassification(VisionDataset):
    """`CLEVR <https://cs.stanford.edu/people/jcjohns/clevr/>`_  classification dataset.

    The number of objects in a scene are used as label.

    Args:
        root (str or ``pathlib.Path``): Root directory of dataset where directory ``root/clevr`` exists or will be saved to if download is
            set to True.
        split (string, optional): The dataset split, supports ``"train"`` (default), ``"val"``, or ``"test"``.
        transform (callable, optional): A function/transform that takes in a PIL image and returns a transformed
            version. E.g, ``transforms.RandomCrop``
        target_transform (callable, optional): A function/transform that takes in them target and transforms it.
        download (bool, optional): If true, downloads the dataset from the internet and puts it in root directory. If
            dataset is already downloaded, it is not downloaded again.
    """

    _URL = "https://dl.fbaipublicfiles.com/clevr/CLEVR_v1.0.zip"
    _MD5 = "b11922020e72d0cd9154779b2d3d07d2"

    def __init__(
        self,
        root: Union[str, pathlib.Path],
        split: str = "train",
        transform: Optional[Callable] = None,
        target_transform: Optional[Callable] = None,
        download: bool = False,
    ) -> None:
        self._split = verify_str_arg(split, "split", ("train", "val", "test"))
        super().__init__(root, transform=transform, target_transform=target_transform)
        self._base_folder = pathlib.Path(self.root) / "clevr"
        self._data_folder = self._base_folder / pathlib.Path(urlparse(self._URL).path).stem

        if download:
            self._download()

        if not self._check_exists():
            raise RuntimeError("Dataset not found or corrupted. You can use download=True to download it")

        self._image_files = sorted(self._data_folder.joinpath("images", self._split).glob("*"))

        self._labels: List[Optional[int]]
        if self._split != "test":
            with open(self._data_folder / "scenes" / f"CLEVR_{self._split}_scenes.json") as file:
                content = json.load(file)
            num_objects = {scene["image_filename"]: len(scene["objects"]) for scene in content["scenes"]}
            self._labels = [num_objects[image_file.name] for image_file in self._image_files]
        else:
            self._labels = [None] * len(self._image_files)

    def __len__(self) -> int:
        return len(self._image_files)

    def __getitem__(self, idx: int) -> Tuple[Any, Any]:
        image_file = self._image_files[idx]
        label = self._labels[idx]

        image = Image.open(image_file).convert("RGB")

        if self.transform:
            image = self.transform(image)

        if self.target_transform:
            label = self.target_transform(label)

        return image, label

    def _check_exists(self) -> bool:
        return self._data_folder.exists() and self._data_folder.is_dir()

    def _download(self) -> None:
        if self._check_exists():
            return

        download_and_extract_archive(self._URL, str(self._base_folder), md5=self._MD5)

    def extra_repr(self) -> str:
        return f"split={self._split}"
