import pathlib
from typing import Any, Callable, Optional, Tuple, Union

from PIL import Image

from .utils import _decompress, download_file_from_google_drive, verify_str_arg
from .vision import VisionDataset


class PCAM(VisionDataset):
    """`PCAM Dataset   <https://github.com/basveeling/pcam>`_.

    The PatchCamelyon dataset is a binary classification dataset with 327,680
    color images (96px x 96px), extracted from histopathologic scans of lymph node
    sections. Each image is annotated with a binary label indicating presence of
    metastatic tissue.

    This dataset requires the ``h5py`` package which you can install with ``pip install h5py``.

    Args:
         root (str or ``pathlib.Path``): Root directory of the dataset.
         split (string, optional): The dataset split, supports ``"train"`` (default), ``"test"`` or ``"val"``.
         transform (callable, optional): A function/transform that takes in a PIL image and returns a transformed
             version. E.g, ``transforms.RandomCrop``.
         target_transform (callable, optional): A function/transform that takes in the target and transforms it.
         download (bool, optional): If True, downloads the dataset from the internet and puts it into ``root/pcam``. If
             dataset is already downloaded, it is not downloaded again.

             .. warning::

                To download the dataset `gdown <https://github.com/wkentaro/gdown>`_ is required.
    """

    _FILES = {
        "train": {
            "images": (
                "camelyonpatch_level_2_split_train_x.h5",  # Data file name
                "1Ka0XfEMiwgCYPdTI-vv6eUElOBnKFKQ2",  # Google Drive ID
                "1571f514728f59376b705fc836ff4b63",  # md5 hash
            ),
            "targets": (
                "camelyonpatch_level_2_split_train_y.h5",
                "1269yhu3pZDP8UYFQs-NYs3FPwuK-nGSG",
                "35c2d7259d906cfc8143347bb8e05be7",
            ),
        },
        "test": {
            "images": (
                "camelyonpatch_level_2_split_test_x.h5",
                "1qV65ZqZvWzuIVthK8eVDhIwrbnsJdbg_",
                "d8c2d60d490dbd479f8199bdfa0cf6ec",
            ),
            "targets": (
                "camelyonpatch_level_2_split_test_y.h5",
                "17BHrSrwWKjYsOgTMmoqrIjDy6Fa2o_gP",
                "60a7035772fbdb7f34eb86d4420cf66a",
            ),
        },
        "val": {
            "images": (
                "camelyonpatch_level_2_split_valid_x.h5",
                "1hgshYGWK8V-eGRy8LToWJJgDU_rXWVJ3",
                "d5b63470df7cfa627aeec8b9dc0c066e",
            ),
            "targets": (
                "camelyonpatch_level_2_split_valid_y.h5",
                "1bH8ZRbhSVAhScTS0p9-ZzGnX91cHT3uO",
                "2b85f58b927af9964a4c15b8f7e8f179",
            ),
        },
    }

    def __init__(
        self,
        root: Union[str, pathlib.Path],
        split: str = "train",
        transform: Optional[Callable] = None,
        target_transform: Optional[Callable] = None,
        download: bool = False,
    ):
        try:
            import h5py

            self.h5py = h5py
        except ImportError:
            raise RuntimeError(
                "h5py is not found. This dataset needs to have h5py installed: please run pip install h5py"
            )

        self._split = verify_str_arg(split, "split", ("train", "test", "val"))

        super().__init__(root, transform=transform, target_transform=target_transform)
        self._base_folder = pathlib.Path(self.root) / "pcam"

        if download:
            self._download()

        if not self._check_exists():
            raise RuntimeError("Dataset not found. You can use download=True to download it")

    def __len__(self) -> int:
        images_file = self._FILES[self._split]["images"][0]
        with self.h5py.File(self._base_folder / images_file) as images_data:
            return images_data["x"].shape[0]

    def __getitem__(self, idx: int) -> Tuple[Any, Any]:
        images_file = self._FILES[self._split]["images"][0]
        with self.h5py.File(self._base_folder / images_file) as images_data:
            image = Image.fromarray(images_data["x"][idx]).convert("RGB")

        targets_file = self._FILES[self._split]["targets"][0]
        with self.h5py.File(self._base_folder / targets_file) as targets_data:
            target = int(targets_data["y"][idx, 0, 0, 0])  # shape is [num_images, 1, 1, 1]

        if self.transform:
            image = self.transform(image)
        if self.target_transform:
            target = self.target_transform(target)

        return image, target

    def _check_exists(self) -> bool:
        images_file = self._FILES[self._split]["images"][0]
        targets_file = self._FILES[self._split]["targets"][0]
        return all(self._base_folder.joinpath(h5_file).exists() for h5_file in (images_file, targets_file))

    def _download(self) -> None:
        if self._check_exists():
            return

        for file_name, file_id, md5 in self._FILES[self._split].values():
            archive_name = file_name + ".gz"
            download_file_from_google_drive(file_id, str(self._base_folder), filename=archive_name, md5=md5)
            _decompress(str(self._base_folder / archive_name))
