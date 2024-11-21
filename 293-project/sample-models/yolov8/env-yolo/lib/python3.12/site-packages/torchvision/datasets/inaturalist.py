import os
import os.path
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from PIL import Image

from .utils import download_and_extract_archive, verify_str_arg
from .vision import VisionDataset

CATEGORIES_2021 = ["kingdom", "phylum", "class", "order", "family", "genus"]

DATASET_URLS = {
    "2017": "https://ml-inat-competition-datasets.s3.amazonaws.com/2017/train_val_images.tar.gz",
    "2018": "https://ml-inat-competition-datasets.s3.amazonaws.com/2018/train_val2018.tar.gz",
    "2019": "https://ml-inat-competition-datasets.s3.amazonaws.com/2019/train_val2019.tar.gz",
    "2021_train": "https://ml-inat-competition-datasets.s3.amazonaws.com/2021/train.tar.gz",
    "2021_train_mini": "https://ml-inat-competition-datasets.s3.amazonaws.com/2021/train_mini.tar.gz",
    "2021_valid": "https://ml-inat-competition-datasets.s3.amazonaws.com/2021/val.tar.gz",
}

DATASET_MD5 = {
    "2017": "7c784ea5e424efaec655bd392f87301f",
    "2018": "b1c6952ce38f31868cc50ea72d066cc3",
    "2019": "c60a6e2962c9b8ccbd458d12c8582644",
    "2021_train": "e0526d53c7f7b2e3167b2b43bb2690ed",
    "2021_train_mini": "db6ed8330e634445efc8fec83ae81442",
    "2021_valid": "f6f6e0e242e3d4c9569ba56400938afc",
}


class INaturalist(VisionDataset):
    """`iNaturalist <https://github.com/visipedia/inat_comp>`_ Dataset.

    Args:
        root (str or ``pathlib.Path``): Root directory of dataset where the image files are stored.
            This class does not require/use annotation files.
        version (string, optional): Which version of the dataset to download/use. One of
            '2017', '2018', '2019', '2021_train', '2021_train_mini', '2021_valid'.
            Default: `2021_train`.
        target_type (string or list, optional): Type of target to use, for 2021 versions, one of:

            - ``full``: the full category (species)
            - ``kingdom``: e.g. "Animalia"
            - ``phylum``: e.g. "Arthropoda"
            - ``class``: e.g. "Insecta"
            - ``order``: e.g. "Coleoptera"
            - ``family``: e.g. "Cleridae"
            - ``genus``: e.g. "Trichodes"

            for 2017-2019 versions, one of:

            - ``full``: the full (numeric) category
            - ``super``: the super category, e.g. "Amphibians"

            Can also be a list to output a tuple with all specified target types.
            Defaults to ``full``.
        transform (callable, optional): A function/transform that takes in a PIL image
            and returns a transformed version. E.g, ``transforms.RandomCrop``
        target_transform (callable, optional): A function/transform that takes in the
            target and transforms it.
        download (bool, optional): If true, downloads the dataset from the internet and
            puts it in root directory. If dataset is already downloaded, it is not
            downloaded again.
    """

    def __init__(
        self,
        root: Union[str, Path],
        version: str = "2021_train",
        target_type: Union[List[str], str] = "full",
        transform: Optional[Callable] = None,
        target_transform: Optional[Callable] = None,
        download: bool = False,
    ) -> None:
        self.version = verify_str_arg(version, "version", DATASET_URLS.keys())

        super().__init__(os.path.join(root, version), transform=transform, target_transform=target_transform)

        os.makedirs(root, exist_ok=True)
        if download:
            self.download()

        if not self._check_integrity():
            raise RuntimeError("Dataset not found or corrupted. You can use download=True to download it")

        self.all_categories: List[str] = []

        # map: category type -> name of category -> index
        self.categories_index: Dict[str, Dict[str, int]] = {}

        # list indexed by category id, containing mapping from category type -> index
        self.categories_map: List[Dict[str, int]] = []

        if not isinstance(target_type, list):
            target_type = [target_type]
        if self.version[:4] == "2021":
            self.target_type = [verify_str_arg(t, "target_type", ("full", *CATEGORIES_2021)) for t in target_type]
            self._init_2021()
        else:
            self.target_type = [verify_str_arg(t, "target_type", ("full", "super")) for t in target_type]
            self._init_pre2021()

        # index of all files: (full category id, filename)
        self.index: List[Tuple[int, str]] = []

        for dir_index, dir_name in enumerate(self.all_categories):
            files = os.listdir(os.path.join(self.root, dir_name))
            for fname in files:
                self.index.append((dir_index, fname))

    def _init_2021(self) -> None:
        """Initialize based on 2021 layout"""

        self.all_categories = sorted(os.listdir(self.root))

        # map: category type -> name of category -> index
        self.categories_index = {k: {} for k in CATEGORIES_2021}

        for dir_index, dir_name in enumerate(self.all_categories):
            pieces = dir_name.split("_")
            if len(pieces) != 8:
                raise RuntimeError(f"Unexpected category name {dir_name}, wrong number of pieces")
            if pieces[0] != f"{dir_index:05d}":
                raise RuntimeError(f"Unexpected category id {pieces[0]}, expecting {dir_index:05d}")
            cat_map = {}
            for cat, name in zip(CATEGORIES_2021, pieces[1:7]):
                if name in self.categories_index[cat]:
                    cat_id = self.categories_index[cat][name]
                else:
                    cat_id = len(self.categories_index[cat])
                    self.categories_index[cat][name] = cat_id
                cat_map[cat] = cat_id
            self.categories_map.append(cat_map)

    def _init_pre2021(self) -> None:
        """Initialize based on 2017-2019 layout"""

        # map: category type -> name of category -> index
        self.categories_index = {"super": {}}

        cat_index = 0
        super_categories = sorted(os.listdir(self.root))
        for sindex, scat in enumerate(super_categories):
            self.categories_index["super"][scat] = sindex
            subcategories = sorted(os.listdir(os.path.join(self.root, scat)))
            for subcat in subcategories:
                if self.version == "2017":
                    # this version does not use ids as directory names
                    subcat_i = cat_index
                    cat_index += 1
                else:
                    try:
                        subcat_i = int(subcat)
                    except ValueError:
                        raise RuntimeError(f"Unexpected non-numeric dir name: {subcat}")
                if subcat_i >= len(self.categories_map):
                    old_len = len(self.categories_map)
                    self.categories_map.extend([{}] * (subcat_i - old_len + 1))
                    self.all_categories.extend([""] * (subcat_i - old_len + 1))
                if self.categories_map[subcat_i]:
                    raise RuntimeError(f"Duplicate category {subcat}")
                self.categories_map[subcat_i] = {"super": sindex}
                self.all_categories[subcat_i] = os.path.join(scat, subcat)

        # validate the dictionary
        for cindex, c in enumerate(self.categories_map):
            if not c:
                raise RuntimeError(f"Missing category {cindex}")

    def __getitem__(self, index: int) -> Tuple[Any, Any]:
        """
        Args:
            index (int): Index

        Returns:
            tuple: (image, target) where the type of target specified by target_type.
        """

        cat_id, fname = self.index[index]
        img = Image.open(os.path.join(self.root, self.all_categories[cat_id], fname))

        target: Any = []
        for t in self.target_type:
            if t == "full":
                target.append(cat_id)
            else:
                target.append(self.categories_map[cat_id][t])
        target = tuple(target) if len(target) > 1 else target[0]

        if self.transform is not None:
            img = self.transform(img)

        if self.target_transform is not None:
            target = self.target_transform(target)

        return img, target

    def __len__(self) -> int:
        return len(self.index)

    def category_name(self, category_type: str, category_id: int) -> str:
        """
        Args:
            category_type(str): one of "full", "kingdom", "phylum", "class", "order", "family", "genus" or "super"
            category_id(int): an index (class id) from this category

        Returns:
            the name of the category
        """
        if category_type == "full":
            return self.all_categories[category_id]
        else:
            if category_type not in self.categories_index:
                raise ValueError(f"Invalid category type '{category_type}'")
            else:
                for name, id in self.categories_index[category_type].items():
                    if id == category_id:
                        return name
                raise ValueError(f"Invalid category id {category_id} for {category_type}")

    def _check_integrity(self) -> bool:
        return os.path.exists(self.root) and len(os.listdir(self.root)) > 0

    def download(self) -> None:
        if self._check_integrity():
            raise RuntimeError(
                f"The directory {self.root} already exists. "
                f"If you want to re-download or re-extract the images, delete the directory."
            )

        base_root = os.path.dirname(self.root)

        download_and_extract_archive(
            DATASET_URLS[self.version], base_root, filename=f"{self.version}.tgz", md5=DATASET_MD5[self.version]
        )

        orig_dir_name = os.path.join(base_root, os.path.basename(DATASET_URLS[self.version]).rstrip(".tar.gz"))
        if not os.path.exists(orig_dir_name):
            raise RuntimeError(f"Unable to find downloaded files at {orig_dir_name}")
        os.rename(orig_dir_name, self.root)
        print(f"Dataset version '{self.version}' has been downloaded and prepared for use")
