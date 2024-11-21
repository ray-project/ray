import os
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from PIL import Image

from .utils import check_integrity, download_and_extract_archive, download_url, verify_str_arg
from .vision import VisionDataset


class _LFW(VisionDataset):

    base_folder = "lfw-py"
    download_url_prefix = "http://vis-www.cs.umass.edu/lfw/"

    file_dict = {
        "original": ("lfw", "lfw.tgz", "a17d05bd522c52d84eca14327a23d494"),
        "funneled": ("lfw_funneled", "lfw-funneled.tgz", "1b42dfed7d15c9b2dd63d5e5840c86ad"),
        "deepfunneled": ("lfw-deepfunneled", "lfw-deepfunneled.tgz", "68331da3eb755a505a502b5aacb3c201"),
    }
    checksums = {
        "pairs.txt": "9f1ba174e4e1c508ff7cdf10ac338a7d",
        "pairsDevTest.txt": "5132f7440eb68cf58910c8a45a2ac10b",
        "pairsDevTrain.txt": "4f27cbf15b2da4a85c1907eb4181ad21",
        "people.txt": "450f0863dd89e85e73936a6d71a3474b",
        "peopleDevTest.txt": "e4bf5be0a43b5dcd9dc5ccfcb8fb19c5",
        "peopleDevTrain.txt": "54eaac34beb6d042ed3a7d883e247a21",
        "lfw-names.txt": "a6d0a479bd074669f656265a6e693f6d",
    }
    annot_file = {"10fold": "", "train": "DevTrain", "test": "DevTest"}
    names = "lfw-names.txt"

    def __init__(
        self,
        root: Union[str, Path],
        split: str,
        image_set: str,
        view: str,
        transform: Optional[Callable] = None,
        target_transform: Optional[Callable] = None,
        download: bool = False,
    ) -> None:
        super().__init__(os.path.join(root, self.base_folder), transform=transform, target_transform=target_transform)

        self.image_set = verify_str_arg(image_set.lower(), "image_set", self.file_dict.keys())
        images_dir, self.filename, self.md5 = self.file_dict[self.image_set]

        self.view = verify_str_arg(view.lower(), "view", ["people", "pairs"])
        self.split = verify_str_arg(split.lower(), "split", ["10fold", "train", "test"])
        self.labels_file = f"{self.view}{self.annot_file[self.split]}.txt"
        self.data: List[Any] = []

        if download:
            self.download()

        if not self._check_integrity():
            raise RuntimeError("Dataset not found or corrupted. You can use download=True to download it")

        self.images_dir = os.path.join(self.root, images_dir)

    def _loader(self, path: str) -> Image.Image:
        with open(path, "rb") as f:
            img = Image.open(f)
            return img.convert("RGB")

    def _check_integrity(self) -> bool:
        st1 = check_integrity(os.path.join(self.root, self.filename), self.md5)
        st2 = check_integrity(os.path.join(self.root, self.labels_file), self.checksums[self.labels_file])
        if not st1 or not st2:
            return False
        if self.view == "people":
            return check_integrity(os.path.join(self.root, self.names), self.checksums[self.names])
        return True

    def download(self) -> None:
        if self._check_integrity():
            print("Files already downloaded and verified")
            return
        url = f"{self.download_url_prefix}{self.filename}"
        download_and_extract_archive(url, self.root, filename=self.filename, md5=self.md5)
        download_url(f"{self.download_url_prefix}{self.labels_file}", self.root)
        if self.view == "people":
            download_url(f"{self.download_url_prefix}{self.names}", self.root)

    def _get_path(self, identity: str, no: Union[int, str]) -> str:
        return os.path.join(self.images_dir, identity, f"{identity}_{int(no):04d}.jpg")

    def extra_repr(self) -> str:
        return f"Alignment: {self.image_set}\nSplit: {self.split}"

    def __len__(self) -> int:
        return len(self.data)


class LFWPeople(_LFW):
    """`LFW <http://vis-www.cs.umass.edu/lfw/>`_ Dataset.

    Args:
        root (str or ``pathlib.Path``): Root directory of dataset where directory
            ``lfw-py`` exists or will be saved to if download is set to True.
        split (string, optional): The image split to use. Can be one of ``train``, ``test``,
            ``10fold`` (default).
        image_set (str, optional): Type of image funneling to use, ``original``, ``funneled`` or
            ``deepfunneled``. Defaults to ``funneled``.
        transform (callable, optional): A function/transform that  takes in a PIL image
            and returns a transformed version. E.g, ``transforms.RandomRotation``
        target_transform (callable, optional): A function/transform that takes in the
            target and transforms it.
        download (bool, optional): If true, downloads the dataset from the internet and
            puts it in root directory. If dataset is already downloaded, it is not
            downloaded again.

    """

    def __init__(
        self,
        root: str,
        split: str = "10fold",
        image_set: str = "funneled",
        transform: Optional[Callable] = None,
        target_transform: Optional[Callable] = None,
        download: bool = False,
    ) -> None:
        super().__init__(root, split, image_set, "people", transform, target_transform, download)

        self.class_to_idx = self._get_classes()
        self.data, self.targets = self._get_people()

    def _get_people(self) -> Tuple[List[str], List[int]]:
        data, targets = [], []
        with open(os.path.join(self.root, self.labels_file)) as f:
            lines = f.readlines()
            n_folds, s = (int(lines[0]), 1) if self.split == "10fold" else (1, 0)

            for fold in range(n_folds):
                n_lines = int(lines[s])
                people = [line.strip().split("\t") for line in lines[s + 1 : s + n_lines + 1]]
                s += n_lines + 1
                for i, (identity, num_imgs) in enumerate(people):
                    for num in range(1, int(num_imgs) + 1):
                        img = self._get_path(identity, num)
                        data.append(img)
                        targets.append(self.class_to_idx[identity])

        return data, targets

    def _get_classes(self) -> Dict[str, int]:
        with open(os.path.join(self.root, self.names)) as f:
            lines = f.readlines()
            names = [line.strip().split()[0] for line in lines]
        class_to_idx = {name: i for i, name in enumerate(names)}
        return class_to_idx

    def __getitem__(self, index: int) -> Tuple[Any, Any]:
        """
        Args:
            index (int): Index

        Returns:
            tuple: Tuple (image, target) where target is the identity of the person.
        """
        img = self._loader(self.data[index])
        target = self.targets[index]

        if self.transform is not None:
            img = self.transform(img)

        if self.target_transform is not None:
            target = self.target_transform(target)

        return img, target

    def extra_repr(self) -> str:
        return super().extra_repr() + f"\nClasses (identities): {len(self.class_to_idx)}"


class LFWPairs(_LFW):
    """`LFW <http://vis-www.cs.umass.edu/lfw/>`_ Dataset.

    Args:
        root (str or ``pathlib.Path``): Root directory of dataset where directory
            ``lfw-py`` exists or will be saved to if download is set to True.
        split (string, optional): The image split to use. Can be one of ``train``, ``test``,
            ``10fold``. Defaults to ``10fold``.
        image_set (str, optional): Type of image funneling to use, ``original``, ``funneled`` or
            ``deepfunneled``. Defaults to ``funneled``.
        transform (callable, optional): A function/transform that takes in a PIL image
            and returns a transformed version. E.g, ``transforms.RandomRotation``
        target_transform (callable, optional): A function/transform that takes in the
            target and transforms it.
        download (bool, optional): If true, downloads the dataset from the internet and
            puts it in root directory. If dataset is already downloaded, it is not
            downloaded again.

    """

    def __init__(
        self,
        root: str,
        split: str = "10fold",
        image_set: str = "funneled",
        transform: Optional[Callable] = None,
        target_transform: Optional[Callable] = None,
        download: bool = False,
    ) -> None:
        super().__init__(root, split, image_set, "pairs", transform, target_transform, download)

        self.pair_names, self.data, self.targets = self._get_pairs(self.images_dir)

    def _get_pairs(self, images_dir: str) -> Tuple[List[Tuple[str, str]], List[Tuple[str, str]], List[int]]:
        pair_names, data, targets = [], [], []
        with open(os.path.join(self.root, self.labels_file)) as f:
            lines = f.readlines()
            if self.split == "10fold":
                n_folds, n_pairs = lines[0].split("\t")
                n_folds, n_pairs = int(n_folds), int(n_pairs)
            else:
                n_folds, n_pairs = 1, int(lines[0])
            s = 1

            for fold in range(n_folds):
                matched_pairs = [line.strip().split("\t") for line in lines[s : s + n_pairs]]
                unmatched_pairs = [line.strip().split("\t") for line in lines[s + n_pairs : s + (2 * n_pairs)]]
                s += 2 * n_pairs
                for pair in matched_pairs:
                    img1, img2, same = self._get_path(pair[0], pair[1]), self._get_path(pair[0], pair[2]), 1
                    pair_names.append((pair[0], pair[0]))
                    data.append((img1, img2))
                    targets.append(same)
                for pair in unmatched_pairs:
                    img1, img2, same = self._get_path(pair[0], pair[1]), self._get_path(pair[2], pair[3]), 0
                    pair_names.append((pair[0], pair[2]))
                    data.append((img1, img2))
                    targets.append(same)

        return pair_names, data, targets

    def __getitem__(self, index: int) -> Tuple[Any, Any, int]:
        """
        Args:
            index (int): Index

        Returns:
            tuple: (image1, image2, target) where target is `0` for different indentities and `1` for same identities.
        """
        img1, img2 = self.data[index]
        img1, img2 = self._loader(img1), self._loader(img2)
        target = self.targets[index]

        if self.transform is not None:
            img1, img2 = self.transform(img1), self.transform(img2)

        if self.target_transform is not None:
            target = self.target_transform(target)

        return img1, img2, target
