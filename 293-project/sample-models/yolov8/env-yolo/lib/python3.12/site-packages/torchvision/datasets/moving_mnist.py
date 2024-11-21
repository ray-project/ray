import os.path
from pathlib import Path
from typing import Callable, Optional, Union

import numpy as np
import torch
from torchvision.datasets.utils import download_url, verify_str_arg
from torchvision.datasets.vision import VisionDataset


class MovingMNIST(VisionDataset):
    """`MovingMNIST <http://www.cs.toronto.edu/~nitish/unsupervised_video/>`_ Dataset.

    Args:
        root (str or ``pathlib.Path``): Root directory of dataset where ``MovingMNIST/mnist_test_seq.npy`` exists.
        split (string, optional): The dataset split, supports ``None`` (default), ``"train"`` and ``"test"``.
            If ``split=None``, the full data is returned.
        split_ratio (int, optional): The split ratio of number of frames. If ``split="train"``, the first split
            frames ``data[:, :split_ratio]`` is returned. If ``split="test"``, the last split frames ``data[:, split_ratio:]``
            is returned. If ``split=None``, this parameter is ignored and the all frames data is returned.
        transform (callable, optional): A function/transform that takes in a torch Tensor
            and returns a transformed version. E.g, ``transforms.RandomCrop``
        download (bool, optional): If true, downloads the dataset from the internet and
            puts it in root directory. If dataset is already downloaded, it is not
            downloaded again.
    """

    _URL = "http://www.cs.toronto.edu/~nitish/unsupervised_video/mnist_test_seq.npy"

    def __init__(
        self,
        root: Union[str, Path],
        split: Optional[str] = None,
        split_ratio: int = 10,
        download: bool = False,
        transform: Optional[Callable] = None,
    ) -> None:
        super().__init__(root, transform=transform)

        self._base_folder = os.path.join(self.root, self.__class__.__name__)
        self._filename = self._URL.split("/")[-1]

        if split is not None:
            verify_str_arg(split, "split", ("train", "test"))
        self.split = split

        if not isinstance(split_ratio, int):
            raise TypeError(f"`split_ratio` should be an integer, but got {type(split_ratio)}")
        elif not (1 <= split_ratio <= 19):
            raise ValueError(f"`split_ratio` should be `1 <= split_ratio <= 19`, but got {split_ratio} instead.")
        self.split_ratio = split_ratio

        if download:
            self.download()

        if not self._check_exists():
            raise RuntimeError("Dataset not found. You can use download=True to download it.")

        data = torch.from_numpy(np.load(os.path.join(self._base_folder, self._filename)))
        if self.split == "train":
            data = data[: self.split_ratio]
        elif self.split == "test":
            data = data[self.split_ratio :]
        self.data = data.transpose(0, 1).unsqueeze(2).contiguous()

    def __getitem__(self, idx: int) -> torch.Tensor:
        """
        Args:
            idx (int): Index
        Returns:
            torch.Tensor: Video frames (torch Tensor[T, C, H, W]). The `T` is the number of frames.
        """
        data = self.data[idx]
        if self.transform is not None:
            data = self.transform(data)

        return data

    def __len__(self) -> int:
        return len(self.data)

    def _check_exists(self) -> bool:
        return os.path.exists(os.path.join(self._base_folder, self._filename))

    def download(self) -> None:
        if self._check_exists():
            return

        download_url(
            url=self._URL,
            root=self._base_folder,
            filename=self._filename,
            md5="be083ec986bfe91a449d63653c411eb2",
        )
