import os
import shutil
from pathlib import Path
from typing import Any, Callable, Optional, Tuple, Union

import numpy as np
from PIL import Image

from .utils import download_and_extract_archive, download_url, verify_str_arg
from .vision import VisionDataset


class SBDataset(VisionDataset):
    """`Semantic Boundaries Dataset <http://home.bharathh.info/pubs/codes/SBD/download.html>`_

    The SBD currently contains annotations from 11355 images taken from the PASCAL VOC 2011 dataset.

    .. note ::

        Please note that the train and val splits included with this dataset are different from
        the splits in the PASCAL VOC dataset. In particular some "train" images might be part of
        VOC2012 val.
        If you are interested in testing on VOC 2012 val, then use `image_set='train_noval'`,
        which excludes all val images.

    .. warning::

        This class needs `scipy <https://docs.scipy.org/doc/>`_ to load target files from `.mat` format.

    Args:
        root (str or ``pathlib.Path``): Root directory of the Semantic Boundaries Dataset
        image_set (string, optional): Select the image_set to use, ``train``, ``val`` or ``train_noval``.
            Image set ``train_noval`` excludes VOC 2012 val images.
        mode (string, optional): Select target type. Possible values 'boundaries' or 'segmentation'.
            In case of 'boundaries', the target is an array of shape `[num_classes, H, W]`,
            where `num_classes=20`.
        download (bool, optional): If true, downloads the dataset from the internet and
            puts it in root directory. If dataset is already downloaded, it is not
            downloaded again.
        transforms (callable, optional): A function/transform that takes input sample and its target as entry
            and returns a transformed version. Input sample is PIL image and target is a numpy array
            if `mode='boundaries'` or PIL image if `mode='segmentation'`.
    """

    url = "https://www2.eecs.berkeley.edu/Research/Projects/CS/vision/grouping/semantic_contours/benchmark.tgz"
    md5 = "82b4d87ceb2ed10f6038a1cba92111cb"
    filename = "benchmark.tgz"

    voc_train_url = "https://www.cs.cornell.edu/~bharathh/train_noval.txt"
    voc_split_filename = "train_noval.txt"
    voc_split_md5 = "79bff800c5f0b1ec6b21080a3c066722"

    def __init__(
        self,
        root: Union[str, Path],
        image_set: str = "train",
        mode: str = "boundaries",
        download: bool = False,
        transforms: Optional[Callable] = None,
    ) -> None:

        try:
            from scipy.io import loadmat

            self._loadmat = loadmat
        except ImportError:
            raise RuntimeError("Scipy is not found. This dataset needs to have scipy installed: pip install scipy")

        super().__init__(root, transforms)
        self.image_set = verify_str_arg(image_set, "image_set", ("train", "val", "train_noval"))
        self.mode = verify_str_arg(mode, "mode", ("segmentation", "boundaries"))
        self.num_classes = 20

        sbd_root = self.root
        image_dir = os.path.join(sbd_root, "img")
        mask_dir = os.path.join(sbd_root, "cls")

        if download:
            download_and_extract_archive(self.url, self.root, filename=self.filename, md5=self.md5)
            extracted_ds_root = os.path.join(self.root, "benchmark_RELEASE", "dataset")
            for f in ["cls", "img", "inst", "train.txt", "val.txt"]:
                old_path = os.path.join(extracted_ds_root, f)
                shutil.move(old_path, sbd_root)
            if self.image_set == "train_noval":
                # Note: this is failing as of June 2024 https://github.com/pytorch/vision/issues/8471
                download_url(self.voc_train_url, sbd_root, self.voc_split_filename, self.voc_split_md5)

        if not os.path.isdir(sbd_root):
            raise RuntimeError("Dataset not found or corrupted. You can use download=True to download it")

        split_f = os.path.join(sbd_root, image_set.rstrip("\n") + ".txt")

        with open(os.path.join(split_f)) as fh:
            file_names = [x.strip() for x in fh.readlines()]

        self.images = [os.path.join(image_dir, x + ".jpg") for x in file_names]
        self.masks = [os.path.join(mask_dir, x + ".mat") for x in file_names]

        self._get_target = self._get_segmentation_target if self.mode == "segmentation" else self._get_boundaries_target

    def _get_segmentation_target(self, filepath: str) -> Image.Image:
        mat = self._loadmat(filepath)
        return Image.fromarray(mat["GTcls"][0]["Segmentation"][0])

    def _get_boundaries_target(self, filepath: str) -> np.ndarray:
        mat = self._loadmat(filepath)
        return np.concatenate(
            [np.expand_dims(mat["GTcls"][0]["Boundaries"][0][i][0].toarray(), axis=0) for i in range(self.num_classes)],
            axis=0,
        )

    def __getitem__(self, index: int) -> Tuple[Any, Any]:
        img = Image.open(self.images[index]).convert("RGB")
        target = self._get_target(self.masks[index])

        if self.transforms is not None:
            img, target = self.transforms(img, target)

        return img, target

    def __len__(self) -> int:
        return len(self.images)

    def extra_repr(self) -> str:
        lines = ["Image set: {image_set}", "Mode: {mode}"]
        return "\n".join(lines).format(**self.__dict__)
