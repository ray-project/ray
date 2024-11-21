import os
from os.path import abspath, expanduser
from pathlib import Path

from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import torch
from PIL import Image

from .utils import download_and_extract_archive, download_file_from_google_drive, extract_archive, verify_str_arg
from .vision import VisionDataset


class WIDERFace(VisionDataset):
    """`WIDERFace <http://shuoyang1213.me/WIDERFACE/>`_ Dataset.

    Args:
        root (str or ``pathlib.Path``): Root directory where images and annotations are downloaded to.
            Expects the following folder structure if download=False:

            .. code::

                <root>
                    └── widerface
                        ├── wider_face_split ('wider_face_split.zip' if compressed)
                        ├── WIDER_train ('WIDER_train.zip' if compressed)
                        ├── WIDER_val ('WIDER_val.zip' if compressed)
                        └── WIDER_test ('WIDER_test.zip' if compressed)
        split (string): The dataset split to use. One of {``train``, ``val``, ``test``}.
            Defaults to ``train``.
        transform (callable, optional): A function/transform that takes in a PIL image
            and returns a transformed version. E.g, ``transforms.RandomCrop``
        target_transform (callable, optional): A function/transform that takes in the
            target and transforms it.
        download (bool, optional): If true, downloads the dataset from the internet and
            puts it in root directory. If dataset is already downloaded, it is not
            downloaded again.

            .. warning::

                To download the dataset `gdown <https://github.com/wkentaro/gdown>`_ is required.

    """

    BASE_FOLDER = "widerface"
    FILE_LIST = [
        # File ID                             MD5 Hash                            Filename
        ("15hGDLhsx8bLgLcIRD5DhYt5iBxnjNF1M", "3fedf70df600953d25982bcd13d91ba2", "WIDER_train.zip"),
        ("1GUCogbp16PMGa39thoMMeWxp7Rp5oM8Q", "dfa7d7e790efa35df3788964cf0bbaea", "WIDER_val.zip"),
        ("1HIfDbVEWKmsYKJZm4lchTBDLW5N7dY5T", "e5d8f4248ed24c334bbd12f49c29dd40", "WIDER_test.zip"),
    ]
    ANNOTATIONS_FILE = (
        "http://shuoyang1213.me/WIDERFACE/support/bbx_annotation/wider_face_split.zip",
        "0e3767bcf0e326556d407bf5bff5d27c",
        "wider_face_split.zip",
    )

    def __init__(
        self,
        root: Union[str, Path],
        split: str = "train",
        transform: Optional[Callable] = None,
        target_transform: Optional[Callable] = None,
        download: bool = False,
    ) -> None:
        super().__init__(
            root=os.path.join(root, self.BASE_FOLDER), transform=transform, target_transform=target_transform
        )
        # check arguments
        self.split = verify_str_arg(split, "split", ("train", "val", "test"))

        if download:
            self.download()

        if not self._check_integrity():
            raise RuntimeError("Dataset not found or corrupted. You can use download=True to download and prepare it")

        self.img_info: List[Dict[str, Union[str, Dict[str, torch.Tensor]]]] = []
        if self.split in ("train", "val"):
            self.parse_train_val_annotations_file()
        else:
            self.parse_test_annotations_file()

    def __getitem__(self, index: int) -> Tuple[Any, Any]:
        """
        Args:
            index (int): Index

        Returns:
            tuple: (image, target) where target is a dict of annotations for all faces in the image.
            target=None for the test split.
        """

        # stay consistent with other datasets and return a PIL Image
        img = Image.open(self.img_info[index]["img_path"])  # type: ignore[arg-type]

        if self.transform is not None:
            img = self.transform(img)

        target = None if self.split == "test" else self.img_info[index]["annotations"]
        if self.target_transform is not None:
            target = self.target_transform(target)

        return img, target

    def __len__(self) -> int:
        return len(self.img_info)

    def extra_repr(self) -> str:
        lines = ["Split: {split}"]
        return "\n".join(lines).format(**self.__dict__)

    def parse_train_val_annotations_file(self) -> None:
        filename = "wider_face_train_bbx_gt.txt" if self.split == "train" else "wider_face_val_bbx_gt.txt"
        filepath = os.path.join(self.root, "wider_face_split", filename)

        with open(filepath) as f:
            lines = f.readlines()
            file_name_line, num_boxes_line, box_annotation_line = True, False, False
            num_boxes, box_counter = 0, 0
            labels = []
            for line in lines:
                line = line.rstrip()
                if file_name_line:
                    img_path = os.path.join(self.root, "WIDER_" + self.split, "images", line)
                    img_path = abspath(expanduser(img_path))
                    file_name_line = False
                    num_boxes_line = True
                elif num_boxes_line:
                    num_boxes = int(line)
                    num_boxes_line = False
                    box_annotation_line = True
                elif box_annotation_line:
                    box_counter += 1
                    line_split = line.split(" ")
                    line_values = [int(x) for x in line_split]
                    labels.append(line_values)
                    if box_counter >= num_boxes:
                        box_annotation_line = False
                        file_name_line = True
                        labels_tensor = torch.tensor(labels)
                        self.img_info.append(
                            {
                                "img_path": img_path,
                                "annotations": {
                                    "bbox": labels_tensor[:, 0:4].clone(),  # x, y, width, height
                                    "blur": labels_tensor[:, 4].clone(),
                                    "expression": labels_tensor[:, 5].clone(),
                                    "illumination": labels_tensor[:, 6].clone(),
                                    "occlusion": labels_tensor[:, 7].clone(),
                                    "pose": labels_tensor[:, 8].clone(),
                                    "invalid": labels_tensor[:, 9].clone(),
                                },
                            }
                        )
                        box_counter = 0
                        labels.clear()
                else:
                    raise RuntimeError(f"Error parsing annotation file {filepath}")

    def parse_test_annotations_file(self) -> None:
        filepath = os.path.join(self.root, "wider_face_split", "wider_face_test_filelist.txt")
        filepath = abspath(expanduser(filepath))
        with open(filepath) as f:
            lines = f.readlines()
            for line in lines:
                line = line.rstrip()
                img_path = os.path.join(self.root, "WIDER_test", "images", line)
                img_path = abspath(expanduser(img_path))
                self.img_info.append({"img_path": img_path})

    def _check_integrity(self) -> bool:
        # Allow original archive to be deleted (zip). Only need the extracted images
        all_files = self.FILE_LIST.copy()
        all_files.append(self.ANNOTATIONS_FILE)
        for (_, md5, filename) in all_files:
            file, ext = os.path.splitext(filename)
            extracted_dir = os.path.join(self.root, file)
            if not os.path.exists(extracted_dir):
                return False
        return True

    def download(self) -> None:
        if self._check_integrity():
            print("Files already downloaded and verified")
            return

        # download and extract image data
        for (file_id, md5, filename) in self.FILE_LIST:
            download_file_from_google_drive(file_id, self.root, filename, md5)
            filepath = os.path.join(self.root, filename)
            extract_archive(filepath)

        # download and extract annotation files
        download_and_extract_archive(
            url=self.ANNOTATIONS_FILE[0], download_root=self.root, md5=self.ANNOTATIONS_FILE[1]
        )
