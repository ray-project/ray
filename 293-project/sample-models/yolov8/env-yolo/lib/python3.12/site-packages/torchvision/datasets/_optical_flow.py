import itertools
import os
from abc import ABC, abstractmethod
from glob import glob
from pathlib import Path
from typing import Callable, List, Optional, Tuple, Union

import numpy as np
import torch
from PIL import Image

from ..io.image import decode_png, read_file
from .utils import _read_pfm, verify_str_arg
from .vision import VisionDataset

T1 = Tuple[Image.Image, Image.Image, Optional[np.ndarray], Optional[np.ndarray]]
T2 = Tuple[Image.Image, Image.Image, Optional[np.ndarray]]


__all__ = (
    "KittiFlow",
    "Sintel",
    "FlyingThings3D",
    "FlyingChairs",
    "HD1K",
)


class FlowDataset(ABC, VisionDataset):
    # Some datasets like Kitti have a built-in valid_flow_mask, indicating which flow values are valid
    # For those we return (img1, img2, flow, valid_flow_mask), and for the rest we return (img1, img2, flow),
    # and it's up to whatever consumes the dataset to decide what valid_flow_mask should be.
    _has_builtin_flow_mask = False

    def __init__(self, root: Union[str, Path], transforms: Optional[Callable] = None) -> None:

        super().__init__(root=root)
        self.transforms = transforms

        self._flow_list: List[str] = []
        self._image_list: List[List[str]] = []

    def _read_img(self, file_name: str) -> Image.Image:
        img = Image.open(file_name)
        if img.mode != "RGB":
            img = img.convert("RGB")  # type: ignore[assignment]
        return img

    @abstractmethod
    def _read_flow(self, file_name: str):
        # Return the flow or a tuple with the flow and the valid_flow_mask if _has_builtin_flow_mask is True
        pass

    def __getitem__(self, index: int) -> Union[T1, T2]:

        img1 = self._read_img(self._image_list[index][0])
        img2 = self._read_img(self._image_list[index][1])

        if self._flow_list:  # it will be empty for some dataset when split="test"
            flow = self._read_flow(self._flow_list[index])
            if self._has_builtin_flow_mask:
                flow, valid_flow_mask = flow
            else:
                valid_flow_mask = None
        else:
            flow = valid_flow_mask = None

        if self.transforms is not None:
            img1, img2, flow, valid_flow_mask = self.transforms(img1, img2, flow, valid_flow_mask)

        if self._has_builtin_flow_mask or valid_flow_mask is not None:
            # The `or valid_flow_mask is not None` part is here because the mask can be generated within a transform
            return img1, img2, flow, valid_flow_mask
        else:
            return img1, img2, flow

    def __len__(self) -> int:
        return len(self._image_list)

    def __rmul__(self, v: int) -> torch.utils.data.ConcatDataset:
        return torch.utils.data.ConcatDataset([self] * v)


class Sintel(FlowDataset):
    """`Sintel <http://sintel.is.tue.mpg.de/>`_ Dataset for optical flow.

    The dataset is expected to have the following structure: ::

        root
            Sintel
                testing
                    clean
                        scene_1
                        scene_2
                        ...
                    final
                        scene_1
                        scene_2
                        ...
                training
                    clean
                        scene_1
                        scene_2
                        ...
                    final
                        scene_1
                        scene_2
                        ...
                    flow
                        scene_1
                        scene_2
                        ...

    Args:
        root (str or ``pathlib.Path``): Root directory of the Sintel Dataset.
        split (string, optional): The dataset split, either "train" (default) or "test"
        pass_name (string, optional): The pass to use, either "clean" (default), "final", or "both". See link above for
            details on the different passes.
        transforms (callable, optional): A function/transform that takes in
            ``img1, img2, flow, valid_flow_mask`` and returns a transformed version.
            ``valid_flow_mask`` is expected for consistency with other datasets which
            return a built-in valid mask, such as :class:`~torchvision.datasets.KittiFlow`.
    """

    def __init__(
        self,
        root: Union[str, Path],
        split: str = "train",
        pass_name: str = "clean",
        transforms: Optional[Callable] = None,
    ) -> None:
        super().__init__(root=root, transforms=transforms)

        verify_str_arg(split, "split", valid_values=("train", "test"))
        verify_str_arg(pass_name, "pass_name", valid_values=("clean", "final", "both"))
        passes = ["clean", "final"] if pass_name == "both" else [pass_name]

        root = Path(root) / "Sintel"
        flow_root = root / "training" / "flow"

        for pass_name in passes:
            split_dir = "training" if split == "train" else split
            image_root = root / split_dir / pass_name
            for scene in os.listdir(image_root):
                image_list = sorted(glob(str(image_root / scene / "*.png")))
                for i in range(len(image_list) - 1):
                    self._image_list += [[image_list[i], image_list[i + 1]]]

                if split == "train":
                    self._flow_list += sorted(glob(str(flow_root / scene / "*.flo")))

    def __getitem__(self, index: int) -> Union[T1, T2]:
        """Return example at given index.

        Args:
            index(int): The index of the example to retrieve

        Returns:
            tuple: A 3-tuple with ``(img1, img2, flow)``.
            The flow is a numpy array of shape (2, H, W) and the images are PIL images.
            ``flow`` is None if ``split="test"``.
            If a valid flow mask is generated within the ``transforms`` parameter,
            a 4-tuple with ``(img1, img2, flow, valid_flow_mask)`` is returned.
        """
        return super().__getitem__(index)

    def _read_flow(self, file_name: str) -> np.ndarray:
        return _read_flo(file_name)


class KittiFlow(FlowDataset):
    """`KITTI <http://www.cvlibs.net/datasets/kitti/eval_scene_flow.php?benchmark=flow>`__ dataset for optical flow (2015).

    The dataset is expected to have the following structure: ::

        root
            KittiFlow
                testing
                    image_2
                training
                    image_2
                    flow_occ

    Args:
        root (str or ``pathlib.Path``): Root directory of the KittiFlow Dataset.
        split (string, optional): The dataset split, either "train" (default) or "test"
        transforms (callable, optional): A function/transform that takes in
            ``img1, img2, flow, valid_flow_mask`` and returns a transformed version.
    """

    _has_builtin_flow_mask = True

    def __init__(self, root: Union[str, Path], split: str = "train", transforms: Optional[Callable] = None) -> None:
        super().__init__(root=root, transforms=transforms)

        verify_str_arg(split, "split", valid_values=("train", "test"))

        root = Path(root) / "KittiFlow" / (split + "ing")
        images1 = sorted(glob(str(root / "image_2" / "*_10.png")))
        images2 = sorted(glob(str(root / "image_2" / "*_11.png")))

        if not images1 or not images2:
            raise FileNotFoundError(
                "Could not find the Kitti flow images. Please make sure the directory structure is correct."
            )

        for img1, img2 in zip(images1, images2):
            self._image_list += [[img1, img2]]

        if split == "train":
            self._flow_list = sorted(glob(str(root / "flow_occ" / "*_10.png")))

    def __getitem__(self, index: int) -> Union[T1, T2]:
        """Return example at given index.

        Args:
            index(int): The index of the example to retrieve

        Returns:
            tuple: A 4-tuple with ``(img1, img2, flow, valid_flow_mask)``
            where ``valid_flow_mask`` is a numpy boolean mask of shape (H, W)
            indicating which flow values are valid. The flow is a numpy array of
            shape (2, H, W) and the images are PIL images. ``flow`` and ``valid_flow_mask`` are None if
            ``split="test"``.
        """
        return super().__getitem__(index)

    def _read_flow(self, file_name: str) -> Tuple[np.ndarray, np.ndarray]:
        return _read_16bits_png_with_flow_and_valid_mask(file_name)


class FlyingChairs(FlowDataset):
    """`FlyingChairs <https://lmb.informatik.uni-freiburg.de/resources/datasets/FlyingChairs.en.html#flyingchairs>`_ Dataset for optical flow.

    You will also need to download the FlyingChairs_train_val.txt file from the dataset page.

    The dataset is expected to have the following structure: ::

        root
            FlyingChairs
                data
                    00001_flow.flo
                    00001_img1.ppm
                    00001_img2.ppm
                    ...
                FlyingChairs_train_val.txt


    Args:
        root (str or ``pathlib.Path``): Root directory of the FlyingChairs Dataset.
        split (string, optional): The dataset split, either "train" (default) or "val"
        transforms (callable, optional): A function/transform that takes in
            ``img1, img2, flow, valid_flow_mask`` and returns a transformed version.
            ``valid_flow_mask`` is expected for consistency with other datasets which
            return a built-in valid mask, such as :class:`~torchvision.datasets.KittiFlow`.
    """

    def __init__(self, root: Union[str, Path], split: str = "train", transforms: Optional[Callable] = None) -> None:
        super().__init__(root=root, transforms=transforms)

        verify_str_arg(split, "split", valid_values=("train", "val"))

        root = Path(root) / "FlyingChairs"
        images = sorted(glob(str(root / "data" / "*.ppm")))
        flows = sorted(glob(str(root / "data" / "*.flo")))

        split_file_name = "FlyingChairs_train_val.txt"

        if not os.path.exists(root / split_file_name):
            raise FileNotFoundError(
                "The FlyingChairs_train_val.txt file was not found - please download it from the dataset page (see docstring)."
            )

        split_list = np.loadtxt(str(root / split_file_name), dtype=np.int32)
        for i in range(len(flows)):
            split_id = split_list[i]
            if (split == "train" and split_id == 1) or (split == "val" and split_id == 2):
                self._flow_list += [flows[i]]
                self._image_list += [[images[2 * i], images[2 * i + 1]]]

    def __getitem__(self, index: int) -> Union[T1, T2]:
        """Return example at given index.

        Args:
            index(int): The index of the example to retrieve

        Returns:
            tuple: A 3-tuple with ``(img1, img2, flow)``.
            The flow is a numpy array of shape (2, H, W) and the images are PIL images.
            ``flow`` is None if ``split="val"``.
            If a valid flow mask is generated within the ``transforms`` parameter,
            a 4-tuple with ``(img1, img2, flow, valid_flow_mask)`` is returned.
        """
        return super().__getitem__(index)

    def _read_flow(self, file_name: str) -> np.ndarray:
        return _read_flo(file_name)


class FlyingThings3D(FlowDataset):
    """`FlyingThings3D <https://lmb.informatik.uni-freiburg.de/resources/datasets/SceneFlowDatasets.en.html>`_ dataset for optical flow.

    The dataset is expected to have the following structure: ::

        root
            FlyingThings3D
                frames_cleanpass
                    TEST
                    TRAIN
                frames_finalpass
                    TEST
                    TRAIN
                optical_flow
                    TEST
                    TRAIN

    Args:
        root (str or ``pathlib.Path``): Root directory of the intel FlyingThings3D Dataset.
        split (string, optional): The dataset split, either "train" (default) or "test"
        pass_name (string, optional): The pass to use, either "clean" (default) or "final" or "both". See link above for
            details on the different passes.
        camera (string, optional): Which camera to return images from. Can be either "left" (default) or "right" or "both".
        transforms (callable, optional): A function/transform that takes in
            ``img1, img2, flow, valid_flow_mask`` and returns a transformed version.
            ``valid_flow_mask`` is expected for consistency with other datasets which
            return a built-in valid mask, such as :class:`~torchvision.datasets.KittiFlow`.
    """

    def __init__(
        self,
        root: Union[str, Path],
        split: str = "train",
        pass_name: str = "clean",
        camera: str = "left",
        transforms: Optional[Callable] = None,
    ) -> None:
        super().__init__(root=root, transforms=transforms)

        verify_str_arg(split, "split", valid_values=("train", "test"))
        split = split.upper()

        verify_str_arg(pass_name, "pass_name", valid_values=("clean", "final", "both"))
        passes = {
            "clean": ["frames_cleanpass"],
            "final": ["frames_finalpass"],
            "both": ["frames_cleanpass", "frames_finalpass"],
        }[pass_name]

        verify_str_arg(camera, "camera", valid_values=("left", "right", "both"))
        cameras = ["left", "right"] if camera == "both" else [camera]

        root = Path(root) / "FlyingThings3D"

        directions = ("into_future", "into_past")
        for pass_name, camera, direction in itertools.product(passes, cameras, directions):
            image_dirs = sorted(glob(str(root / pass_name / split / "*/*")))
            image_dirs = sorted(Path(image_dir) / camera for image_dir in image_dirs)

            flow_dirs = sorted(glob(str(root / "optical_flow" / split / "*/*")))
            flow_dirs = sorted(Path(flow_dir) / direction / camera for flow_dir in flow_dirs)

            if not image_dirs or not flow_dirs:
                raise FileNotFoundError(
                    "Could not find the FlyingThings3D flow images. "
                    "Please make sure the directory structure is correct."
                )

            for image_dir, flow_dir in zip(image_dirs, flow_dirs):
                images = sorted(glob(str(image_dir / "*.png")))
                flows = sorted(glob(str(flow_dir / "*.pfm")))
                for i in range(len(flows) - 1):
                    if direction == "into_future":
                        self._image_list += [[images[i], images[i + 1]]]
                        self._flow_list += [flows[i]]
                    elif direction == "into_past":
                        self._image_list += [[images[i + 1], images[i]]]
                        self._flow_list += [flows[i + 1]]

    def __getitem__(self, index: int) -> Union[T1, T2]:
        """Return example at given index.

        Args:
            index(int): The index of the example to retrieve

        Returns:
            tuple: A 3-tuple with ``(img1, img2, flow)``.
            The flow is a numpy array of shape (2, H, W) and the images are PIL images.
            ``flow`` is None if ``split="test"``.
            If a valid flow mask is generated within the ``transforms`` parameter,
            a 4-tuple with ``(img1, img2, flow, valid_flow_mask)`` is returned.
        """
        return super().__getitem__(index)

    def _read_flow(self, file_name: str) -> np.ndarray:
        return _read_pfm(file_name)


class HD1K(FlowDataset):
    """`HD1K <http://hci-benchmark.iwr.uni-heidelberg.de/>`__ dataset for optical flow.

    The dataset is expected to have the following structure: ::

        root
            hd1k
                hd1k_challenge
                    image_2
                hd1k_flow_gt
                    flow_occ
                hd1k_input
                    image_2

    Args:
        root (str or ``pathlib.Path``): Root directory of the HD1K Dataset.
        split (string, optional): The dataset split, either "train" (default) or "test"
        transforms (callable, optional): A function/transform that takes in
            ``img1, img2, flow, valid_flow_mask`` and returns a transformed version.
    """

    _has_builtin_flow_mask = True

    def __init__(self, root: Union[str, Path], split: str = "train", transforms: Optional[Callable] = None) -> None:
        super().__init__(root=root, transforms=transforms)

        verify_str_arg(split, "split", valid_values=("train", "test"))

        root = Path(root) / "hd1k"
        if split == "train":
            # There are 36 "sequences" and we don't want seq i to overlap with seq i + 1, so we need this for loop
            for seq_idx in range(36):
                flows = sorted(glob(str(root / "hd1k_flow_gt" / "flow_occ" / f"{seq_idx:06d}_*.png")))
                images = sorted(glob(str(root / "hd1k_input" / "image_2" / f"{seq_idx:06d}_*.png")))
                for i in range(len(flows) - 1):
                    self._flow_list += [flows[i]]
                    self._image_list += [[images[i], images[i + 1]]]
        else:
            images1 = sorted(glob(str(root / "hd1k_challenge" / "image_2" / "*10.png")))
            images2 = sorted(glob(str(root / "hd1k_challenge" / "image_2" / "*11.png")))
            for image1, image2 in zip(images1, images2):
                self._image_list += [[image1, image2]]

        if not self._image_list:
            raise FileNotFoundError(
                "Could not find the HD1K images. Please make sure the directory structure is correct."
            )

    def _read_flow(self, file_name: str) -> Tuple[np.ndarray, np.ndarray]:
        return _read_16bits_png_with_flow_and_valid_mask(file_name)

    def __getitem__(self, index: int) -> Union[T1, T2]:
        """Return example at given index.

        Args:
            index(int): The index of the example to retrieve

        Returns:
            tuple: A 4-tuple with ``(img1, img2, flow, valid_flow_mask)`` where ``valid_flow_mask``
            is a numpy boolean mask of shape (H, W)
            indicating which flow values are valid. The flow is a numpy array of
            shape (2, H, W) and the images are PIL images. ``flow`` and ``valid_flow_mask`` are None if
            ``split="test"``.
        """
        return super().__getitem__(index)


def _read_flo(file_name: str) -> np.ndarray:
    """Read .flo file in Middlebury format"""
    # Code adapted from:
    # http://stackoverflow.com/questions/28013200/reading-middlebury-flow-files-with-python-bytes-array-numpy
    # Everything needs to be in little Endian according to
    # https://vision.middlebury.edu/flow/code/flow-code/README.txt
    with open(file_name, "rb") as f:
        magic = np.fromfile(f, "c", count=4).tobytes()
        if magic != b"PIEH":
            raise ValueError("Magic number incorrect. Invalid .flo file")

        w = int(np.fromfile(f, "<i4", count=1))
        h = int(np.fromfile(f, "<i4", count=1))
        data = np.fromfile(f, "<f4", count=2 * w * h)
        return data.reshape(h, w, 2).transpose(2, 0, 1)


def _read_16bits_png_with_flow_and_valid_mask(file_name: str) -> Tuple[np.ndarray, np.ndarray]:

    flow_and_valid = decode_png(read_file(file_name)).to(torch.float32)
    flow, valid_flow_mask = flow_and_valid[:2, :, :], flow_and_valid[2, :, :]
    flow = (flow - 2**15) / 64  # This conversion is explained somewhere on the kitti archive
    valid_flow_mask = valid_flow_mask.bool()

    # For consistency with other datasets, we convert to numpy
    return flow.numpy(), valid_flow_mask.numpy()
