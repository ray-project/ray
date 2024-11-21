import functools
import json
import os
import random
import shutil
from abc import ABC, abstractmethod
from glob import glob
from pathlib import Path
from typing import Callable, cast, List, Optional, Tuple, Union

import numpy as np
from PIL import Image

from .utils import _read_pfm, download_and_extract_archive, verify_str_arg
from .vision import VisionDataset

T1 = Tuple[Image.Image, Image.Image, Optional[np.ndarray], np.ndarray]
T2 = Tuple[Image.Image, Image.Image, Optional[np.ndarray]]

__all__ = ()

_read_pfm_file = functools.partial(_read_pfm, slice_channels=1)


class StereoMatchingDataset(ABC, VisionDataset):
    """Base interface for Stereo matching datasets"""

    _has_built_in_disparity_mask = False

    def __init__(self, root: Union[str, Path], transforms: Optional[Callable] = None) -> None:
        """
        Args:
            root(str): Root directory of the dataset.
            transforms(callable, optional): A function/transform that takes in Tuples of
                (images, disparities, valid_masks) and returns a transformed version of each of them.
                images is a Tuple of (``PIL.Image``, ``PIL.Image``)
                disparities is a Tuple of (``np.ndarray``, ``np.ndarray``) with shape (1, H, W)
                valid_masks is a Tuple of (``np.ndarray``, ``np.ndarray``) with shape (H, W)
                In some cases, when a dataset does not provide disparities, the ``disparities`` and
                ``valid_masks`` can be Tuples containing None values.
                For training splits generally the datasets provide a minimal guarantee of
                images: (``PIL.Image``, ``PIL.Image``)
                disparities: (``np.ndarray``, ``None``) with shape (1, H, W)
                Optionally, based on the dataset, it can return a ``mask`` as well:
                valid_masks: (``np.ndarray | None``, ``None``) with shape (H, W)
                For some test splits, the datasets provides outputs that look like:
                imgaes: (``PIL.Image``, ``PIL.Image``)
                disparities: (``None``, ``None``)
                Optionally, based on the dataset, it can return a ``mask`` as well:
                valid_masks: (``None``, ``None``)
        """
        super().__init__(root=root)
        self.transforms = transforms

        self._images = []  # type: ignore
        self._disparities = []  # type: ignore

    def _read_img(self, file_path: Union[str, Path]) -> Image.Image:
        img = Image.open(file_path)
        if img.mode != "RGB":
            img = img.convert("RGB")  # type: ignore [assignment]
        return img

    def _scan_pairs(
        self,
        paths_left_pattern: str,
        paths_right_pattern: Optional[str] = None,
    ) -> List[Tuple[str, Optional[str]]]:

        left_paths = list(sorted(glob(paths_left_pattern)))

        right_paths: List[Union[None, str]]
        if paths_right_pattern:
            right_paths = list(sorted(glob(paths_right_pattern)))
        else:
            right_paths = list(None for _ in left_paths)

        if not left_paths:
            raise FileNotFoundError(f"Could not find any files matching the patterns: {paths_left_pattern}")

        if not right_paths:
            raise FileNotFoundError(f"Could not find any files matching the patterns: {paths_right_pattern}")

        if len(left_paths) != len(right_paths):
            raise ValueError(
                f"Found {len(left_paths)} left files but {len(right_paths)} right files using:\n "
                f"left pattern: {paths_left_pattern}\n"
                f"right pattern: {paths_right_pattern}\n"
            )

        paths = list((left, right) for left, right in zip(left_paths, right_paths))
        return paths

    @abstractmethod
    def _read_disparity(self, file_path: str) -> Tuple[Optional[np.ndarray], Optional[np.ndarray]]:
        # function that returns a disparity map and an occlusion map
        pass

    def __getitem__(self, index: int) -> Union[T1, T2]:
        """Return example at given index.

        Args:
            index(int): The index of the example to retrieve

        Returns:
            tuple: A 3 or 4-tuple with ``(img_left, img_right, disparity, Optional[valid_mask])`` where ``valid_mask``
                can be a numpy boolean mask of shape (H, W) if the dataset provides a file
                indicating which disparity pixels are valid. The disparity is a numpy array of
                shape (1, H, W) and the images are PIL images. ``disparity`` is None for
                datasets on which for ``split="test"`` the authors did not provide annotations.
        """
        img_left = self._read_img(self._images[index][0])
        img_right = self._read_img(self._images[index][1])

        dsp_map_left, valid_mask_left = self._read_disparity(self._disparities[index][0])
        dsp_map_right, valid_mask_right = self._read_disparity(self._disparities[index][1])

        imgs = (img_left, img_right)
        dsp_maps = (dsp_map_left, dsp_map_right)
        valid_masks = (valid_mask_left, valid_mask_right)

        if self.transforms is not None:
            (
                imgs,
                dsp_maps,
                valid_masks,
            ) = self.transforms(imgs, dsp_maps, valid_masks)

        if self._has_built_in_disparity_mask or valid_masks[0] is not None:
            return imgs[0], imgs[1], dsp_maps[0], cast(np.ndarray, valid_masks[0])
        else:
            return imgs[0], imgs[1], dsp_maps[0]

    def __len__(self) -> int:
        return len(self._images)


class CarlaStereo(StereoMatchingDataset):
    """
    Carla simulator data linked in the `CREStereo github repo <https://github.com/megvii-research/CREStereo>`_.

    The dataset is expected to have the following structure: ::

        root
            carla-highres
                trainingF
                    scene1
                        img0.png
                        img1.png
                        disp0GT.pfm
                        disp1GT.pfm
                        calib.txt
                    scene2
                        img0.png
                        img1.png
                        disp0GT.pfm
                        disp1GT.pfm
                        calib.txt
                    ...

    Args:
        root (str or ``pathlib.Path``): Root directory where `carla-highres` is located.
        transforms (callable, optional): A function/transform that takes in a sample and returns a transformed version.
    """

    def __init__(self, root: Union[str, Path], transforms: Optional[Callable] = None) -> None:
        super().__init__(root, transforms)

        root = Path(root) / "carla-highres"

        left_image_pattern = str(root / "trainingF" / "*" / "im0.png")
        right_image_pattern = str(root / "trainingF" / "*" / "im1.png")
        imgs = self._scan_pairs(left_image_pattern, right_image_pattern)
        self._images = imgs

        left_disparity_pattern = str(root / "trainingF" / "*" / "disp0GT.pfm")
        right_disparity_pattern = str(root / "trainingF" / "*" / "disp1GT.pfm")
        disparities = self._scan_pairs(left_disparity_pattern, right_disparity_pattern)
        self._disparities = disparities

    def _read_disparity(self, file_path: str) -> Tuple[np.ndarray, None]:
        disparity_map = _read_pfm_file(file_path)
        disparity_map = np.abs(disparity_map)  # ensure that the disparity is positive
        valid_mask = None
        return disparity_map, valid_mask

    def __getitem__(self, index: int) -> T1:
        """Return example at given index.

        Args:
            index(int): The index of the example to retrieve

        Returns:
            tuple: A 3-tuple with ``(img_left, img_right, disparity)``.
            The disparity is a numpy array of shape (1, H, W) and the images are PIL images.
            If a ``valid_mask`` is generated within the ``transforms`` parameter,
            a 4-tuple with ``(img_left, img_right, disparity, valid_mask)`` is returned.
        """
        return cast(T1, super().__getitem__(index))


class Kitti2012Stereo(StereoMatchingDataset):
    """
    KITTI dataset from the `2012 stereo evaluation benchmark <http://www.cvlibs.net/datasets/kitti/eval_stereo_flow.php>`_.
    Uses the RGB images for consistency with KITTI 2015.

    The dataset is expected to have the following structure: ::

        root
            Kitti2012
                testing
                    colored_0
                        1_10.png
                        2_10.png
                        ...
                    colored_1
                        1_10.png
                        2_10.png
                        ...
                training
                    colored_0
                        1_10.png
                        2_10.png
                        ...
                    colored_1
                        1_10.png
                        2_10.png
                        ...
                    disp_noc
                        1.png
                        2.png
                        ...
                    calib

    Args:
        root (str or ``pathlib.Path``): Root directory where `Kitti2012` is located.
        split (string, optional): The dataset split of scenes, either "train" (default) or "test".
        transforms (callable, optional): A function/transform that takes in a sample and returns a transformed version.
    """

    _has_built_in_disparity_mask = True

    def __init__(self, root: Union[str, Path], split: str = "train", transforms: Optional[Callable] = None) -> None:
        super().__init__(root, transforms)

        verify_str_arg(split, "split", valid_values=("train", "test"))

        root = Path(root) / "Kitti2012" / (split + "ing")

        left_img_pattern = str(root / "colored_0" / "*_10.png")
        right_img_pattern = str(root / "colored_1" / "*_10.png")
        self._images = self._scan_pairs(left_img_pattern, right_img_pattern)

        if split == "train":
            disparity_pattern = str(root / "disp_noc" / "*.png")
            self._disparities = self._scan_pairs(disparity_pattern, None)
        else:
            self._disparities = list((None, None) for _ in self._images)

    def _read_disparity(self, file_path: str) -> Tuple[Optional[np.ndarray], None]:
        # test split has no disparity maps
        if file_path is None:
            return None, None

        disparity_map = np.asarray(Image.open(file_path)) / 256.0
        # unsqueeze the disparity map into (C, H, W) format
        disparity_map = disparity_map[None, :, :]
        valid_mask = None
        return disparity_map, valid_mask

    def __getitem__(self, index: int) -> T1:
        """Return example at given index.

        Args:
            index(int): The index of the example to retrieve

        Returns:
            tuple: A 4-tuple with ``(img_left, img_right, disparity, valid_mask)``.
            The disparity is a numpy array of shape (1, H, W) and the images are PIL images.
            ``valid_mask`` is implicitly ``None`` if the ``transforms`` parameter does not
            generate a valid mask.
            Both ``disparity`` and ``valid_mask`` are ``None`` if the dataset split is test.
        """
        return cast(T1, super().__getitem__(index))


class Kitti2015Stereo(StereoMatchingDataset):
    """
    KITTI dataset from the `2015 stereo evaluation benchmark <http://www.cvlibs.net/datasets/kitti/eval_scene_flow.php>`_.

    The dataset is expected to have the following structure: ::

        root
            Kitti2015
                testing
                    image_2
                        img1.png
                        img2.png
                        ...
                    image_3
                        img1.png
                        img2.png
                        ...
                training
                    image_2
                        img1.png
                        img2.png
                        ...
                    image_3
                        img1.png
                        img2.png
                        ...
                    disp_occ_0
                        img1.png
                        img2.png
                        ...
                    disp_occ_1
                        img1.png
                        img2.png
                        ...
                    calib

    Args:
        root (str or ``pathlib.Path``): Root directory where `Kitti2015` is located.
        split (string, optional): The dataset split of scenes, either "train" (default) or "test".
        transforms (callable, optional): A function/transform that takes in a sample and returns a transformed version.
    """

    _has_built_in_disparity_mask = True

    def __init__(self, root: Union[str, Path], split: str = "train", transforms: Optional[Callable] = None) -> None:
        super().__init__(root, transforms)

        verify_str_arg(split, "split", valid_values=("train", "test"))

        root = Path(root) / "Kitti2015" / (split + "ing")
        left_img_pattern = str(root / "image_2" / "*.png")
        right_img_pattern = str(root / "image_3" / "*.png")
        self._images = self._scan_pairs(left_img_pattern, right_img_pattern)

        if split == "train":
            left_disparity_pattern = str(root / "disp_occ_0" / "*.png")
            right_disparity_pattern = str(root / "disp_occ_1" / "*.png")
            self._disparities = self._scan_pairs(left_disparity_pattern, right_disparity_pattern)
        else:
            self._disparities = list((None, None) for _ in self._images)

    def _read_disparity(self, file_path: str) -> Tuple[Optional[np.ndarray], None]:
        # test split has no disparity maps
        if file_path is None:
            return None, None

        disparity_map = np.asarray(Image.open(file_path)) / 256.0
        # unsqueeze the disparity map into (C, H, W) format
        disparity_map = disparity_map[None, :, :]
        valid_mask = None
        return disparity_map, valid_mask

    def __getitem__(self, index: int) -> T1:
        """Return example at given index.

        Args:
            index(int): The index of the example to retrieve

        Returns:
            tuple: A 4-tuple with ``(img_left, img_right, disparity, valid_mask)``.
            The disparity is a numpy array of shape (1, H, W) and the images are PIL images.
            ``valid_mask`` is implicitly ``None`` if the ``transforms`` parameter does not
            generate a valid mask.
            Both ``disparity`` and ``valid_mask`` are ``None`` if the dataset split is test.
        """
        return cast(T1, super().__getitem__(index))


class Middlebury2014Stereo(StereoMatchingDataset):
    """Publicly available scenes from the Middlebury dataset `2014 version <https://vision.middlebury.edu/stereo/data/scenes2014/>`.

    The dataset mostly follows the original format, without containing the ambient subdirectories.  : ::

        root
            Middlebury2014
                train
                    scene1-{perfect,imperfect}
                        calib.txt
                        im{0,1}.png
                        im1E.png
                        im1L.png
                        disp{0,1}.pfm
                        disp{0,1}-n.png
                        disp{0,1}-sd.pfm
                        disp{0,1}y.pfm
                    scene2-{perfect,imperfect}
                        calib.txt
                        im{0,1}.png
                        im1E.png
                        im1L.png
                        disp{0,1}.pfm
                        disp{0,1}-n.png
                        disp{0,1}-sd.pfm
                        disp{0,1}y.pfm
                    ...
                additional
                    scene1-{perfect,imperfect}
                        calib.txt
                        im{0,1}.png
                        im1E.png
                        im1L.png
                        disp{0,1}.pfm
                        disp{0,1}-n.png
                        disp{0,1}-sd.pfm
                        disp{0,1}y.pfm
                    ...
                test
                    scene1
                        calib.txt
                        im{0,1}.png
                    scene2
                        calib.txt
                        im{0,1}.png
                    ...

    Args:
        root (str or ``pathlib.Path``): Root directory of the Middleburry 2014 Dataset.
        split (string, optional): The dataset split of scenes, either "train" (default), "test", or "additional"
        use_ambient_views (boolean, optional): Whether to use different expose or lightning views when possible.
            The dataset samples with equal probability between ``[im1.png, im1E.png, im1L.png]``.
        calibration (string, optional): Whether or not to use the calibrated (default) or uncalibrated scenes.
        transforms (callable, optional): A function/transform that takes in a sample and returns a transformed version.
        download (boolean, optional): Whether or not to download the dataset in the ``root`` directory.
    """

    splits = {
        "train": [
            "Adirondack",
            "Jadeplant",
            "Motorcycle",
            "Piano",
            "Pipes",
            "Playroom",
            "Playtable",
            "Recycle",
            "Shelves",
            "Vintage",
        ],
        "additional": [
            "Backpack",
            "Bicycle1",
            "Cable",
            "Classroom1",
            "Couch",
            "Flowers",
            "Mask",
            "Shopvac",
            "Sticks",
            "Storage",
            "Sword1",
            "Sword2",
            "Umbrella",
        ],
        "test": [
            "Plants",
            "Classroom2E",
            "Classroom2",
            "Australia",
            "DjembeL",
            "CrusadeP",
            "Crusade",
            "Hoops",
            "Bicycle2",
            "Staircase",
            "Newkuba",
            "AustraliaP",
            "Djembe",
            "Livingroom",
            "Computer",
        ],
    }

    _has_built_in_disparity_mask = True

    def __init__(
        self,
        root: Union[str, Path],
        split: str = "train",
        calibration: Optional[str] = "perfect",
        use_ambient_views: bool = False,
        transforms: Optional[Callable] = None,
        download: bool = False,
    ) -> None:
        super().__init__(root, transforms)

        verify_str_arg(split, "split", valid_values=("train", "test", "additional"))
        self.split = split

        if calibration:
            verify_str_arg(calibration, "calibration", valid_values=("perfect", "imperfect", "both", None))  # type: ignore
            if split == "test":
                raise ValueError("Split 'test' has only no calibration settings, please set `calibration=None`.")
        else:
            if split != "test":
                raise ValueError(
                    f"Split '{split}' has calibration settings, however None was provided as an argument."
                    f"\nSetting calibration to 'perfect' for split '{split}'. Available calibration settings are: 'perfect', 'imperfect', 'both'.",
                )

        if download:
            self._download_dataset(root)

        root = Path(root) / "Middlebury2014"

        if not os.path.exists(root / split):
            raise FileNotFoundError(f"The {split} directory was not found in the provided root directory")

        split_scenes = self.splits[split]
        # check that the provided root folder contains the scene splits
        if not any(
            # using startswith to account for perfect / imperfect calibrartion
            scene.startswith(s)
            for scene in os.listdir(root / split)
            for s in split_scenes
        ):
            raise FileNotFoundError(f"Provided root folder does not contain any scenes from the {split} split.")

        calibrartion_suffixes = {
            None: [""],
            "perfect": ["-perfect"],
            "imperfect": ["-imperfect"],
            "both": ["-perfect", "-imperfect"],
        }[calibration]

        for calibration_suffix in calibrartion_suffixes:
            scene_pattern = "*" + calibration_suffix
            left_img_pattern = str(root / split / scene_pattern / "im0.png")
            right_img_pattern = str(root / split / scene_pattern / "im1.png")
            self._images += self._scan_pairs(left_img_pattern, right_img_pattern)

            if split == "test":
                self._disparities = list((None, None) for _ in self._images)
            else:
                left_dispartity_pattern = str(root / split / scene_pattern / "disp0.pfm")
                right_dispartity_pattern = str(root / split / scene_pattern / "disp1.pfm")
                self._disparities += self._scan_pairs(left_dispartity_pattern, right_dispartity_pattern)

        self.use_ambient_views = use_ambient_views

    def _read_img(self, file_path: Union[str, Path]) -> Image.Image:
        """
        Function that reads either the original right image or an augmented view when ``use_ambient_views`` is True.
        When ``use_ambient_views`` is True, the dataset will return at random one of ``[im1.png, im1E.png, im1L.png]``
        as the right image.
        """
        ambient_file_paths: List[Union[str, Path]]  # make mypy happy

        if not isinstance(file_path, Path):
            file_path = Path(file_path)

        if file_path.name == "im1.png" and self.use_ambient_views:
            base_path = file_path.parent
            # initialize sampleable container
            ambient_file_paths = list(base_path / view_name for view_name in ["im1E.png", "im1L.png"])
            # double check that we're not going to try to read from an invalid file path
            ambient_file_paths = list(filter(lambda p: os.path.exists(p), ambient_file_paths))
            # keep the original image as an option as well for uniform sampling between base views
            ambient_file_paths.append(file_path)
            file_path = random.choice(ambient_file_paths)  # type: ignore
        return super()._read_img(file_path)

    def _read_disparity(self, file_path: str) -> Union[Tuple[None, None], Tuple[np.ndarray, np.ndarray]]:
        # test split has not disparity maps
        if file_path is None:
            return None, None

        disparity_map = _read_pfm_file(file_path)
        disparity_map = np.abs(disparity_map)  # ensure that the disparity is positive
        disparity_map[disparity_map == np.inf] = 0  # remove infinite disparities
        valid_mask = (disparity_map > 0).squeeze(0)  # mask out invalid disparities
        return disparity_map, valid_mask

    def _download_dataset(self, root: Union[str, Path]) -> None:
        base_url = "https://vision.middlebury.edu/stereo/data/scenes2014/zip"
        # train and additional splits have 2 different calibration settings
        root = Path(root) / "Middlebury2014"
        split_name = self.split

        if split_name != "test":
            for split_scene in self.splits[split_name]:
                split_root = root / split_name
                for calibration in ["perfect", "imperfect"]:
                    scene_name = f"{split_scene}-{calibration}"
                    scene_url = f"{base_url}/{scene_name}.zip"
                    print(f"Downloading {scene_url}")
                    # download the scene only if it doesn't exist
                    if not (split_root / scene_name).exists():
                        download_and_extract_archive(
                            url=scene_url,
                            filename=f"{scene_name}.zip",
                            download_root=str(split_root),
                            remove_finished=True,
                        )
        else:
            os.makedirs(root / "test")
            if any(s not in os.listdir(root / "test") for s in self.splits["test"]):
                # test split is downloaded from a different location
                test_set_url = "https://vision.middlebury.edu/stereo/submit3/zip/MiddEval3-data-F.zip"
                # the unzip is going to produce a directory MiddEval3 with two subdirectories trainingF and testF
                # we want to move the contents from testF into the  directory
                download_and_extract_archive(url=test_set_url, download_root=str(root), remove_finished=True)
                for scene_dir, scene_names, _ in os.walk(str(root / "MiddEval3/testF")):
                    for scene in scene_names:
                        scene_dst_dir = root / "test"
                        scene_src_dir = Path(scene_dir) / scene
                        os.makedirs(scene_dst_dir, exist_ok=True)
                        shutil.move(str(scene_src_dir), str(scene_dst_dir))

                # cleanup MiddEval3 directory
                shutil.rmtree(str(root / "MiddEval3"))

    def __getitem__(self, index: int) -> T2:
        """Return example at given index.

        Args:
            index(int): The index of the example to retrieve

        Returns:
            tuple: A 4-tuple with ``(img_left, img_right, disparity, valid_mask)``.
            The disparity is a numpy array of shape (1, H, W) and the images are PIL images.
            ``valid_mask`` is implicitly ``None`` for `split=test`.
        """
        return cast(T2, super().__getitem__(index))


class CREStereo(StereoMatchingDataset):
    """Synthetic dataset used in training the `CREStereo <https://arxiv.org/pdf/2203.11483.pdf>`_ architecture.
    Dataset details on the official paper `repo <https://github.com/megvii-research/CREStereo>`_.

    The dataset is expected to have the following structure: ::

        root
            CREStereo
                tree
                    img1_left.jpg
                    img1_right.jpg
                    img1_left.disp.jpg
                    img1_right.disp.jpg
                    img2_left.jpg
                    img2_right.jpg
                    img2_left.disp.jpg
                    img2_right.disp.jpg
                    ...
                shapenet
                    img1_left.jpg
                    img1_right.jpg
                    img1_left.disp.jpg
                    img1_right.disp.jpg
                    ...
                reflective
                    img1_left.jpg
                    img1_right.jpg
                    img1_left.disp.jpg
                    img1_right.disp.jpg
                    ...
                hole
                    img1_left.jpg
                    img1_right.jpg
                    img1_left.disp.jpg
                    img1_right.disp.jpg
                    ...

    Args:
        root (str): Root directory of the dataset.
        transforms (callable, optional): A function/transform that takes in a sample and returns a transformed version.
    """

    _has_built_in_disparity_mask = True

    def __init__(
        self,
        root: Union[str, Path],
        transforms: Optional[Callable] = None,
    ) -> None:
        super().__init__(root, transforms)

        root = Path(root) / "CREStereo"

        dirs = ["shapenet", "reflective", "tree", "hole"]

        for s in dirs:
            left_image_pattern = str(root / s / "*_left.jpg")
            right_image_pattern = str(root / s / "*_right.jpg")
            imgs = self._scan_pairs(left_image_pattern, right_image_pattern)
            self._images += imgs

            left_disparity_pattern = str(root / s / "*_left.disp.png")
            right_disparity_pattern = str(root / s / "*_right.disp.png")
            disparities = self._scan_pairs(left_disparity_pattern, right_disparity_pattern)
            self._disparities += disparities

    def _read_disparity(self, file_path: str) -> Tuple[np.ndarray, None]:
        disparity_map = np.asarray(Image.open(file_path), dtype=np.float32)
        # unsqueeze the disparity map into (C, H, W) format
        disparity_map = disparity_map[None, :, :] / 32.0
        valid_mask = None
        return disparity_map, valid_mask

    def __getitem__(self, index: int) -> T1:
        """Return example at given index.

        Args:
            index(int): The index of the example to retrieve

        Returns:
            tuple: A 4-tuple with ``(img_left, img_right, disparity, valid_mask)``.
            The disparity is a numpy array of shape (1, H, W) and the images are PIL images.
            ``valid_mask`` is implicitly ``None`` if the ``transforms`` parameter does not
            generate a valid mask.
        """
        return cast(T1, super().__getitem__(index))


class FallingThingsStereo(StereoMatchingDataset):
    """`FallingThings <https://research.nvidia.com/publication/2018-06_falling-things-synthetic-dataset-3d-object-detection-and-pose-estimation>`_ dataset.

    The dataset is expected to have the following structure: ::

        root
            FallingThings
                single
                    dir1
                        scene1
                            _object_settings.json
                            _camera_settings.json
                            image1.left.depth.png
                            image1.right.depth.png
                            image1.left.jpg
                            image1.right.jpg
                            image2.left.depth.png
                            image2.right.depth.png
                            image2.left.jpg
                            image2.right
                            ...
                        scene2
                    ...
                mixed
                    scene1
                        _object_settings.json
                        _camera_settings.json
                        image1.left.depth.png
                        image1.right.depth.png
                        image1.left.jpg
                        image1.right.jpg
                        image2.left.depth.png
                        image2.right.depth.png
                        image2.left.jpg
                        image2.right
                        ...
                    scene2
                    ...

    Args:
        root (str or ``pathlib.Path``): Root directory where FallingThings is located.
        variant (string): Which variant to use. Either "single", "mixed", or "both".
        transforms (callable, optional): A function/transform that takes in a sample and returns a transformed version.
    """

    def __init__(self, root: Union[str, Path], variant: str = "single", transforms: Optional[Callable] = None) -> None:
        super().__init__(root, transforms)

        root = Path(root) / "FallingThings"

        verify_str_arg(variant, "variant", valid_values=("single", "mixed", "both"))

        variants = {
            "single": ["single"],
            "mixed": ["mixed"],
            "both": ["single", "mixed"],
        }[variant]

        split_prefix = {
            "single": Path("*") / "*",
            "mixed": Path("*"),
        }

        for s in variants:
            left_img_pattern = str(root / s / split_prefix[s] / "*.left.jpg")
            right_img_pattern = str(root / s / split_prefix[s] / "*.right.jpg")
            self._images += self._scan_pairs(left_img_pattern, right_img_pattern)

            left_disparity_pattern = str(root / s / split_prefix[s] / "*.left.depth.png")
            right_disparity_pattern = str(root / s / split_prefix[s] / "*.right.depth.png")
            self._disparities += self._scan_pairs(left_disparity_pattern, right_disparity_pattern)

    def _read_disparity(self, file_path: str) -> Tuple[np.ndarray, None]:
        # (H, W) image
        depth = np.asarray(Image.open(file_path))
        # as per https://research.nvidia.com/sites/default/files/pubs/2018-06_Falling-Things/readme_0.txt
        # in order to extract disparity from depth maps
        camera_settings_path = Path(file_path).parent / "_camera_settings.json"
        with open(camera_settings_path, "r") as f:
            # inverse of depth-from-disparity equation: depth = (baseline * focal) / (disparity * pixel_constant)
            intrinsics = json.load(f)
            focal = intrinsics["camera_settings"][0]["intrinsic_settings"]["fx"]
            baseline, pixel_constant = 6, 100  # pixel constant is inverted
            disparity_map = (baseline * focal * pixel_constant) / depth.astype(np.float32)
            # unsqueeze disparity to (C, H, W)
            disparity_map = disparity_map[None, :, :]
            valid_mask = None
            return disparity_map, valid_mask

    def __getitem__(self, index: int) -> T1:
        """Return example at given index.

        Args:
            index(int): The index of the example to retrieve

        Returns:
            tuple: A 3-tuple with ``(img_left, img_right, disparity)``.
            The disparity is a numpy array of shape (1, H, W) and the images are PIL images.
            If a ``valid_mask`` is generated within the ``transforms`` parameter,
            a 4-tuple with ``(img_left, img_right, disparity, valid_mask)`` is returned.
        """
        return cast(T1, super().__getitem__(index))


class SceneFlowStereo(StereoMatchingDataset):
    """Dataset interface for `Scene Flow <https://lmb.informatik.uni-freiburg.de/resources/datasets/SceneFlowDatasets.en.html>`_ datasets.
    This interface provides access to the `FlyingThings3D, `Monkaa` and `Driving` datasets.

    The dataset is expected to have the following structure: ::

        root
            SceneFlow
                Monkaa
                    frames_cleanpass
                        scene1
                            left
                                img1.png
                                img2.png
                            right
                                img1.png
                                img2.png
                        scene2
                            left
                                img1.png
                                img2.png
                            right
                                img1.png
                                img2.png
                    frames_finalpass
                        scene1
                            left
                                img1.png
                                img2.png
                            right
                                img1.png
                                img2.png
                        ...
                        ...
                    disparity
                        scene1
                            left
                                img1.pfm
                                img2.pfm
                            right
                                img1.pfm
                                img2.pfm
                FlyingThings3D
                    ...
                    ...

    Args:
        root (str or ``pathlib.Path``): Root directory where SceneFlow is located.
        variant (string): Which dataset variant to user, "FlyingThings3D" (default), "Monkaa" or "Driving".
        pass_name (string): Which pass to use, "clean" (default), "final" or "both".
        transforms (callable, optional): A function/transform that takes in a sample and returns a transformed version.

    """

    def __init__(
        self,
        root: Union[str, Path],
        variant: str = "FlyingThings3D",
        pass_name: str = "clean",
        transforms: Optional[Callable] = None,
    ) -> None:
        super().__init__(root, transforms)

        root = Path(root) / "SceneFlow"

        verify_str_arg(variant, "variant", valid_values=("FlyingThings3D", "Driving", "Monkaa"))
        verify_str_arg(pass_name, "pass_name", valid_values=("clean", "final", "both"))

        passes = {
            "clean": ["frames_cleanpass"],
            "final": ["frames_finalpass"],
            "both": ["frames_cleanpass", "frames_finalpass"],
        }[pass_name]

        root = root / variant

        prefix_directories = {
            "Monkaa": Path("*"),
            "FlyingThings3D": Path("*") / "*" / "*",
            "Driving": Path("*") / "*" / "*",
        }

        for p in passes:
            left_image_pattern = str(root / p / prefix_directories[variant] / "left" / "*.png")
            right_image_pattern = str(root / p / prefix_directories[variant] / "right" / "*.png")
            self._images += self._scan_pairs(left_image_pattern, right_image_pattern)

            left_disparity_pattern = str(root / "disparity" / prefix_directories[variant] / "left" / "*.pfm")
            right_disparity_pattern = str(root / "disparity" / prefix_directories[variant] / "right" / "*.pfm")
            self._disparities += self._scan_pairs(left_disparity_pattern, right_disparity_pattern)

    def _read_disparity(self, file_path: str) -> Tuple[np.ndarray, None]:
        disparity_map = _read_pfm_file(file_path)
        disparity_map = np.abs(disparity_map)  # ensure that the disparity is positive
        valid_mask = None
        return disparity_map, valid_mask

    def __getitem__(self, index: int) -> T1:
        """Return example at given index.

        Args:
            index(int): The index of the example to retrieve

        Returns:
            tuple: A 3-tuple with ``(img_left, img_right, disparity)``.
            The disparity is a numpy array of shape (1, H, W) and the images are PIL images.
            If a ``valid_mask`` is generated within the ``transforms`` parameter,
            a 4-tuple with ``(img_left, img_right, disparity, valid_mask)`` is returned.
        """
        return cast(T1, super().__getitem__(index))


class SintelStereo(StereoMatchingDataset):
    """Sintel `Stereo Dataset <http://sintel.is.tue.mpg.de/stereo>`_.

    The dataset is expected to have the following structure: ::

        root
            Sintel
                training
                    final_left
                        scene1
                            img1.png
                            img2.png
                            ...
                        ...
                    final_right
                        scene2
                            img1.png
                            img2.png
                            ...
                        ...
                    disparities
                        scene1
                            img1.png
                            img2.png
                            ...
                        ...
                    occlusions
                        scene1
                            img1.png
                            img2.png
                            ...
                        ...
                    outofframe
                        scene1
                            img1.png
                            img2.png
                            ...
                        ...

    Args:
        root (str or ``pathlib.Path``): Root directory where Sintel Stereo is located.
        pass_name (string): The name of the pass to use, either "final", "clean" or "both".
        transforms (callable, optional): A function/transform that takes in a sample and returns a transformed version.
    """

    _has_built_in_disparity_mask = True

    def __init__(self, root: Union[str, Path], pass_name: str = "final", transforms: Optional[Callable] = None) -> None:
        super().__init__(root, transforms)

        verify_str_arg(pass_name, "pass_name", valid_values=("final", "clean", "both"))

        root = Path(root) / "Sintel"
        pass_names = {
            "final": ["final"],
            "clean": ["clean"],
            "both": ["final", "clean"],
        }[pass_name]

        for p in pass_names:
            left_img_pattern = str(root / "training" / f"{p}_left" / "*" / "*.png")
            right_img_pattern = str(root / "training" / f"{p}_right" / "*" / "*.png")
            self._images += self._scan_pairs(left_img_pattern, right_img_pattern)

            disparity_pattern = str(root / "training" / "disparities" / "*" / "*.png")
            self._disparities += self._scan_pairs(disparity_pattern, None)

    def _get_occlussion_mask_paths(self, file_path: str) -> Tuple[str, str]:
        # helper function to get the occlusion mask paths
        # a path will look like  .../.../.../training/disparities/scene1/img1.png
        # we want to get something like .../.../.../training/occlusions/scene1/img1.png
        fpath = Path(file_path)
        basename = fpath.name
        scenedir = fpath.parent
        # the parent of the scenedir is actually the disparity dir
        sampledir = scenedir.parent.parent

        occlusion_path = str(sampledir / "occlusions" / scenedir.name / basename)
        outofframe_path = str(sampledir / "outofframe" / scenedir.name / basename)

        if not os.path.exists(occlusion_path):
            raise FileNotFoundError(f"Occlusion mask {occlusion_path} does not exist")

        if not os.path.exists(outofframe_path):
            raise FileNotFoundError(f"Out of frame mask {outofframe_path} does not exist")

        return occlusion_path, outofframe_path

    def _read_disparity(self, file_path: str) -> Union[Tuple[None, None], Tuple[np.ndarray, np.ndarray]]:
        if file_path is None:
            return None, None

        # disparity decoding as per Sintel instructions in the README provided with the dataset
        disparity_map = np.asarray(Image.open(file_path), dtype=np.float32)
        r, g, b = np.split(disparity_map, 3, axis=-1)
        disparity_map = r * 4 + g / (2**6) + b / (2**14)
        # reshape into (C, H, W) format
        disparity_map = np.transpose(disparity_map, (2, 0, 1))
        # find the appropriate file paths
        occlued_mask_path, out_of_frame_mask_path = self._get_occlussion_mask_paths(file_path)
        # occlusion masks
        valid_mask = np.asarray(Image.open(occlued_mask_path)) == 0
        # out of frame masks
        off_mask = np.asarray(Image.open(out_of_frame_mask_path)) == 0
        # combine the masks together
        valid_mask = np.logical_and(off_mask, valid_mask)
        return disparity_map, valid_mask

    def __getitem__(self, index: int) -> T2:
        """Return example at given index.

        Args:
            index(int): The index of the example to retrieve

        Returns:
            tuple: A 4-tuple with ``(img_left, img_right, disparity, valid_mask)`` is returned.
            The disparity is a numpy array of shape (1, H, W) and the images are PIL images whilst
            the valid_mask is a numpy array of shape (H, W).
        """
        return cast(T2, super().__getitem__(index))


class InStereo2k(StereoMatchingDataset):
    """`InStereo2k <https://github.com/YuhuaXu/StereoDataset>`_ dataset.

    The dataset is expected to have the following structure: ::

        root
            InStereo2k
                train
                    scene1
                        left.png
                        right.png
                        left_disp.png
                        right_disp.png
                        ...
                    scene2
                    ...
                test
                    scene1
                        left.png
                        right.png
                        left_disp.png
                        right_disp.png
                        ...
                    scene2
                    ...

    Args:
        root (str or ``pathlib.Path``): Root directory where InStereo2k is located.
        split (string): Either "train" or "test".
        transforms (callable, optional): A function/transform that takes in a sample and returns a transformed version.
    """

    def __init__(self, root: Union[str, Path], split: str = "train", transforms: Optional[Callable] = None) -> None:
        super().__init__(root, transforms)

        root = Path(root) / "InStereo2k" / split

        verify_str_arg(split, "split", valid_values=("train", "test"))

        left_img_pattern = str(root / "*" / "left.png")
        right_img_pattern = str(root / "*" / "right.png")
        self._images = self._scan_pairs(left_img_pattern, right_img_pattern)

        left_disparity_pattern = str(root / "*" / "left_disp.png")
        right_disparity_pattern = str(root / "*" / "right_disp.png")
        self._disparities = self._scan_pairs(left_disparity_pattern, right_disparity_pattern)

    def _read_disparity(self, file_path: str) -> Tuple[np.ndarray, None]:
        disparity_map = np.asarray(Image.open(file_path), dtype=np.float32)
        # unsqueeze disparity to (C, H, W)
        disparity_map = disparity_map[None, :, :] / 1024.0
        valid_mask = None
        return disparity_map, valid_mask

    def __getitem__(self, index: int) -> T1:
        """Return example at given index.

        Args:
            index(int): The index of the example to retrieve

        Returns:
            tuple: A 3-tuple with ``(img_left, img_right, disparity)``.
            The disparity is a numpy array of shape (1, H, W) and the images are PIL images.
            If a ``valid_mask`` is generated within the ``transforms`` parameter,
            a 4-tuple with ``(img_left, img_right, disparity, valid_mask)`` is returned.
        """
        return cast(T1, super().__getitem__(index))


class ETH3DStereo(StereoMatchingDataset):
    """ETH3D `Low-Res Two-View <https://www.eth3d.net/datasets>`_ dataset.

    The dataset is expected to have the following structure: ::

        root
            ETH3D
                two_view_training
                    scene1
                        im1.png
                        im0.png
                        images.txt
                        cameras.txt
                        calib.txt
                    scene2
                        im1.png
                        im0.png
                        images.txt
                        cameras.txt
                        calib.txt
                    ...
                two_view_training_gt
                    scene1
                        disp0GT.pfm
                        mask0nocc.png
                    scene2
                        disp0GT.pfm
                        mask0nocc.png
                    ...
                two_view_testing
                    scene1
                        im1.png
                        im0.png
                        images.txt
                        cameras.txt
                        calib.txt
                    scene2
                        im1.png
                        im0.png
                        images.txt
                        cameras.txt
                        calib.txt
                    ...

    Args:
        root (str or ``pathlib.Path``): Root directory of the ETH3D Dataset.
        split (string, optional): The dataset split of scenes, either "train" (default) or "test".
        transforms (callable, optional): A function/transform that takes in a sample and returns a transformed version.
    """

    _has_built_in_disparity_mask = True

    def __init__(self, root: Union[str, Path], split: str = "train", transforms: Optional[Callable] = None) -> None:
        super().__init__(root, transforms)

        verify_str_arg(split, "split", valid_values=("train", "test"))

        root = Path(root) / "ETH3D"

        img_dir = "two_view_training" if split == "train" else "two_view_test"
        anot_dir = "two_view_training_gt"

        left_img_pattern = str(root / img_dir / "*" / "im0.png")
        right_img_pattern = str(root / img_dir / "*" / "im1.png")
        self._images = self._scan_pairs(left_img_pattern, right_img_pattern)

        if split == "test":
            self._disparities = list((None, None) for _ in self._images)
        else:
            disparity_pattern = str(root / anot_dir / "*" / "disp0GT.pfm")
            self._disparities = self._scan_pairs(disparity_pattern, None)

    def _read_disparity(self, file_path: str) -> Union[Tuple[None, None], Tuple[np.ndarray, np.ndarray]]:
        # test split has no disparity maps
        if file_path is None:
            return None, None

        disparity_map = _read_pfm_file(file_path)
        disparity_map = np.abs(disparity_map)  # ensure that the disparity is positive
        mask_path = Path(file_path).parent / "mask0nocc.png"
        valid_mask = Image.open(mask_path)
        valid_mask = np.asarray(valid_mask).astype(bool)
        return disparity_map, valid_mask

    def __getitem__(self, index: int) -> T2:
        """Return example at given index.

        Args:
            index(int): The index of the example to retrieve

        Returns:
            tuple: A 4-tuple with ``(img_left, img_right, disparity, valid_mask)``.
            The disparity is a numpy array of shape (1, H, W) and the images are PIL images.
            ``valid_mask`` is implicitly ``None`` if the ``transforms`` parameter does not
            generate a valid mask.
            Both ``disparity`` and ``valid_mask`` are ``None`` if the dataset split is test.
        """
        return cast(T2, super().__getitem__(index))
