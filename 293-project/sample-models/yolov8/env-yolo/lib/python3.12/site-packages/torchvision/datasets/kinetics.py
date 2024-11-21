import csv
import os
import time
import urllib
from functools import partial
from multiprocessing import Pool
from os import path
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple, Union

from torch import Tensor

from .folder import find_classes, make_dataset
from .utils import check_integrity, download_and_extract_archive, download_url, verify_str_arg
from .video_utils import VideoClips
from .vision import VisionDataset


def _dl_wrap(tarpath: Union[str, Path], videopath: Union[str, Path], line: str) -> None:
    download_and_extract_archive(line, tarpath, videopath)


class Kinetics(VisionDataset):
    """`Generic Kinetics <https://www.deepmind.com/open-source/kinetics>`_
    dataset.

    Kinetics-400/600/700 are action recognition video datasets.
    This dataset consider every video as a collection of video clips of fixed size, specified
    by ``frames_per_clip``, where the step in frames between each clip is given by
    ``step_between_clips``.

    To give an example, for 2 videos with 10 and 15 frames respectively, if ``frames_per_clip=5``
    and ``step_between_clips=5``, the dataset size will be (2 + 3) = 5, where the first two
    elements will come from video 1, and the next three elements from video 2.
    Note that we drop clips which do not have exactly ``frames_per_clip`` elements, so not all
    frames in a video might be present.

    Args:
        root (str or ``pathlib.Path``): Root directory of the Kinetics Dataset.
            Directory should be structured as follows:
            .. code::

                root/
                ├── split
                │   ├──  class1
                │   │   ├──  vid1.mp4
                │   │   ├──  vid2.mp4
                │   │   ├──  vid3.mp4
                │   │   ├──  ...
                │   ├──  class2
                │   │   ├──   vidx.mp4
                │   │    └── ...

            Note: split is appended automatically using the split argument.
        frames_per_clip (int): number of frames in a clip
        num_classes (int): select between Kinetics-400 (default), Kinetics-600, and Kinetics-700
        split (str): split of the dataset to consider; supports ``"train"`` (default) ``"val"`` ``"test"``
        frame_rate (float): If omitted, interpolate different frame rate for each clip.
        step_between_clips (int): number of frames between each clip
        transform (callable, optional): A function/transform that takes in a TxHxWxC video
            and returns a transformed version.
        download (bool): Download the official version of the dataset to root folder.
        num_workers (int): Use multiple workers for VideoClips creation
        num_download_workers (int): Use multiprocessing in order to speed up download.
        output_format (str, optional): The format of the output video tensors (before transforms).
            Can be either "THWC" or "TCHW" (default).
            Note that in most other utils and datasets, the default is actually "THWC".

    Returns:
        tuple: A 3-tuple with the following entries:

            - video (Tensor[T, C, H, W] or Tensor[T, H, W, C]): the `T` video frames in torch.uint8 tensor
            - audio(Tensor[K, L]): the audio frames, where `K` is the number of channels
              and `L` is the number of points in torch.float tensor
            - label (int): class of the video clip

    Raises:
        RuntimeError: If ``download is True`` and the video archives are already extracted.
    """

    _TAR_URLS = {
        "400": "https://s3.amazonaws.com/kinetics/400/{split}/k400_{split}_path.txt",
        "600": "https://s3.amazonaws.com/kinetics/600/{split}/k600_{split}_path.txt",
        "700": "https://s3.amazonaws.com/kinetics/700_2020/{split}/k700_2020_{split}_path.txt",
    }
    _ANNOTATION_URLS = {
        "400": "https://s3.amazonaws.com/kinetics/400/annotations/{split}.csv",
        "600": "https://s3.amazonaws.com/kinetics/600/annotations/{split}.csv",
        "700": "https://s3.amazonaws.com/kinetics/700_2020/annotations/{split}.csv",
    }

    def __init__(
        self,
        root: Union[str, Path],
        frames_per_clip: int,
        num_classes: str = "400",
        split: str = "train",
        frame_rate: Optional[int] = None,
        step_between_clips: int = 1,
        transform: Optional[Callable] = None,
        extensions: Tuple[str, ...] = ("avi", "mp4"),
        download: bool = False,
        num_download_workers: int = 1,
        num_workers: int = 1,
        _precomputed_metadata: Optional[Dict[str, Any]] = None,
        _video_width: int = 0,
        _video_height: int = 0,
        _video_min_dimension: int = 0,
        _audio_samples: int = 0,
        _audio_channels: int = 0,
        _legacy: bool = False,
        output_format: str = "TCHW",
    ) -> None:

        # TODO: support test
        self.num_classes = verify_str_arg(num_classes, arg="num_classes", valid_values=["400", "600", "700"])
        self.extensions = extensions
        self.num_download_workers = num_download_workers

        self.root = root
        self._legacy = _legacy

        if _legacy:
            print("Using legacy structure")
            self.split_folder = root
            self.split = "unknown"
            output_format = "THWC"
            if download:
                raise ValueError("Cannot download the videos using legacy_structure.")
        else:
            self.split_folder = path.join(root, split)
            self.split = verify_str_arg(split, arg="split", valid_values=["train", "val", "test"])

        if download:
            self.download_and_process_videos()

        super().__init__(self.root)

        self.classes, class_to_idx = find_classes(self.split_folder)
        self.samples = make_dataset(self.split_folder, class_to_idx, extensions, is_valid_file=None)
        video_list = [x[0] for x in self.samples]
        self.video_clips = VideoClips(
            video_list,
            frames_per_clip,
            step_between_clips,
            frame_rate,
            _precomputed_metadata,
            num_workers=num_workers,
            _video_width=_video_width,
            _video_height=_video_height,
            _video_min_dimension=_video_min_dimension,
            _audio_samples=_audio_samples,
            _audio_channels=_audio_channels,
            output_format=output_format,
        )
        self.transform = transform

    def download_and_process_videos(self) -> None:
        """Downloads all the videos to the _root_ folder in the expected format."""
        tic = time.time()
        self._download_videos()
        toc = time.time()
        print("Elapsed time for downloading in mins ", (toc - tic) / 60)
        self._make_ds_structure()
        toc2 = time.time()
        print("Elapsed time for processing in mins ", (toc2 - toc) / 60)
        print("Elapsed time overall in mins ", (toc2 - tic) / 60)

    def _download_videos(self) -> None:
        """download tarballs containing the video to "tars" folder and extract them into the _split_ folder where
        split is one of the official dataset splits.

        Raises:
            RuntimeError: if download folder exists, break to prevent downloading entire dataset again.
        """
        if path.exists(self.split_folder):
            raise RuntimeError(
                f"The directory {self.split_folder} already exists. "
                f"If you want to re-download or re-extract the images, delete the directory."
            )
        tar_path = path.join(self.root, "tars")
        file_list_path = path.join(self.root, "files")

        split_url = self._TAR_URLS[self.num_classes].format(split=self.split)
        split_url_filepath = path.join(file_list_path, path.basename(split_url))
        if not check_integrity(split_url_filepath):
            download_url(split_url, file_list_path)
        with open(split_url_filepath) as file:
            list_video_urls = [urllib.parse.quote(line, safe="/,:") for line in file.read().splitlines()]

        if self.num_download_workers == 1:
            for line in list_video_urls:
                download_and_extract_archive(line, tar_path, self.split_folder)
        else:
            part = partial(_dl_wrap, tar_path, self.split_folder)
            poolproc = Pool(self.num_download_workers)
            poolproc.map(part, list_video_urls)

    def _make_ds_structure(self) -> None:
        """move videos from
        split_folder/
            ├── clip1.avi
            ├── clip2.avi

        to the correct format as described below:
        split_folder/
            ├── class1
            │   ├── clip1.avi

        """
        annotation_path = path.join(self.root, "annotations")
        if not check_integrity(path.join(annotation_path, f"{self.split}.csv")):
            download_url(self._ANNOTATION_URLS[self.num_classes].format(split=self.split), annotation_path)
        annotations = path.join(annotation_path, f"{self.split}.csv")

        file_fmtstr = "{ytid}_{start:06}_{end:06}.mp4"
        with open(annotations) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                f = file_fmtstr.format(
                    ytid=row["youtube_id"],
                    start=int(row["time_start"]),
                    end=int(row["time_end"]),
                )
                label = row["label"].replace(" ", "_").replace("'", "").replace("(", "").replace(")", "")
                os.makedirs(path.join(self.split_folder, label), exist_ok=True)
                downloaded_file = path.join(self.split_folder, f)
                if path.isfile(downloaded_file):
                    os.replace(
                        downloaded_file,
                        path.join(self.split_folder, label, f),
                    )

    @property
    def metadata(self) -> Dict[str, Any]:
        return self.video_clips.metadata

    def __len__(self) -> int:
        return self.video_clips.num_clips()

    def __getitem__(self, idx: int) -> Tuple[Tensor, Tensor, int]:
        video, audio, info, video_idx = self.video_clips.get_clip(idx)
        label = self.samples[video_idx][1]

        if self.transform is not None:
            video = self.transform(video)

        return video, audio, label
