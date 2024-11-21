import bisect
import math
import warnings
from fractions import Fraction
from typing import Any, Callable, cast, Dict, List, Optional, Tuple, TypeVar, Union

import torch
from torchvision.io import _probe_video_from_file, _read_video_from_file, read_video, read_video_timestamps

from .utils import tqdm

T = TypeVar("T")


def pts_convert(pts: int, timebase_from: Fraction, timebase_to: Fraction, round_func: Callable = math.floor) -> int:
    """convert pts between different time bases
    Args:
        pts: presentation timestamp, float
        timebase_from: original timebase. Fraction
        timebase_to: new timebase. Fraction
        round_func: rounding function.
    """
    new_pts = Fraction(pts, 1) * timebase_from / timebase_to
    return round_func(new_pts)


def unfold(tensor: torch.Tensor, size: int, step: int, dilation: int = 1) -> torch.Tensor:
    """
    similar to tensor.unfold, but with the dilation
    and specialized for 1d tensors

    Returns all consecutive windows of `size` elements, with
    `step` between windows. The distance between each element
    in a window is given by `dilation`.
    """
    if tensor.dim() != 1:
        raise ValueError(f"tensor should have 1 dimension instead of {tensor.dim()}")
    o_stride = tensor.stride(0)
    numel = tensor.numel()
    new_stride = (step * o_stride, dilation * o_stride)
    new_size = ((numel - (dilation * (size - 1) + 1)) // step + 1, size)
    if new_size[0] < 1:
        new_size = (0, size)
    return torch.as_strided(tensor, new_size, new_stride)


class _VideoTimestampsDataset:
    """
    Dataset used to parallelize the reading of the timestamps
    of a list of videos, given their paths in the filesystem.

    Used in VideoClips and defined at top level, so it can be
    pickled when forking.
    """

    def __init__(self, video_paths: List[str]) -> None:
        self.video_paths = video_paths

    def __len__(self) -> int:
        return len(self.video_paths)

    def __getitem__(self, idx: int) -> Tuple[List[int], Optional[float]]:
        return read_video_timestamps(self.video_paths[idx])


def _collate_fn(x: T) -> T:
    """
    Dummy collate function to be used with _VideoTimestampsDataset
    """
    return x


class VideoClips:
    """
    Given a list of video files, computes all consecutive subvideos of size
    `clip_length_in_frames`, where the distance between each subvideo in the
    same video is defined by `frames_between_clips`.
    If `frame_rate` is specified, it will also resample all the videos to have
    the same frame rate, and the clips will refer to this frame rate.

    Creating this instance the first time is time-consuming, as it needs to
    decode all the videos in `video_paths`. It is recommended that you
    cache the results after instantiation of the class.

    Recreating the clips for different clip lengths is fast, and can be done
    with the `compute_clips` method.

    Args:
        video_paths (List[str]): paths to the video files
        clip_length_in_frames (int): size of a clip in number of frames
        frames_between_clips (int): step (in frames) between each clip
        frame_rate (float, optional): if specified, it will resample the video
            so that it has `frame_rate`, and then the clips will be defined
            on the resampled video
        num_workers (int): how many subprocesses to use for data loading.
            0 means that the data will be loaded in the main process. (default: 0)
        output_format (str): The format of the output video tensors. Can be either "THWC" (default) or "TCHW".
    """

    def __init__(
        self,
        video_paths: List[str],
        clip_length_in_frames: int = 16,
        frames_between_clips: int = 1,
        frame_rate: Optional[float] = None,
        _precomputed_metadata: Optional[Dict[str, Any]] = None,
        num_workers: int = 0,
        _video_width: int = 0,
        _video_height: int = 0,
        _video_min_dimension: int = 0,
        _video_max_dimension: int = 0,
        _audio_samples: int = 0,
        _audio_channels: int = 0,
        output_format: str = "THWC",
    ) -> None:

        self.video_paths = video_paths
        self.num_workers = num_workers

        # these options are not valid for pyav backend
        self._video_width = _video_width
        self._video_height = _video_height
        self._video_min_dimension = _video_min_dimension
        self._video_max_dimension = _video_max_dimension
        self._audio_samples = _audio_samples
        self._audio_channels = _audio_channels
        self.output_format = output_format.upper()
        if self.output_format not in ("THWC", "TCHW"):
            raise ValueError(f"output_format should be either 'THWC' or 'TCHW', got {output_format}.")

        if _precomputed_metadata is None:
            self._compute_frame_pts()
        else:
            self._init_from_metadata(_precomputed_metadata)
        self.compute_clips(clip_length_in_frames, frames_between_clips, frame_rate)

    def _compute_frame_pts(self) -> None:
        self.video_pts = []  # len = num_videos. Each entry is a tensor of shape (num_frames_in_video,)
        self.video_fps: List[float] = []  # len = num_videos

        # strategy: use a DataLoader to parallelize read_video_timestamps
        # so need to create a dummy dataset first
        import torch.utils.data

        dl: torch.utils.data.DataLoader = torch.utils.data.DataLoader(
            _VideoTimestampsDataset(self.video_paths),  # type: ignore[arg-type]
            batch_size=16,
            num_workers=self.num_workers,
            collate_fn=_collate_fn,
        )

        with tqdm(total=len(dl)) as pbar:
            for batch in dl:
                pbar.update(1)
                batch_pts, batch_fps = list(zip(*batch))
                # we need to specify dtype=torch.long because for empty list,
                # torch.as_tensor will use torch.float as default dtype. This
                # happens when decoding fails and no pts is returned in the list.
                batch_pts = [torch.as_tensor(pts, dtype=torch.long) for pts in batch_pts]
                self.video_pts.extend(batch_pts)
                self.video_fps.extend(batch_fps)

    def _init_from_metadata(self, metadata: Dict[str, Any]) -> None:
        self.video_paths = metadata["video_paths"]
        assert len(self.video_paths) == len(metadata["video_pts"])
        self.video_pts = metadata["video_pts"]
        assert len(self.video_paths) == len(metadata["video_fps"])
        self.video_fps = metadata["video_fps"]

    @property
    def metadata(self) -> Dict[str, Any]:
        _metadata = {
            "video_paths": self.video_paths,
            "video_pts": self.video_pts,
            "video_fps": self.video_fps,
        }
        return _metadata

    def subset(self, indices: List[int]) -> "VideoClips":
        video_paths = [self.video_paths[i] for i in indices]
        video_pts = [self.video_pts[i] for i in indices]
        video_fps = [self.video_fps[i] for i in indices]
        metadata = {
            "video_paths": video_paths,
            "video_pts": video_pts,
            "video_fps": video_fps,
        }
        return type(self)(
            video_paths,
            clip_length_in_frames=self.num_frames,
            frames_between_clips=self.step,
            frame_rate=self.frame_rate,
            _precomputed_metadata=metadata,
            num_workers=self.num_workers,
            _video_width=self._video_width,
            _video_height=self._video_height,
            _video_min_dimension=self._video_min_dimension,
            _video_max_dimension=self._video_max_dimension,
            _audio_samples=self._audio_samples,
            _audio_channels=self._audio_channels,
            output_format=self.output_format,
        )

    @staticmethod
    def compute_clips_for_video(
        video_pts: torch.Tensor, num_frames: int, step: int, fps: Optional[float], frame_rate: Optional[float] = None
    ) -> Tuple[torch.Tensor, Union[List[slice], torch.Tensor]]:
        if fps is None:
            # if for some reason the video doesn't have fps (because doesn't have a video stream)
            # set the fps to 1. The value doesn't matter, because video_pts is empty anyway
            fps = 1
        if frame_rate is None:
            frame_rate = fps
        total_frames = len(video_pts) * frame_rate / fps
        _idxs = VideoClips._resample_video_idx(int(math.floor(total_frames)), fps, frame_rate)
        video_pts = video_pts[_idxs]
        clips = unfold(video_pts, num_frames, step)
        if not clips.numel():
            warnings.warn(
                "There aren't enough frames in the current video to get a clip for the given clip length and "
                "frames between clips. The video (and potentially others) will be skipped."
            )
        idxs: Union[List[slice], torch.Tensor]
        if isinstance(_idxs, slice):
            idxs = [_idxs] * len(clips)
        else:
            idxs = unfold(_idxs, num_frames, step)
        return clips, idxs

    def compute_clips(self, num_frames: int, step: int, frame_rate: Optional[float] = None) -> None:
        """
        Compute all consecutive sequences of clips from video_pts.
        Always returns clips of size `num_frames`, meaning that the
        last few frames in a video can potentially be dropped.

        Args:
            num_frames (int): number of frames for the clip
            step (int): distance between two clips
            frame_rate (int, optional): The frame rate
        """
        self.num_frames = num_frames
        self.step = step
        self.frame_rate = frame_rate
        self.clips = []
        self.resampling_idxs = []
        for video_pts, fps in zip(self.video_pts, self.video_fps):
            clips, idxs = self.compute_clips_for_video(video_pts, num_frames, step, fps, frame_rate)
            self.clips.append(clips)
            self.resampling_idxs.append(idxs)
        clip_lengths = torch.as_tensor([len(v) for v in self.clips])
        self.cumulative_sizes = clip_lengths.cumsum(0).tolist()

    def __len__(self) -> int:
        return self.num_clips()

    def num_videos(self) -> int:
        return len(self.video_paths)

    def num_clips(self) -> int:
        """
        Number of subclips that are available in the video list.
        """
        return self.cumulative_sizes[-1]

    def get_clip_location(self, idx: int) -> Tuple[int, int]:
        """
        Converts a flattened representation of the indices into a video_idx, clip_idx
        representation.
        """
        video_idx = bisect.bisect_right(self.cumulative_sizes, idx)
        if video_idx == 0:
            clip_idx = idx
        else:
            clip_idx = idx - self.cumulative_sizes[video_idx - 1]
        return video_idx, clip_idx

    @staticmethod
    def _resample_video_idx(num_frames: int, original_fps: float, new_fps: float) -> Union[slice, torch.Tensor]:
        step = original_fps / new_fps
        if step.is_integer():
            # optimization: if step is integer, don't need to perform
            # advanced indexing
            step = int(step)
            return slice(None, None, step)
        idxs = torch.arange(num_frames, dtype=torch.float32) * step
        idxs = idxs.floor().to(torch.int64)
        return idxs

    def get_clip(self, idx: int) -> Tuple[torch.Tensor, torch.Tensor, Dict[str, Any], int]:
        """
        Gets a subclip from a list of videos.

        Args:
            idx (int): index of the subclip. Must be between 0 and num_clips().

        Returns:
            video (Tensor)
            audio (Tensor)
            info (Dict)
            video_idx (int): index of the video in `video_paths`
        """
        if idx >= self.num_clips():
            raise IndexError(f"Index {idx} out of range ({self.num_clips()} number of clips)")
        video_idx, clip_idx = self.get_clip_location(idx)
        video_path = self.video_paths[video_idx]
        clip_pts = self.clips[video_idx][clip_idx]

        from torchvision import get_video_backend

        backend = get_video_backend()

        if backend == "pyav":
            # check for invalid options
            if self._video_width != 0:
                raise ValueError("pyav backend doesn't support _video_width != 0")
            if self._video_height != 0:
                raise ValueError("pyav backend doesn't support _video_height != 0")
            if self._video_min_dimension != 0:
                raise ValueError("pyav backend doesn't support _video_min_dimension != 0")
            if self._video_max_dimension != 0:
                raise ValueError("pyav backend doesn't support _video_max_dimension != 0")
            if self._audio_samples != 0:
                raise ValueError("pyav backend doesn't support _audio_samples != 0")

        if backend == "pyav":
            start_pts = clip_pts[0].item()
            end_pts = clip_pts[-1].item()
            video, audio, info = read_video(video_path, start_pts, end_pts)
        else:
            _info = _probe_video_from_file(video_path)
            video_fps = _info.video_fps
            audio_fps = None

            video_start_pts = cast(int, clip_pts[0].item())
            video_end_pts = cast(int, clip_pts[-1].item())

            audio_start_pts, audio_end_pts = 0, -1
            audio_timebase = Fraction(0, 1)
            video_timebase = Fraction(_info.video_timebase.numerator, _info.video_timebase.denominator)
            if _info.has_audio:
                audio_timebase = Fraction(_info.audio_timebase.numerator, _info.audio_timebase.denominator)
                audio_start_pts = pts_convert(video_start_pts, video_timebase, audio_timebase, math.floor)
                audio_end_pts = pts_convert(video_end_pts, video_timebase, audio_timebase, math.ceil)
                audio_fps = _info.audio_sample_rate
            video, audio, _ = _read_video_from_file(
                video_path,
                video_width=self._video_width,
                video_height=self._video_height,
                video_min_dimension=self._video_min_dimension,
                video_max_dimension=self._video_max_dimension,
                video_pts_range=(video_start_pts, video_end_pts),
                video_timebase=video_timebase,
                audio_samples=self._audio_samples,
                audio_channels=self._audio_channels,
                audio_pts_range=(audio_start_pts, audio_end_pts),
                audio_timebase=audio_timebase,
            )

            info = {"video_fps": video_fps}
            if audio_fps is not None:
                info["audio_fps"] = audio_fps

        if self.frame_rate is not None:
            resampling_idx = self.resampling_idxs[video_idx][clip_idx]
            if isinstance(resampling_idx, torch.Tensor):
                resampling_idx = resampling_idx - resampling_idx[0]
            video = video[resampling_idx]
            info["video_fps"] = self.frame_rate
        assert len(video) == self.num_frames, f"{video.shape} x {self.num_frames}"

        if self.output_format == "TCHW":
            # [T,H,W,C] --> [T,C,H,W]
            video = video.permute(0, 3, 1, 2)

        return video, audio, info, video_idx

    def __getstate__(self) -> Dict[str, Any]:
        video_pts_sizes = [len(v) for v in self.video_pts]
        # To be back-compatible, we convert data to dtype torch.long as needed
        # because for empty list, in legacy implementation, torch.as_tensor will
        # use torch.float as default dtype. This happens when decoding fails and
        # no pts is returned in the list.
        video_pts = [x.to(torch.int64) for x in self.video_pts]
        # video_pts can be an empty list if no frames have been decoded
        if video_pts:
            video_pts = torch.cat(video_pts)  # type: ignore[assignment]
            # avoid bug in https://github.com/pytorch/pytorch/issues/32351
            # TODO: Revert it once the bug is fixed.
            video_pts = video_pts.numpy()  # type: ignore[attr-defined]

        # make a copy of the fields of self
        d = self.__dict__.copy()
        d["video_pts_sizes"] = video_pts_sizes
        d["video_pts"] = video_pts
        # delete the following attributes to reduce the size of dictionary. They
        # will be re-computed in "__setstate__()"
        del d["clips"]
        del d["resampling_idxs"]
        del d["cumulative_sizes"]

        # for backwards-compatibility
        d["_version"] = 2
        return d

    def __setstate__(self, d: Dict[str, Any]) -> None:
        # for backwards-compatibility
        if "_version" not in d:
            self.__dict__ = d
            return

        video_pts = torch.as_tensor(d["video_pts"], dtype=torch.int64)
        video_pts = torch.split(video_pts, d["video_pts_sizes"], dim=0)
        # don't need this info anymore
        del d["video_pts_sizes"]

        d["video_pts"] = video_pts
        self.__dict__ = d
        # recompute attributes "clips", "resampling_idxs" and other derivative ones
        self.compute_clips(self.num_frames, self.step, self.frame_rate)
