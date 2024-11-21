import math
from typing import cast, Iterator, List, Optional, Sized, Union

import torch
import torch.distributed as dist
from torch.utils.data import Sampler
from torchvision.datasets.video_utils import VideoClips


class DistributedSampler(Sampler):
    """
    Extension of DistributedSampler, as discussed in
    https://github.com/pytorch/pytorch/issues/23430

    Example:
        dataset: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
        num_replicas: 4
        shuffle: False

    when group_size = 1
            RANK    |  shard_dataset
            =========================
            rank_0  |  [0, 4, 8, 12]
            rank_1  |  [1, 5, 9, 13]
            rank_2  |  [2, 6, 10, 0]
            rank_3  |  [3, 7, 11, 1]

    when group_size = 2

            RANK    |  shard_dataset
            =========================
            rank_0  |  [0, 1, 8, 9]
            rank_1  |  [2, 3, 10, 11]
            rank_2  |  [4, 5, 12, 13]
            rank_3  |  [6, 7, 0, 1]

    """

    def __init__(
        self,
        dataset: Sized,
        num_replicas: Optional[int] = None,
        rank: Optional[int] = None,
        shuffle: bool = False,
        group_size: int = 1,
    ) -> None:
        if num_replicas is None:
            if not dist.is_available():
                raise RuntimeError("Requires distributed package to be available")
            num_replicas = dist.get_world_size()
        if rank is None:
            if not dist.is_available():
                raise RuntimeError("Requires distributed package to be available")
            rank = dist.get_rank()
        if len(dataset) % group_size != 0:
            raise ValueError(
                f"dataset length must be a multiplier of group size dataset length: {len(dataset)}, group size: {group_size}"
            )
        self.dataset = dataset
        self.group_size = group_size
        self.num_replicas = num_replicas
        self.rank = rank
        self.epoch = 0
        dataset_group_length = len(dataset) // group_size
        self.num_group_samples = int(math.ceil(dataset_group_length * 1.0 / self.num_replicas))
        self.num_samples = self.num_group_samples * group_size
        self.total_size = self.num_samples * self.num_replicas
        self.shuffle = shuffle

    def __iter__(self) -> Iterator[int]:
        # deterministically shuffle based on epoch
        g = torch.Generator()
        g.manual_seed(self.epoch)
        indices: Union[torch.Tensor, List[int]]
        if self.shuffle:
            indices = torch.randperm(len(self.dataset), generator=g).tolist()
        else:
            indices = list(range(len(self.dataset)))

        # add extra samples to make it evenly divisible
        indices += indices[: (self.total_size - len(indices))]
        assert len(indices) == self.total_size

        total_group_size = self.total_size // self.group_size
        indices = torch.reshape(torch.LongTensor(indices), (total_group_size, self.group_size))

        # subsample
        indices = indices[self.rank : total_group_size : self.num_replicas, :]
        indices = torch.reshape(indices, (-1,)).tolist()
        assert len(indices) == self.num_samples

        if isinstance(self.dataset, Sampler):
            orig_indices = list(iter(self.dataset))
            indices = [orig_indices[i] for i in indices]

        return iter(indices)

    def __len__(self) -> int:
        return self.num_samples

    def set_epoch(self, epoch: int) -> None:
        self.epoch = epoch


class UniformClipSampler(Sampler):
    """
    Sample `num_video_clips_per_video` clips for each video, equally spaced.
    When number of unique clips in the video is fewer than num_video_clips_per_video,
    repeat the clips until `num_video_clips_per_video` clips are collected

    Args:
        video_clips (VideoClips): video clips to sample from
        num_clips_per_video (int): number of clips to be sampled per video
    """

    def __init__(self, video_clips: VideoClips, num_clips_per_video: int) -> None:
        if not isinstance(video_clips, VideoClips):
            raise TypeError(f"Expected video_clips to be an instance of VideoClips, got {type(video_clips)}")
        self.video_clips = video_clips
        self.num_clips_per_video = num_clips_per_video

    def __iter__(self) -> Iterator[int]:
        idxs = []
        s = 0
        # select num_clips_per_video for each video, uniformly spaced
        for c in self.video_clips.clips:
            length = len(c)
            if length == 0:
                # corner case where video decoding fails
                continue

            sampled = torch.linspace(s, s + length - 1, steps=self.num_clips_per_video).floor().to(torch.int64)
            s += length
            idxs.append(sampled)
        return iter(cast(List[int], torch.cat(idxs).tolist()))

    def __len__(self) -> int:
        return sum(self.num_clips_per_video for c in self.video_clips.clips if len(c) > 0)


class RandomClipSampler(Sampler):
    """
    Samples at most `max_video_clips_per_video` clips for each video randomly

    Args:
        video_clips (VideoClips): video clips to sample from
        max_clips_per_video (int): maximum number of clips to be sampled per video
    """

    def __init__(self, video_clips: VideoClips, max_clips_per_video: int) -> None:
        if not isinstance(video_clips, VideoClips):
            raise TypeError(f"Expected video_clips to be an instance of VideoClips, got {type(video_clips)}")
        self.video_clips = video_clips
        self.max_clips_per_video = max_clips_per_video

    def __iter__(self) -> Iterator[int]:
        idxs = []
        s = 0
        # select at most max_clips_per_video for each video, randomly
        for c in self.video_clips.clips:
            length = len(c)
            size = min(length, self.max_clips_per_video)
            sampled = torch.randperm(length)[:size] + s
            s += length
            idxs.append(sampled)
        idxs_ = torch.cat(idxs)
        # shuffle all clips randomly
        perm = torch.randperm(len(idxs_))
        return iter(idxs_[perm].tolist())

    def __len__(self) -> int:
        return sum(min(len(c), self.max_clips_per_video) for c in self.video_clips.clips)
