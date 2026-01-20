import os
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from huggingface_hub import snapshot_download

from ray.data.util.lerobot_utils.lerobot_meta_utils import (
    get_safe_version,
    is_valid_version,
    load_episodes,
    load_info,
    load_stats,
    load_tasks,
)

CODEBASE_VERSION = "v3.0"
HF_LEROBOT_HOME = Path(
    os.getenv("HF_LEROBOT_HOME", "~/.cache/huggingface/lerobot")
).expanduser()

INFO_PATH = "meta/info.json"
STATS_PATH = "meta/stats.json"

EPISODES_DIR = "meta/episodes"
DATA_DIR = "data"
VIDEO_DIR = "videos"

CHUNK_FILE_PATTERN = "chunk-{chunk_index:03d}/file-{file_index:03d}"
DEFAULT_TASKS_PATH = "meta/tasks.parquet"
DEFAULT_EPISODES_PATH = EPISODES_DIR + "/" + CHUNK_FILE_PATTERN + ".parquet"
DEFAULT_DATA_PATH = DATA_DIR + "/" + CHUNK_FILE_PATTERN + ".parquet"
DEFAULT_VIDEO_PATH = VIDEO_DIR + "/{video_key}/" + CHUNK_FILE_PATTERN + ".mp4"
DEFAULT_IMAGE_PATH = (
    "images/{image_key}/episode-{episode_index:06d}/frame-{frame_index:06d}.png"
)


class LeRobotDatasetMetadata:
    def __init__(
        self,
        repo_id: str,
        root: str | Path | None = None,
        revision: str | None = None,
        force_cache_sync: bool = False,
        metadata_buffer_size: int = 10,
    ):
        self.repo_id = repo_id
        self.revision = revision if revision else CODEBASE_VERSION
        self.root = Path(root) if root is not None else HF_LEROBOT_HOME / repo_id
        self.writer = None
        self.latest_episode = None
        self.metadata_buffer: list[dict] = []
        self.metadata_buffer_size = metadata_buffer_size

        try:
            if force_cache_sync:
                raise FileNotFoundError
            self.load_metadata()
        except (FileNotFoundError, NotADirectoryError):
            if is_valid_version(self.revision):
                self.revision = get_safe_version(self.repo_id, self.revision)

            (self.root / "meta").mkdir(exist_ok=True, parents=True)
            self.pull_from_repo(allow_patterns="meta/")
            self.load_metadata()

    def _flush_metadata_buffer(self) -> None:
        """Write all buffered episode metadata to parquet file."""
        if not hasattr(self, "metadata_buffer") or len(self.metadata_buffer) == 0:
            return

        combined_dict = {}
        for episode_dict in self.metadata_buffer:
            for key, value in episode_dict.items():
                if key not in combined_dict:
                    combined_dict[key] = []
                # Extract value and serialize numpy arrays
                # because PyArrow's from_pydict function doesn't support numpy arrays
                val = value[0] if isinstance(value, list) else value
                combined_dict[key].append(
                    val.tolist() if isinstance(val, np.ndarray) else val
                )

        first_ep = self.metadata_buffer[0]
        chunk_idx = first_ep["meta/episodes/chunk_index"][0]
        file_idx = first_ep["meta/episodes/file_index"][0]

        table = pa.Table.from_pydict(combined_dict)

        if not self.writer:
            path = Path(
                self.root
                / DEFAULT_EPISODES_PATH.format(
                    chunk_index=chunk_idx, file_index=file_idx
                )
            )
            path.parent.mkdir(parents=True, exist_ok=True)
            self.writer = pq.ParquetWriter(
                path, schema=table.schema, compression="snappy", use_dictionary=True
            )

        self.writer.write_table(table)

        self.latest_episode = self.metadata_buffer[-1]
        self.metadata_buffer.clear()

    def _close_writer(self) -> None:
        """Close and cleanup the parquet writer if it exists."""
        self._flush_metadata_buffer()

        writer = getattr(self, "writer", None)
        if writer is not None:
            writer.close()
            self.writer = None

    def __del__(self):
        """
        Trust the user to call .finalize() but as an added safety check call the parquet writer to stop when calling the destructor
        """
        self._close_writer()

    def load_metadata(self):
        self.info = load_info(self.root)
        self.tasks = load_tasks(self.root)
        self.episodes = load_episodes(self.root)
        self.stats = load_stats(self.root)

    def pull_from_repo(
        self,
        allow_patterns: list[str] | str | None = None,
        ignore_patterns: list[str] | str | None = None,
    ) -> None:
        snapshot_download(
            self.repo_id,
            repo_type="dataset",
            revision=self.revision,
            local_dir=self.root,
            allow_patterns=allow_patterns,
            ignore_patterns=ignore_patterns,
        )

    @property
    def url_root(self) -> str:
        return f"hf://datasets/{self.repo_id}"

    # @property
    # def _version(self) -> packaging.version.Version:
    #     """Codebase version used to create this dataset."""
    #     return packaging.version.parse(self.info["codebase_version"])

    def get_data_file_path(self, ep_index: int) -> Path:
        if self.episodes is None:
            self.episodes = load_episodes(self.root)
        if ep_index >= len(self.episodes):
            raise IndexError(
                f"Episode index {ep_index} out of range. Episodes: {len(self.episodes) if self.episodes else 0}"
            )
        ep = self.episodes[ep_index]
        chunk_idx = ep["data/chunk_index"]
        file_idx = ep["data/file_index"]
        fpath = self.data_path.format(chunk_index=chunk_idx, file_index=file_idx)
        return Path(fpath)

    def get_video_file_path(self, ep_index: int, vid_key: str) -> Path:
        if self.episodes is None:
            self.episodes = load_episodes(self.root)
        if ep_index >= len(self.episodes):
            raise IndexError(
                f"Episode index {ep_index} out of range. Episodes: {len(self.episodes) if self.episodes else 0}"
            )
        ep = self.episodes[ep_index]
        chunk_idx = ep[f"videos/{vid_key}/chunk_index"]
        file_idx = ep[f"videos/{vid_key}/file_index"]
        fpath = self.video_path.format(
            video_key=vid_key, chunk_index=chunk_idx, file_index=file_idx
        )
        return Path(fpath)

    @property
    def data_path(self) -> str:
        """Formattable string for the parquet files."""
        return self.info["data_path"]

    @property
    def video_path(self) -> str | None:
        """Formattable string for the video files."""
        return self.info["video_path"]

    @property
    def robot_type(self) -> str | None:
        """Robot type used in recording this dataset."""
        return self.info["robot_type"]

    @property
    def fps(self) -> int:
        """Frames per second used during data collection."""
        return self.info["fps"]

    @property
    def features(self) -> dict[str, dict]:
        """All features contained in the dataset."""
        return self.info["features"]

    @property
    def image_keys(self) -> list[str]:
        """Keys to access visual modalities stored as images."""
        return [key for key, ft in self.features.items() if ft["dtype"] == "image"]

    @property
    def video_keys(self) -> list[str]:
        """Keys to access visual modalities stored as videos."""
        return [key for key, ft in self.features.items() if ft["dtype"] == "video"]

    @property
    def camera_keys(self) -> list[str]:
        """Keys to access visual modalities (regardless of their storage method)."""
        return [
            key
            for key, ft in self.features.items()
            if ft["dtype"] in ["video", "image"]
        ]

    @property
    def names(self) -> dict[str, list | dict]:
        """Names of the various dimensions of vector modalities."""
        return {key: ft["names"] for key, ft in self.features.items()}

    @property
    def shapes(self) -> dict:
        """Shapes for the different features."""
        return {key: tuple(ft["shape"]) for key, ft in self.features.items()}

    @property
    def total_episodes(self) -> int:
        """Total number of episodes available."""
        return self.info["total_episodes"]

    @property
    def total_frames(self) -> int:
        """Total number of frames saved in this dataset."""
        return self.info["total_frames"]

    @property
    def total_tasks(self) -> int:
        """Total number of different tasks performed in this dataset."""
        return self.info["total_tasks"]

    @property
    def chunks_size(self) -> int:
        """Max number of files per chunk."""
        return self.info["chunks_size"]

    @property
    def data_files_size_in_mb(self) -> int:
        """Max size of data file in mega bytes."""
        return self.info["data_files_size_in_mb"]

    @property
    def video_files_size_in_mb(self) -> int:
        """Max size of video file in mega bytes."""
        return self.info["video_files_size_in_mb"]

    def get_task_index(self, task: str) -> int | None:
        """
        Given a task in natural language, returns its task_index if the task already exists in the dataset,
        otherwise return None.
        """
        if task in self.tasks.index:
            return int(self.tasks.loc[task].task_index)
        else:
            return None

    def get_chunk_settings(self) -> dict[str, int]:
        """Get current chunk and file size settings.

        Returns:
            Dict containing chunks_size, data_files_size_in_mb, and video_files_size_in_mb.
        """
        return {
            "chunks_size": self.chunks_size,
            "data_files_size_in_mb": self.data_files_size_in_mb,
            "video_files_size_in_mb": self.video_files_size_in_mb,
        }

    def __repr__(self):
        feature_keys = list(self.features)
        return (
            f"{self.__class__.__name__}({{\n"
            f"    Repository ID: '{self.repo_id}',\n"
            f"    Total episodes: '{self.total_episodes}',\n"
            f"    Total frames: '{self.total_frames}',\n"
            f"    Features: '{feature_keys}',\n"
            "})',\n"
        )


if __name__ == "__main__":
    meta = LeRobotDatasetMetadata(repo_id="lerobot/pusht")
    print(meta.total_episodes)
    print(meta.url_root)
    print(meta.data_path)
    print(meta.video_path)
    print(meta.robot_type)
    print(meta.fps)
    print(meta.features)
    print(meta.image_keys)
    print(meta.video_keys)
    print(meta.camera_keys)

    meta = LeRobotDatasetMetadata(
        repo_id="lerobot/xvla_soft_fold",
        root="/scratch/zhexin/dataset_source/xvla_soft_fold",
    )
    print(meta.total_episodes)
    print(meta.url_root)
    print(meta.data_path)
    print(meta.video_path)
    print(meta.robot_type)
    print(meta.fps)
    print(meta.features)
    print(meta.image_keys)
    print(meta.video_keys)
    print(meta.camera_keys)
