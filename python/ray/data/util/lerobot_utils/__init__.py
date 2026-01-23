# Copyright 2024 The HuggingFace Inc. team. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# This file has been modified by Jeffery Shen (jeffery4011, jefferyshen1015@gmail.com)
# for integration with Ray Data.
# Original source: https://github.com/huggingface/lerobot

"""LeRobot utilities for Ray Data integration."""

from .horizon_utils import get_horizon_indices
from .lerobot_dataset_meta import LeRobotDatasetMetadata
from .lerobot_meta_utils import (
    get_safe_version,
    is_valid_version,
    load_episodes,
    load_info,
    load_stats,
    load_tasks,
)

# Note: video_utils imports are not included here to avoid importing
# optional dependencies (av/PyAV) at module level. Import them directly
# from ray.data.util.lerobot_utils.video_utils when needed.

__all__ = [
    "load_info",
    "load_stats",
    "load_tasks",
    "load_episodes",
    "is_valid_version",
    "get_safe_version",
    "LeRobotDatasetMetadata",
    "get_horizon_indices",
]
