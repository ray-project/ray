#!/usr/bin/env python

# Copyright 2021 The HuggingFace Team. All rights reserved.
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

import logging
import os
from argparse import Namespace
from typing import Optional, Tuple, Union
import tempfile
from pathlib import Path
from packaging.version import Version

from contextlib import nullcontext

import accelerate

if Version(accelerate.__version__) < Version("0.17.0.dev0"):
    raise ImportError(
        f"AccelerateTrainer requires accelerate>=0.17.0, got {accelerate.__version__}"
    )

from accelerate.commands.launch import (
    ComputeEnvironment,
    _validate_launch_command,
    launch_command_parser,
    prepare_deepspeed_cmd_env,
    prepare_multi_gpu_env,
    prepare_simple_launcher_cmd_env,
)
from accelerate.utils import is_deepspeed_available
from accelerate.commands.config import default_config_file, load_config_from_file
from accelerate.commands.config.config_args import ClusterConfig

logger = logging.getLogger(__name__)


class AccelerateDefaultNamespace(Namespace):
    @property
    def parser(self):
        return launch_command_parser()

    def __getattr__(self, name: str):
        if name == "training_script_args":
            return []
        return self.parser.get_default(name)


class AccelerateConfigWrapper:
    """
    Lets Trainables know to treat this as already loaded file content instead of path.
    """

    def __init__(
        self, config_raw: str, deepspeed_config_raw: Optional[str] = None
    ) -> None:
        self.config_raw = config_raw
        self.deepspeed_config_raw = deepspeed_config_raw

    def __bool__(self) -> bool:
        return bool(self.config_raw)


def simple_launcher(args):
    _, current_env = prepare_simple_launcher_cmd_env(args)

    os.environ.update(current_env)


def multi_gpu_launcher(args):
    current_env = prepare_multi_gpu_env(args)
    os.environ.update(current_env)


def deepspeed_launcher(args):
    if not is_deepspeed_available():
        raise ImportError(
            "DeepSpeed is not installed => run `pip3 install deepspeed` "
            "or build it from source."
        )

    _, current_env = prepare_deepspeed_cmd_env(args)

    os.environ.update(current_env)


def launch_command(args):
    args, defaults, mp_from_config_flag = _validate_launch_command(args)

    # Use the proper launcher
    if args.use_deepspeed and not args.cpu:
        args.deepspeed_fields_from_accelerate_config = (
            list(defaults.deepspeed_config.keys()) if defaults else []
        )
        if mp_from_config_flag:
            args.deepspeed_fields_from_accelerate_config.append("mixed_precision")
        args.deepspeed_fields_from_accelerate_config = ",".join(
            args.deepspeed_fields_from_accelerate_config
        )
        deepspeed_launcher(args)
    elif args.use_fsdp and not args.cpu:
        multi_gpu_launcher(args)
    elif args.use_megatron_lm and not args.cpu:
        multi_gpu_launcher(args)
    elif args.multi_gpu and not args.cpu:
        multi_gpu_launcher(args)
    elif args.tpu and not args.cpu:
        raise NotImplementedError("")
    elif (
        defaults is not None
        and defaults.compute_environment == ComputeEnvironment.AMAZON_SAGEMAKER
    ):
        raise NotImplementedError(
            "Cannot use SageMaker compute environment with Ray Train."
        )
    else:
        simple_launcher(args)


def load_accelerate_config(
    accelerate_config: Optional[Union[str, dict]],
) -> Tuple[str, Optional[str]]:
    if isinstance(accelerate_config, dict):
        ctx = tempfile.TemporaryDirectory
    else:
        ctx = nullcontext

    with ctx() as tempdir:
        if isinstance(accelerate_config, dict):
            # We save the dict to file, as Accelerate doesn't allow users to pass
            # dicts directly. That way, we ensure the behavior is consistent with
            # vanilla Accelerate, which has side effects when loading the file.
            # The file is loaded by Accelerate in `launch_command`.
            tempdir = Path(tempdir)
            accelerate_config_path = str(tempdir / "accelerate_config.json")
            # Those are the same default settings as in Accelerate.
            # Those keys cannot be missing from the config.
            accelerate_config.setdefault("num_processes", 1)
            accelerate_config.setdefault("distributed_type", "NO")
            accelerate_config.setdefault("mixed_precision", "no")
            accelerate_config.setdefault("compute_environment", "LOCAL_MACHINE")
            accelerate_config.setdefault("use_cpu", True)
            config = ClusterConfig(**accelerate_config)
            config.to_json_file(accelerate_config_path)
        else:
            accelerate_config_path = (
                str(accelerate_config) if accelerate_config else default_config_file
            )

        # We only load config to dict to obtain the deepspeed_config_file
        config = load_config_from_file(accelerate_config_path)
        deepspeed_config_file = getattr(config, "deepspeed_config_file", None)
        deepspeed_config_file_raw = None

        if deepspeed_config_file and not isinstance(deepspeed_config_file, dict):
            with open(deepspeed_config_file, "r") as f:
                deepspeed_config_file_raw = f.read()

        # Otherwise, we want to pass raw contents to Trainables for maximum
        # compatibility.
        with open(accelerate_config_path, "r") as f:
            raw_loaded_config = f.read()

        return raw_loaded_config, deepspeed_config_file_raw
