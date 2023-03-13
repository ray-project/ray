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
from typing import Optional, Tuple

try:
    from packaging.version import Version
except ImportError:
    from distutils.version import LooseVersion as Version

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
        raise NotImplementedError()
    elif (
        defaults is not None
        and defaults.compute_environment == ComputeEnvironment.AMAZON_SAGEMAKER
    ):
        raise NotImplementedError()
    else:
        simple_launcher(args)


def load_accelerate_config(
    accelerate_config: Optional[str],
) -> Tuple[str, Optional[str]]:
    # We only load config to dict to obtain the deepspeed_config_file
    config = load_config_from_file(
        str(accelerate_config) if accelerate_config else default_config_file
    )
    deepspeed_config_file = getattr(config, "deepspeed_config_file", None)
    deepspeed_config_file_raw = None

    if deepspeed_config_file and not isinstance(deepspeed_config_file, dict):
        with open(deepspeed_config_file, "r") as f:
            deepspeed_config_file_raw = f.read()

    # Otherwise, we want to pass raw contents to Trainables for maximum
    # compatibility.
    with open(accelerate_config, "r") as f:
        raw_loaded_config = f.read()

    return raw_loaded_config, deepspeed_config_file_raw
