# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation.  All rights reserved.
# Licensed under the MIT License.
# --------------------------------------------------------------------------
#
# This script converts stable diffusion onnx models from float to half (mixed) precision for GPU inference.
#
# Before running this script, follow README.md to setup python environment and convert stable diffusion checkpoint
# to float32 onnx models.
#
# For example, the float32 ONNX pipeline is saved to ./sd-v1-5 directory, you can optimize and convert it to float16
# like the following:
#    python optimize_pipeline.py -i ./sd-v1-5 -o ./sd-v1-5-fp16 --float16
#
# Note that the optimizations are carried out for CUDA Execution Provider at first, other EPs may not have the support
# for the fused operators. The users could disable the operator fusion manually to workaround.

import argparse
import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import List, Optional

import __init__  # noqa: F401. Walk-around to run this script directly
import coloredlogs
import onnx
from fusion_options import FusionOptions
from onnx_model_clip import ClipOnnxModel
from onnx_model_unet import UnetOnnxModel
from onnx_model_vae import VaeOnnxModel
from optimizer import optimize_by_onnxruntime, optimize_model
from packaging import version

import onnxruntime

logger = logging.getLogger(__name__)


def has_external_data(onnx_model_path):
    original_model = onnx.load_model(str(onnx_model_path), load_external_data=False)
    for initializer in original_model.graph.initializer:
        if initializer.HasField("data_location") and initializer.data_location == onnx.TensorProto.EXTERNAL:
            return True
    return False


def _optimize_sd_pipeline(
    source_dir: Path,
    target_dir: Path,
    use_external_data_format: Optional[bool],
    float16: bool,
    force_fp32_ops: List[str],
    enable_runtime_optimization: bool,
    args,
):
    """Optimize onnx models used in stable diffusion onnx pipeline and optionally convert to float16.

    Args:
        source_dir (Path): Root of input directory of stable diffusion onnx pipeline with float32 models.
        target_dir (Path): Root of output directory of stable diffusion onnx pipeline with optimized models.
        use_external_data_format (Optional[bool]): use external data format.
        float16 (bool): use half precision
        force_fp32_ops(List[str]): operators that are forced to run in float32.
        enable_runtime_optimization(bool): run graph optimization using Onnx Runtime.

    Raises:
        RuntimeError: input onnx model does not exist
        RuntimeError: output onnx model path existed
    """
    model_type_mapping = {
        "unet": "unet",
        "vae_encoder": "vae",
        "vae_decoder": "vae",
        "text_encoder": "clip",
        "text_encoder_2": "clip",
        "safety_checker": "unet",
    }

    model_type_class_mapping = {
        "unet": UnetOnnxModel,
        "vae": VaeOnnxModel,
        "clip": ClipOnnxModel,
    }

    force_fp32_operators = {
        "unet": [],
        "vae_encoder": [],
        "vae_decoder": [],
        "text_encoder": [],
        "text_encoder_2": [],
        "safety_checker": [],
    }

    is_xl = (source_dir / "text_encoder_2").exists()

    if force_fp32_ops:
        for fp32_operator in force_fp32_ops:
            parts = fp32_operator.split(":")
            if len(parts) == 2 and parts[0] in force_fp32_operators and (parts[1] and parts[1][0].isupper()):
                force_fp32_operators[parts[0]].append(parts[1])
            else:
                raise ValueError(
                    f"--force_fp32_ops shall be in the format of module:operator like unet:Attention, got {fp32_operator}"
                )

    for name, model_type in model_type_mapping.items():
        onnx_model_path = source_dir / name / "model.onnx"
        if not os.path.exists(onnx_model_path):
            if name != "safety_checker":
                logger.info("input onnx model does not exist: %s", onnx_model_path)
            # some model are optional so we do not raise error here.
            continue

        # Prepare output directory
        optimized_model_path = target_dir / name / "model.onnx"
        output_dir = optimized_model_path.parent
        output_dir.mkdir(parents=True, exist_ok=True)

        if use_external_data_format is None:
            use_external_data_format = has_external_data(onnx_model_path)

        # Graph fusion before fp16 conversion, otherwise they cannot be fused later.
        logger.info(f"Optimize {onnx_model_path}...")

        args.model_type = model_type
        fusion_options = FusionOptions.parse(args)

        if model_type in ["unet"]:
            # Some optimizations are not available in v1.14 or older version: packed QKV and BiasAdd
            has_all_optimizations = version.parse(onnxruntime.__version__) >= version.parse("1.15.0")
            fusion_options.enable_packed_kv = float16 and fusion_options.enable_packed_kv
            fusion_options.enable_packed_qkv = float16 and has_all_optimizations and fusion_options.enable_packed_qkv
            fusion_options.enable_bias_add = has_all_optimizations and fusion_options.enable_bias_add

        m = optimize_model(
            str(onnx_model_path),
            model_type=model_type,
            num_heads=0,  # will be deduced from graph
            hidden_size=0,  # will be deduced from graph
            opt_level=0,
            optimization_options=fusion_options,
            use_gpu=True,
            provider=args.provider,
        )

        if float16:
            # For SD-XL, use FP16 in VAE decoder will cause NaN and black image so we keep it in FP32.
            if is_xl and name == "vae_decoder":
                logger.info("Skip converting %s to float16 to avoid NaN", name)
            else:
                logger.info("Convert %s to float16 ...", name)
                m.convert_float_to_float16(
                    keep_io_types=False,
                    op_block_list=force_fp32_operators[name],
                )

        if enable_runtime_optimization:
            # Use this step to see the final graph that executed by Onnx Runtime.
            with tempfile.TemporaryDirectory() as tmp_dir:
                # Save to a temporary file so that we can load it with Onnx Runtime.
                logger.info("Saving a temporary model to run OnnxRuntime graph optimizations...")
                tmp_model_path = Path(tmp_dir) / "model.onnx"
                m.save_model_to_file(str(tmp_model_path), use_external_data_format=use_external_data_format)
                ort_optimized_model_path = Path(tmp_dir) / "optimized.onnx"
                optimize_by_onnxruntime(
                    str(tmp_model_path),
                    use_gpu=True,
                    provider=args.provider,
                    optimized_model_path=str(ort_optimized_model_path),
                    save_as_external_data=use_external_data_format,
                )
                model = onnx.load(str(ort_optimized_model_path), load_external_data=True)
                m = model_type_class_mapping[model_type](model)

        m.get_operator_statistics()
        m.get_fused_operator_statistics()
        m.save_model_to_file(str(optimized_model_path), use_external_data_format=use_external_data_format)
        logger.info("%s is optimized", name)
        logger.info("*" * 20)


def _copy_extra_directory(source_dir: Path, target_dir: Path):
    """Copy extra directory that does not have onnx model

    Args:
        source_dir (Path): source directory
        target_dir (Path): target directory

    Raises:
        RuntimeError: source path does not exist
    """
    extra_dirs = ["scheduler", "tokenizer", "tokenizer_2", "feature_extractor"]

    for name in extra_dirs:
        source_path = source_dir / name
        if not os.path.exists(source_path):
            continue

        target_path = target_dir / name
        shutil.copytree(source_path, target_path)
        logger.info("%s => %s", source_path, target_path)

    extra_files = ["model_index.json"]
    for name in extra_files:
        source_path = source_dir / name
        if not os.path.exists(source_path):
            raise RuntimeError(f"source path does not exist: {source_path}")

        target_path = target_dir / name
        shutil.copyfile(source_path, target_path)
        logger.info("%s => %s", source_path, target_path)

    # Some directory are optional
    onnx_model_dirs = ["text_encoder", "text_encoder_2", "unet", "vae_encoder", "vae_decoder", "safety_checker"]
    for onnx_model_dir in onnx_model_dirs:
        source_path = source_dir / onnx_model_dir / "config.json"
        target_path = target_dir / onnx_model_dir / "config.json"
        if source_path.exists():
            target_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source_path, target_path)
            logger.info("%s => %s", source_path, target_path)


def optimize_stable_diffusion_pipeline(
    input_dir: str,
    output_dir: str,
    overwrite: bool,
    use_external_data_format: Optional[bool],
    float16: bool,
    enable_runtime_optimization: bool,
    args,
):
    if os.path.exists(output_dir):
        if overwrite:
            shutil.rmtree(output_dir, ignore_errors=True)
        else:
            raise RuntimeError("output directory existed:{output_dir}. Add --overwrite to empty the directory.")

    source_dir = Path(input_dir)
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    _copy_extra_directory(source_dir, target_dir)

    _optimize_sd_pipeline(
        source_dir,
        target_dir,
        use_external_data_format,
        float16,
        args.force_fp32_ops,
        enable_runtime_optimization,
        args,
    )


def parse_arguments(argv: Optional[List[str]] = None):
    """Parse arguments

    Returns:
        Namespace: arguments
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-i",
        "--input",
        required=True,
        type=str,
        help="Root of input directory of stable diffusion onnx pipeline with float32 models.",
    )

    parser.add_argument(
        "-o",
        "--output",
        required=True,
        type=str,
        help="Root of output directory of stable diffusion onnx pipeline with optimized models.",
    )

    parser.add_argument(
        "--float16",
        required=False,
        action="store_true",
        help="Output models of half or mixed precision.",
    )
    parser.set_defaults(float16=False)

    parser.add_argument(
        "--force_fp32_ops",
        required=False,
        nargs="+",
        type=str,
        help="Force given operators (like unet:Attention) to run in float32. It is case sensitive!",
    )

    parser.add_argument(
        "--inspect",
        required=False,
        action="store_true",
        help="Save the optimized graph from Onnx Runtime. "
        "This option has no impact on inference performance except it might reduce session creation time.",
    )
    parser.set_defaults(inspect=False)

    parser.add_argument(
        "--overwrite",
        required=False,
        action="store_true",
        help="Overwrite exists files.",
    )
    parser.set_defaults(overwrite=False)

    parser.add_argument(
        "-e",
        "--use_external_data_format",
        required=False,
        action="store_true",
        help="Onnx model larger than 2GB need to use external data format. "
        "If specified, save each onnx model to two files: one for onnx graph, another for weights. "
        "If not specified, use same format as original model by default. ",
    )
    parser.set_defaults(use_external_data_format=None)

    parser.add_argument(
        "--provider",
        required=False,
        type=str,
        default=None,
        help="Execution provider to use.",
    )

    FusionOptions.add_arguments(parser)

    args = parser.parse_args(argv)
    return args


def main(argv: Optional[List[str]] = None):
    args = parse_arguments(argv)
    logger.info("Arguments: %s", str(args))
    optimize_stable_diffusion_pipeline(
        args.input, args.output, args.overwrite, args.use_external_data_format, args.float16, args.inspect, args
    )


if __name__ == "__main__":
    coloredlogs.install(fmt="%(funcName)20s: %(message)s")
    main()
