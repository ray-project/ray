# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

# Helper script that will check if the types and operators used in an ONNX model
# are supported by the pre-built ORT Mobile package.

import argparse
import logging
import pathlib
import sys

import onnx

from ..onnx_model_utils import ModelProtoWithShapeInfo, get_opsets_imported
from ..reduced_build_config_parser import parse_config

cpp_to_tensorproto_type = {
    "float": 1,
    "uint8_t": 2,
    "int8_t": 3,
    "uint16_t": 4,
    "int16_t": 5,
    "int32_t": 6,
    "int64_t": 7,
    "std::string": 8,
    "bool": 9,
    "MLFloat16": 10,
    "double": 11,
    "uint32_t": 12,
    "uint64_t": 13,
    "Complex64": 14,  # not supported by ORT
    "Complex128": 15,  # not supported by ORT
    "BFloat16": 16,
}

tensorproto_type_to_cpp = {v: k for k, v in cpp_to_tensorproto_type.items()}


def check_graph(graph, opsets, required_ops, global_types, special_types, unsupported_ops, logger):
    """
    Check the graph and any subgraphs for usage of types or operators which we know are not supported.
    :param graph: Graph to process.
    :param opsets: Map of domain to opset version that the model imports.
    :param required_ops: Operators that are included in the pre-built package.
    :param global_types: Types globally enabled in the pre-built package.
    :param special_types: Types that are always enabled for a subset of operators and are _usually_ supported but are
                          not guaranteed to be. We would need to add a lot of infrastructure to know for sure so
                          currently we treat them as supported.
    :param unsupported_ops: Set of unsupported operators that were found.
    :param logger: Logger for diagnostic output.
    :return: Returns whether the graph uses unsupported operators or types.
    """
    has_unsupported_types = False
    value_info_map = {vi.name: vi for vi in graph.value_info}

    def _is_type_supported(value_info, description):
        is_supported = True
        type_name = value_info.type.WhichOneof("value")
        if type_name == "tensor_type":
            t = value_info.type.tensor_type.elem_type
            if t not in global_types and t not in special_types:
                cpp_type = tensorproto_type_to_cpp[t]
                logger.debug(f"Element type {cpp_type} of {description} is not supported.")
                is_supported = False
        else:
            # we don't support sequences, map, sparse tensors, or optional types in the pre-built package
            logger.debug(f"Data type {type_name} of {description} is not supported.")
            is_supported = False

        return is_supported

    def _input_output_is_supported(value_info, input_output):
        return _is_type_supported(value_info, f"graph {input_output} {value_info.name}")

    # node outputs are simpler to check.
    # node inputs have a much wider mix of types, some of which come from initializers and most likely are always
    # enabled as we generally do type reduction on the user data input to the operator and not the weights/etc. which
    # come from initializers.
    def _node_output_is_supported(name):
        is_supported = True
        if name in value_info_map:
            vi = value_info_map[name]
            is_supported = _is_type_supported(vi, f"node output {name}")
        else:
            # we don't have type info so ignore
            pass

        return is_supported

    for i in graph.input:
        if not _input_output_is_supported(i, "input"):
            has_unsupported_types = True

    for o in graph.output:
        if not _input_output_is_supported(o, "output"):
            has_unsupported_types = True

    for node in graph.node:
        # required_ops are map of [domain][opset] to set of op_type names. '' == ai.onnx
        domain = node.domain or "ai.onnx"

        # special case Constant as we will convert to an initializer during model load
        if domain == "ai.onnx" and node.op_type == "Constant":
            continue

        # some models don't have complete imports. use 1 as a default as that's valid for custom domains and should
        # result in an error for any others. not sure why ONNX or ORT validation allows this though.
        opset = opsets.get(domain, 1)
        if (
            domain not in required_ops
            or opset not in required_ops[domain]
            or node.op_type not in required_ops[domain][opset]
        ):
            unsupported_ops.add(f"{domain}:{opset}:{node.op_type}")

        for output_name in node.output:
            if not _node_output_is_supported(output_name):
                has_unsupported_types = True

        # recurse into subgraph for control flow nodes (Scan/Loop/If)
        for attr in node.attribute:
            if attr.HasField("g"):
                check_graph(attr.g, opsets, required_ops, global_types, special_types, unsupported_ops, logger)

    return has_unsupported_types or unsupported_ops


def _get_global_tensorproto_types(op_type_impl_filter, logger: logging.Logger):
    """
    Map the globally supported types (C++) to onnx.TensorProto.DataType values used in the model
    See https://github.com/onnx/onnx/blob/1faae95520649c93ae8d0b403816938a190f4fa7/onnx/onnx.proto#L485

    Additionally return a set of types we special case as being able to generally be considered as supported.
    :param op_type_impl_filter: type filter from reduced build configuration parser
    :param logger: Logger
    :return: tuple of globally enabled types and special cased types
    """
    global_cpp_types = op_type_impl_filter.global_type_list()
    global_onnx_tensorproto_types = set()

    for t in global_cpp_types:
        if t in cpp_to_tensorproto_type:
            global_onnx_tensorproto_types.add(cpp_to_tensorproto_type[t])
        else:
            logger.error(f"Error: Unexpected data type of {t} in package build config's globally enabled types.")
            sys.exit(-1)

    # a subset of operators require int32 and int64 to always be enabled, as those types are used for dimensions in
    # shapes and indices.
    # additionally we have a number of operators (e.g. Not, Where) that always require the use of bool.
    # this _may_ mean values involving these types can be processed, but without adding a lot more code we don't know
    # for sure.
    special_types = [
        cpp_to_tensorproto_type["int32_t"],
        cpp_to_tensorproto_type["int64_t"],
        cpp_to_tensorproto_type["bool"],
    ]

    return global_onnx_tensorproto_types, special_types


def get_default_config_path():
    # get default path to config that was used to create the pre-built package.
    script_dir = pathlib.Path(__file__).parent
    local_config = script_dir / "mobile_package.required_operators.config"

    # if we're running in the ORT python package the file should be local. otherwise assume we're running from the
    # ORT repo
    if local_config.exists():
        default_config_path = local_config
    else:
        ort_root = script_dir.parents[3]
        default_config_path = (
            ort_root / "tools" / "ci_build" / "github" / "android" / "mobile_package.required_operators.config"
        )

    return default_config_path


def run_check_with_model(
    model_with_type_info: onnx.ModelProto, mobile_pkg_build_config: pathlib.Path, logger: logging.Logger
):
    """
    Check if an ONNX model can be used with the ORT Mobile pre-built package.
    :param model_with_type_info: ONNX model that has had ONNX shape inferencing run on to add type/shape information.
    :param mobile_pkg_build_config: Configuration file used to build the ORT Mobile package.
    :param logger: Logger for output
    :return: True if supported
    """
    if not mobile_pkg_build_config:
        mobile_pkg_build_config = get_default_config_path()

    enable_type_reduction = True
    config_path = str(mobile_pkg_build_config.resolve(strict=True))
    required_ops, op_type_impl_filter = parse_config(config_path, enable_type_reduction)
    global_onnx_tensorproto_types, special_types = _get_global_tensorproto_types(op_type_impl_filter, logger)

    # get the opset imports
    opsets = get_opsets_imported(model_with_type_info)

    # If the ONNX opset of the model is not supported we can recommend using our tools to update that first.
    supported_onnx_opsets = set(required_ops["ai.onnx"].keys())
    # we have a contrib op that is erroneously in the ai.onnx domain with opset 1. manually remove that incorrect value
    supported_onnx_opsets.remove(1)
    onnx_opset_model_uses = opsets["ai.onnx"]
    if onnx_opset_model_uses not in supported_onnx_opsets:
        logger.info(f"Model uses ONNX opset {onnx_opset_model_uses}.")
        logger.info(f"The pre-built package only supports ONNX opsets {sorted(supported_onnx_opsets)}.")
        logger.info(
            "Please try updating the ONNX model opset to a supported version using "
            "python -m onnxruntime.tools.onnx_model_utils.update_onnx_opset ..."
        )

        return False

    unsupported_ops = set()
    logger.debug(
        "Checking if the data types and operators used in the model are supported in the pre-built ORT package..."
    )
    unsupported = check_graph(
        model_with_type_info.graph,
        opsets,
        required_ops,
        global_onnx_tensorproto_types,
        special_types,
        unsupported_ops,
        logger,
    )

    if unsupported_ops:
        logger.info("Unsupported operators:")
        for entry in sorted(unsupported_ops):
            logger.info("  " + entry)  # noqa: G003

    if unsupported:
        logger.info("\nModel is not supported by the pre-built package due to unsupported types and/or operators.")
        logger.info(
            "Please see https://onnxruntime.ai/docs/install/#install-on-web-and-mobile for information "
            "on what is supported in the pre-built package."
        )
        logger.info(
            "The 'full' ORT package for Android (onnxruntime-android) or iOS (onnxruntime-{objc|c}) could be used, "
            "or a custom build of ONNX Runtime will be required if binary size is critical. Please see "
            "https://onnxruntime.ai/docs/build/custom.html for details on performing that."
        )
    else:
        logger.info("Model should work with the pre-built package.")

    logger.info("---------------\n")

    return not unsupported


def run_check(model_path: pathlib.Path, mobile_pkg_build_config: pathlib.Path, logger: logging.Logger):
    """
    Check if an ONNX model will be able to be used with the ORT Mobile pre-built package.
    :param model_path: Path to ONNX model.
    :param mobile_pkg_build_config: Configuration file used to build the ORT Mobile package.
    :param logger: Logger for output
    :return: True if supported
    """
    logger.info(
        f"Checking if pre-built ORT Mobile package can be used with {model_path} once model is "
        "converted from ONNX to ORT format using onnxruntime.tools.convert_onnx_models_to_ort..."
    )

    model_file = model_path.resolve(strict=True)

    # we need to run shape inferencing to populate that type info for node outputs.
    # we will get warnings if the model uses ORT contrib ops (ONNX does not have shape inferencing for those),
    # and shape inferencing will be lost downstream of those.
    # TODO: add support for checking ORT format model as it will have full type/shape info for all nodes
    model_wrapper = ModelProtoWithShapeInfo(model_file)
    return run_check_with_model(model_wrapper.model_with_shape_info, mobile_pkg_build_config, logger)


def main():
    parser = argparse.ArgumentParser(
        description="Check if model can be run using the ONNX Runtime Mobile Pre-Built Package",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--config_path",
        help="Path to required operators and types configuration used to build the pre-built ORT mobile package.",
        required=False,
        type=pathlib.Path,
        default=get_default_config_path(),
    )

    parser.add_argument("model_path", help="Path to ONNX model to check", type=pathlib.Path)

    args = parser.parse_args()

    logger = logging.getLogger("default")
    logger.setLevel(logging.INFO)
    run_check(args.model_path, args.config_path, logger)


if __name__ == "__main__":
    main()
