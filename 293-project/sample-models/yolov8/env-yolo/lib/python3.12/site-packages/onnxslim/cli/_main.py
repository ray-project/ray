from typing import Union

import onnx


def slim(model: Union[str, onnx.ModelProto], *args, **kwargs):
    import os
    import time
    from pathlib import Path

    from onnxslim.core import (
        convert_data_format,
        freeze,
        input_modification,
        input_shape_modification,
        optimize,
        output_modification,
        shape_infer,
    )
    from onnxslim.utils import (
        check_onnx,
        check_point,
        check_result,
        dump_model_info_to_disk,
        init_logging,
        onnxruntime_inference,
        print_model_info_as_table,
        save,
        summarize_model,
    )

    output_model = args[0] if len(args) > 0 else kwargs.get("output_model", None)
    model_check = kwargs.get("model_check", False)
    input_shapes = kwargs.get("input_shapes", None)
    inputs = kwargs.get("inputs", None)
    outputs = kwargs.get("outputs", None)
    no_shape_infer = kwargs.get("no_shape_infer", False)
    no_constant_folding = kwargs.get("no_constant_folding", False)
    dtype = kwargs.get("dtype", None)
    skip_fusion_patterns = kwargs.get("skip_fusion_patterns", None)
    inspect = kwargs.get("inspect", False)
    dump_to_disk = kwargs.get("dump_to_disk", False)
    save_as_external_data = kwargs.get("save_as_external_data", False)
    model_check_inputs = kwargs.get("model_check_inputs", None)
    verbose = kwargs.get("verbose", False)

    logger = init_logging(verbose)

    MAX_ITER = int(os.getenv("ONNXSLIM_MAX_ITER")) if os.getenv("ONNXSLIM_MAX_ITER") else 10

    if isinstance(model, str):
        model_name = Path(model).name
        model = onnx.load(model)
    else:
        model_name = "OnnxModel"

    freeze(model)

    start_time = time.time()

    if output_model or inspect:
        float_info = summarize_model(model)

    if inspect:
        print_model_info_as_table(model_name, [float_info])
        if dump_to_disk:
            dump_model_info_to_disk(model_name, float_info)
        return None

    if inputs:
        model = input_modification(model, inputs)

    if input_shapes:
        model = input_shape_modification(model, input_shapes)

    if outputs:
        model = output_modification(model, outputs)

    if model_check:
        input_data_dict, raw_onnx_output, model = check_onnx(model, model_check_inputs)

    if not no_shape_infer:
        model = shape_infer(model)

    if not no_constant_folding:
        graph_check_point = check_point(model)
        while MAX_ITER > 0:
            logger.debug(f"iter: {MAX_ITER}")
            model = optimize(model, skip_fusion_patterns)
            if not no_shape_infer:
                model = shape_infer(model)
            graph = check_point(model)
            if graph == graph_check_point:
                logger.debug(f"converged at iter: {MAX_ITER}")
                break
            else:
                graph_check_point = graph

            MAX_ITER -= 1

    if dtype:
        model = convert_data_format(model, dtype)

    if model_check:
        slimmed_onnx_output, model = onnxruntime_inference(model, input_data_dict)
        check_result(raw_onnx_output, slimmed_onnx_output)

    if not output_model:
        return model

    slimmed_info = summarize_model(model)
    save(model, output_model, model_check, save_as_external_data, slimmed_info)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print_model_info_as_table(
        model_name,
        [float_info, slimmed_info],
        elapsed_time,
    )


def main():
    """Entry point for the OnnxSlim toolkit, processes command-line arguments and passes them to the slim function."""
    from onnxslim.argparser import (
        ArgumentParser,
        CheckerArguments,
        ModelArguments,
        ModificationArguments,
        OptimizationArguments,
    )

    argument_parser = ArgumentParser(ModelArguments, OptimizationArguments, ModificationArguments, CheckerArguments)
    model_args, optimization_args, modification_args, checker_args = argument_parser.parse_args_into_dataclasses()

    if checker_args.inspect and model_args.output_model:
        argument_parser.error("--inspect and output_model are mutually exclusive")

    if not checker_args.inspect and checker_args.dump_to_disk:
        argument_parser.error("dump_to_disk can only be used with --inspect")

    if not optimization_args.no_shape_infer or optimization_args.no_constant_folding:
        from onnxslim.utils import check_onnx_compatibility

        check_onnx_compatibility()

    slim(
        model_args.input_model,
        model_args.output_model,
        **optimization_args.__dict__,
        **modification_args.__dict__,
        **checker_args.__dict__,
    )

    return 0
