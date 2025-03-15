load("@rules_python//python:defs.bzl", "py_library", "py_test")
load("//bazel:python.bzl", "doctest", "py_test_module_list")

doctest(
    size = "large",
    files = glob(
        ["**/*.py"],
        exclude = glob([
            "examples/**/*",
            "tests/**/*",
        ]),
    ),
    tags = ["team:data"],
)

py_library(
    name = "conftest",
    srcs = ["tests/conftest.py"],
    deps = ["//python/ray/tests:conftest"],
)

py_test_module_list(
    size = "medium",
    files = glob(["tests/block_batching/test_*.py"]),
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test_module_list(
    size = "small",
    files = glob(["tests/preprocessors/test_*.py"]),
    tags = [
        "exclusive",
        "ray_air",
        "team:ml",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_formats",
    size = "medium",
    srcs = ["tests/test_formats.py"],
    data = glob(["tests/image-folder/**/*"]),
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_strict_mode",
    size = "small",
    srcs = ["tests/test_strict_mode.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_numpy_support",
    size = "small",
    srcs = ["tests/test_numpy_support.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_sql",
    size = "small",
    srcs = ["tests/test_sql.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_mongo",
    size = "large",
    srcs = ["tests/test_mongo.py"],
    tags = [
        "data_integration",
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_bigquery",
    size = "large",
    srcs = ["tests/test_bigquery.py"],
    tags = [
        "data_integration",
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_actor_pool_map_operator",
    size = "medium",
    srcs = ["tests/test_actor_pool_map_operator.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_arrow_serialization",
    size = "small",
    srcs = ["tests/test_arrow_serialization.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_arrow_block",
    size = "medium",
    srcs = ["tests/test_arrow_block.py"],
    tags = [
        "data_non_parallel",
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_auto_parallelism",
    size = "medium",
    srcs = ["tests/test_auto_parallelism.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_batcher",
    size = "small",
    srcs = ["tests/test_batcher.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_block_batching",
    size = "medium",
    srcs = ["tests/block_batching/test_block_batching.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_block_sizing",
    size = "medium",
    srcs = ["tests/test_block_sizing.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_context_propagation",
    size = "small",
    srcs = ["tests/test_context_propagation.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_audio",
    size = "small",
    srcs = ["tests/test_audio.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_aggregate",
    size = "small",
    srcs = ["tests/test_aggregate.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_avro",
    size = "small",
    srcs = ["tests/test_avro.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_all_to_all",
    size = "enormous",
    srcs = ["tests/test_all_to_all.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_binary",
    size = "small",
    srcs = ["tests/test_binary.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_consumption",
    size = "large",
    srcs = ["tests/test_consumption.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_csv",
    size = "medium",
    srcs = ["tests/test_csv.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_datasink",
    size = "small",
    srcs = ["tests/test_datasink.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_ecosystem",
    size = "small",
    srcs = ["tests/test_ecosystem.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_file_based_datasource",
    size = "small",
    srcs = ["tests/test_file_based_datasource.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_file_datasink",
    size = "small",
    srcs = ["tests/test_file_datasink.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_filename_provider",
    size = "small",
    srcs = ["tests/test_filename_provider.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_hudi",
    size = "small",
    srcs = ["tests/test_hudi.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_image",
    size = "medium",
    srcs = ["tests/test_image.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_iterator",
    size = "small",
    srcs = ["tests/test_iterator.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_json",
    size = "medium",
    srcs = ["tests/test_json.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_logging",
    size = "small",
    srcs = ["tests/test_logging.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_map",
    size = "large",
    srcs = ["tests/test_map.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_numpy",
    size = "medium",
    srcs = ["tests/test_numpy.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_pandas",
    size = "small",
    srcs = ["tests/test_pandas.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_pandas_block",
    size = "small",
    srcs = ["tests/test_pandas_block.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_parquet",
    size = "medium",
    srcs = ["tests/test_parquet.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_tensor",
    size = "small",
    srcs = ["tests/test_tensor.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_text",
    size = "small",
    srcs = ["tests/test_text.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_tf",
    size = "small",
    srcs = ["tests/test_tf.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_tfrecords",
    size = "small",
    srcs = ["tests/test_tfrecords.py"],
    tags = [
        "exclusive",
        "team:data",
        "tfxbsl",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_torch",
    size = "small",
    srcs = ["tests/test_torch.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_dynamic_block_split",
    size = "medium",
    srcs = ["tests/test_dynamic_block_split.py"],
    tags = [
        "data_non_parallel",
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_splitblocks",
    size = "medium",
    srcs = ["tests/test_splitblocks.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_exceptions",
    size = "small",
    srcs = ["tests/test_exceptions.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_execution_optimizer",
    size = "medium",
    srcs = ["tests/test_execution_optimizer.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_executor_resource_management",
    size = "small",
    srcs = ["tests/test_executor_resource_management.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_huggingface",
    size = "medium",
    srcs = ["tests/test_huggingface.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_iceberg",
    size = "medium",
    srcs = ["tests/test_iceberg.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_mars",
    size = "medium",
    srcs = ["tests/test_mars.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_metadata_provider",
    size = "small",
    srcs = ["tests/test_metadata_provider.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_object_gc",
    size = "large",
    srcs = ["tests/test_object_gc.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_operators",
    size = "medium",
    srcs = ["tests/test_operators.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_op_runtime_metrics",
    size = "medium",
    srcs = ["tests/test_op_runtime_metrics.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_optimize",
    size = "medium",
    srcs = ["tests/test_optimize.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_partitioning",
    size = "medium",
    srcs = ["tests/test_partitioning.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_progress_bar",
    size = "small",
    srcs = ["tests/test_progress_bar.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_random_access",
    size = "small",
    srcs = ["tests/test_random_access.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_randomize_block_order",
    size = "small",
    srcs = ["tests/test_randomize_block_order.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_raydp",
    size = "medium",
    srcs = ["tests/test_raydp.py"],
    tags = [
        "data_integration",
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_size_estimation",
    size = "medium",
    srcs = ["tests/test_size_estimation.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_sort",
    size = "enormous",
    srcs = ["tests/test_sort.py"],
    tags = [
        "data_non_parallel",
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_split",
    size = "large",
    srcs = ["tests/test_split.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_stats",
    size = "medium",
    srcs = ["tests/test_stats.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_streaming_executor",
    size = "medium",
    srcs = ["tests/test_streaming_executor.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_resource_manager",
    size = "medium",
    srcs = ["tests/test_resource_manager.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_streaming_executor_errored_blocks",
    size = "medium",
    srcs = ["tests/test_streaming_executor_errored_blocks.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_streaming_integration",
    size = "medium",
    srcs = ["tests/test_streaming_integration.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_transform_pyarrow",
    size = "small",
    srcs = ["tests/test_transform_pyarrow.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_util",
    size = "small",
    srcs = ["tests/test_util.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_union",
    size = "small",
    srcs = ["tests/test_union.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_video",
    size = "small",
    srcs = ["tests/test_video.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_webdataset",
    size = "medium",
    srcs = ["tests/test_webdataset.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_zip",
    size = "small",
    srcs = ["tests/test_zip.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_backpressure_policies",
    size = "medium",
    srcs = ["tests/test_backpressure_policies.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_backpressure_e2e",
    size = "large",
    srcs = ["tests/test_backpressure_e2e.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_bundle_queue",
    size = "small",
    srcs = ["tests/test_bundle_queue.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_autoscaler",
    size = "small",
    srcs = ["tests/test_autoscaler.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_clickhouse",
    size = "small",
    srcs = ["tests/test_clickhouse.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_lance",
    size = "small",
    srcs = ["tests/test_lance.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_logical_plan",
    size = "small",
    srcs = ["tests/test_logical_plan.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_delta_sharing",
    size = "small",
    srcs = ["tests/test_delta_sharing.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_context",
    size = "small",
    srcs = ["tests/test_context.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_expression_evaluator",
    size = "small",
    srcs = ["tests/test_expression_evaluator.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_block_boundaries",
    size = "small",
    srcs = ["tests/test_block_boundaries.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_telemetry",
    size = "small",
    srcs = ["tests/test_telemetry.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)

py_test(
    name = "test_import",
    size = "small",
    srcs = ["tests/test_import.py"],
    tags = [
        "exclusive",
        "team:data",
    ],
    deps = [
        ":conftest",
        "//:ray_lib",
    ],
)
