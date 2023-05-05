# --------------------------------------------------------------------
# Tests from the python/ray/data/tests directory.
# Covers all tests starting with `test_`.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------
load("//bazel:python.bzl", "py_test_module_list")

py_library(
    name = "conftest",
    srcs = ["tests/conftest.py"],
    deps = ["//python/ray/tests:conftest"],
)

py_test_module_list(
    files = glob(["tests/block_batching/test_*.py"]),
    size = "medium",
    tags = ["team:ml", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test_module_list(
    files = glob(["tests/preprocessors/test_*.py"]),
    size = "small",
    tags = ["team:ml", "exclusive", "ray_air"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_formats",
    size = "medium",
    srcs = ["tests/test_formats.py"],
    data = glob(["tests/image-folder/**/*"]),
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_strict_mode",
    size = "small",
    srcs = ["tests/test_strict_mode.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_nonstrict_mode",
    size = "small",
    srcs = ["tests/test_nonstrict_mode.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_sql",
    size = "small",
    srcs = ["tests/test_sql.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_mongo",
    size = "large",
    srcs = ["tests/test_mongo.py"],
    tags = ["team:data", "exclusive", "data_integration"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_actor_pool_map_operator",
    size = "small",
    srcs = ["tests/test_actor_pool_map_operator.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_arrow_serialization",
    size = "small",
    srcs = ["tests/test_arrow_serialization.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_auto_parallelism",
    size = "small",
    srcs = ["tests/test_auto_parallelism.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_batcher",
    size = "small",
    srcs = ["tests/test_batcher.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_block_batching",
    size = "medium",
    srcs = ["tests/block_batching/test_block_batching.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_bulk_executor",
    size = "small",
    srcs = ["tests/test_bulk_executor.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_context_propagation",
    size = "small",
    srcs = ["tests/test_context_propagation.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_all_to_all",
    size = "large",
    srcs = ["tests/test_all_to_all.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_binary",
    size = "small",
    srcs = ["tests/test_binary.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_consumption",
    size = "large",
    srcs = ["tests/test_consumption.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_csv",
    size = "medium",
    srcs = ["tests/test_csv.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_ecosystem",
    size = "small",
    srcs = ["tests/test_ecosystem.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_image",
    size = "small",
    srcs = ["tests/test_image.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_iterator",
    size = "small",
    srcs = ["tests/test_iterator.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_json",
    size = "medium",
    srcs = ["tests/test_json.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_logger",
    size = "small",
    srcs = ["tests/test_logger.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_map",
    size = "large",
    srcs = ["tests/test_map.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_numpy",
    size = "small",
    srcs = ["tests/test_numpy.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_pandas",
    size = "small",
    srcs = ["tests/test_pandas.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_parquet",
    size = "medium",
    srcs = ["tests/test_parquet.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_pipeline",
    size = "medium",
    srcs = ["tests/test_pipeline.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_tensor",
    size = "small",
    srcs = ["tests/test_tensor.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_text",
    size = "small",
    srcs = ["tests/test_text.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_tf",
    size = "small",
    srcs = ["tests/test_tf.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_tfrecords",
    size = "small",
    srcs = ["tests/test_tfrecords.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_torch",
    size = "small",
    srcs = ["tests/test_torch.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_dynamic_block_split",
    size = "medium",
    srcs = ["tests/test_dynamic_block_split.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_execution_optimizer",
    size = "medium",
    srcs = ["tests/test_execution_optimizer.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_executor_resource_management",
    size = "small",
    srcs = ["tests/test_executor_resource_management.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_huggingface",
    size = "small",
    srcs = ["tests/test_huggingface.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_mars",
    size = "medium",
    srcs = ["tests/test_mars.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_metadata_provider",
    size = "small",
    srcs = ["tests/test_metadata_provider.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_object_gc",
    size = "large",
    srcs = ["tests/test_object_gc.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_operators",
    size = "medium",
    srcs = ["tests/test_operators.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_optimize",
    size = "medium",
    srcs = ["tests/test_optimize.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)


py_test(
    name = "test_partitioning",
    size = "medium",
    srcs = ["tests/test_partitioning.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_pipeline_incremental_take",
    size = "small",
    srcs = ["tests/test_pipeline_incremental_take.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_pipeline_nohang",
    size = "medium",
    srcs = ["tests/test_pipeline_nohang.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_random_access",
    size = "small",
    srcs = ["tests/test_random_access.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_randomize_block_order",
    size = "small",
    srcs = ["tests/test_randomize_block_order.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_raydp",
    size = "medium",
    srcs = ["tests/test_raydp.py"],
    tags = ["team:data", "exclusive", "data_integration"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_size_estimation",
    size = "medium",
    srcs = ["tests/test_size_estimation.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_sort",
    size = "large",
    srcs = ["tests/test_sort.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_split",
    size = "medium",
    srcs = ["tests/test_split.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_stats",
    size = "small",
    srcs = ["tests/test_stats.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_streaming_executor",
    size = "small",
    srcs = ["tests/test_streaming_executor.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_streaming_integration",
    size = "medium",
    srcs = ["tests/test_streaming_integration.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_streaming_backpressure_edge_case",
    size = "medium",
    srcs = ["tests/test_streaming_backpressure_edge_case.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_transform_pyarrow",
    size = "small",
    srcs = ["tests/test_transform_pyarrow.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_util",
    size = "small",
    srcs = ["tests/test_util.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_webdataset",
    size = "medium",
    srcs = ["tests/test_webdataset.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)
