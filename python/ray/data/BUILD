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
    name = "test_dataset_formats",
    size = "medium",
    srcs = ["tests/test_dataset_formats.py"],
    data = glob(["tests/image-folder/**/*"]),
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_dataset_sql",
    size = "small",
    srcs = ["tests/test_dataset_sql.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_mongo_dataset",
    size = "large",
    srcs = ["tests/test_mongo_dataset.py"],
    tags = ["team:data", "exclusive", "dataset_integration"],
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
    name = "test_dataset_all_to_all",
    size = "large",
    srcs = ["tests/test_dataset_all_to_all.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_dataset_binary",
    size = "small",
    srcs = ["tests/test_dataset_binary.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_dataset_consumption",
    size = "large",
    srcs = ["tests/test_dataset_consumption.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_dataset_csv",
    size = "medium",
    srcs = ["tests/test_dataset_csv.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_dataset_ecosystem",
    size = "small",
    srcs = ["tests/test_dataset_ecosystem.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_dataset_image",
    size = "small",
    srcs = ["tests/test_dataset_image.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_dataset_iterator",
    size = "small",
    srcs = ["tests/test_dataset_iterator.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_dataset_json",
    size = "small",
    srcs = ["tests/test_dataset_json.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_dataset_logger",
    size = "small",
    srcs = ["tests/test_dataset_logger.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_dataset_map",
    size = "large",
    srcs = ["tests/test_dataset_map.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_dataset_numpy",
    size = "small",
    srcs = ["tests/test_dataset_numpy.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_dataset_pandas",
    size = "small",
    srcs = ["tests/test_dataset_pandas.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_dataset_parquet",
    size = "medium",
    srcs = ["tests/test_dataset_parquet.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_dataset_pipeline",
    size = "medium",
    srcs = ["tests/test_dataset_pipeline.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_dataset_tensor",
    size = "small",
    srcs = ["tests/test_dataset_tensor.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_dataset_text",
    size = "small",
    srcs = ["tests/test_dataset_text.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_dataset_tf",
    size = "small",
    srcs = ["tests/test_dataset_tf.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_dataset_tfrecords",
    size = "small",
    srcs = ["tests/test_dataset_tfrecords.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)

py_test(
    name = "test_dataset_torch",
    size = "small",
    srcs = ["tests/test_dataset_torch.py"],
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
    size = "small",
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
    size = "small",
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
    name = "test_raydp_dataset",
    size = "medium",
    srcs = ["tests/test_raydp_dataset.py"],
    tags = ["team:data", "exclusive", "dataset_integration"],
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
    name = "test_dataset_webdataset",
    size = "medium",
    srcs = ["tests/test_dataset_webdataset.py"],
    tags = ["team:data", "exclusive"],
    deps = ["//:ray_lib", ":conftest"],
)
