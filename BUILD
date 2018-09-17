# Bazel build
# C/C++ documentation: https://docs.bazel.build/versions/master/be/c-cpp.html
#
# Notes:
# * All external deps referenced as THIRDPARTY:name. Need to be implemented.
# * Credis not implemented.
# * Flatbuffers not implemented
#   * Need to add flatbuffer_cc_library
#     See # https://github.com/tensorflow/tensorflow/blob/3ec29c57b728f5f3b8f80e84f3189f70f86536ea/third_party/flatbuffers/build_defs.bzl#L107
#   * Need to add flatbuffer_py_library

package(
    features = [
        "-layering_check",
        "-parse_headers",
    ],
    default_visibility=["//visibility:private"],
)

cc_binary(
    name="raylet",
    srcs=["src/ray/raylet/main.cc"],
    deps=[
        ":common",
        ":ray_util",
        ":raylet_lib",
    ],
)

cc_binary(
    name="raylet_monitor",
    srcs=[
        "src/ray/raylet/monitor.cc",
        "src/ray/raylet/monitor.h",
        "src/ray/raylet/monitor_main.cc",
    ],
    deps=[
        ":gcs",
        ":ray_util",
    ],
)

cc_library(
    name="raylet_lib",
    srcs=glob(
        [
            "src/ray/raylet/*.cc",
        ],
        exclude=[
            "src/ray/raylet/mock_gcs_client.cc",
            "src/ray/raylet/monitor_main.cc",
            "src/ray/raylet/*_test.cc",
        ],
    ),
    hdrs=glob([
        "src/ray/raylet/*.h",
    ],),
    deps=[
        ":common",
        ":common_fbs",
        ":gcs",
        ":gcs_fbs",
        ":local_scheduler_fbs",
        ":node_manager_fbs",
        ":object_manager",
        ":ray_common",
        ":ray_util",
        ":sha256",
        "THIRDPARTY:boost",
    ],
)

cc_test(
    name="lineage_cache_test",
    srcs=["src/ray/raylet/lineage_cache_test.cc"],
    deps=[
        "THIRDPARTY:gmock",
        "THIRDPARTY:gtest",
        ":node_manager_fbs",
        ":raylet_lib",
    ],
)

cc_test(
    name="reconstruction_policy_test",
    srcs=["src/ray/raylet/reconstruction_policy_test.cc"],
    deps=[
        "THIRDPARTY:gmock",
        "THIRDPARTY:gtest",
        ":node_manager_fbs",
        ":object_manager",
        ":raylet_lib",
        "THIRDPARTY:boost",
    ],
)

cc_test(
    name="worker_pool_test",
    srcs=["src/ray/raylet/worker_pool_test.cc"],
    deps=[
        "THIRDPARTY:gmock",
        "THIRDPARTY:gtest",
        ":raylet_lib",
    ],
)

cc_test(
    name="logging_test",
    srcs=["src/ray/util/logging_test.cc"],
    deps=[
        "THIRDPARTY:gmock",
        "THIRDPARTY:gtest",
        ":ray_util",
    ],
)

cc_test(
    name="task_dependency_manager_test",
    srcs=["src/ray/raylet/task_dependency_manager_test.cc"],
    deps=[
        "THIRDPARTY:gmock",
        "THIRDPARTY:gtest",
        ":raylet_lib",
        "THIRDPARTY:boost",
    ],
)

cc_test(
    name="task_test",
    srcs=["src/ray/raylet/task_test.cc"],
    deps=[
        "THIRDPARTY:gmock",
        "THIRDPARTY:gtest",
        ":raylet_lib",
    ],
)

cc_library(
    name="sha256",
    srcs=[
        "src/common/thirdparty/sha256.c",
    ],
    hdrs=[
        "src/common/thirdparty/sha256.h",
    ],
    includes=["src/common/thirdparty"],
)

cc_library(
    name="common",
    srcs=glob([
        "src/common/*.cc",
        "src/common/state/*.cc",
    ]),
    hdrs=glob([
        "src/common/*.h",
        "src/common/state/*.h",
    ]),
    includes=[
        "src",
        "src/common",
    ],
    deps=[
        "THIRDPARTY:ae",
        ":common_fbs",
        "THIRDPARTY:hiredis",
        ":ray_util",
        ":sha256",
        "THIRDPARTY:arrow",
        "THIRDPARTY:plasma",
    ],
)

cc_library(
    name="object_manager",
    srcs=glob([
        "src/ray/object_manager/*.cc",
    ]),
    hdrs=glob([
        "src/ray/object_manager/*.h",
    ]),
    includes=[
        "src",
    ],
    deps=[
        ":common",
        ":gcs",
        ":object_manager_fbs",
        ":ray_common",
        ":ray_util",
        "THIRDPARTY:plasma",
        "THIRDPARTY:boost",
    ],
)

cc_binary(
    name="object_manager_test",
    testonly=1,
    srcs=["src/ray/object_manager/test/object_manager_test.cc"],
    deps=[
        "THIRDPARTY:gtest",
        ":object_manager",
    ],
)

cc_binary(
    name="object_manager_stress_test",
    testonly=1,
    srcs=["src/ray/object_manager/test/object_manager_stress_test.cc"],
    deps=[
        "THIRDPARTY:gtest",
        ":object_manager",
    ],
)

cc_binary(
    name="db_tests",
    testonly=1,
    srcs=["src/common/test/db_tests.cc"],
    deps=[
        ":common",
        ":test_common",
    ],
)

cc_binary(
    name="io_tests",
    testonly=1,
    srcs=["src/common/test/io_tests.cc"],
    deps=[
        ":common",
        ":test_common",
    ],
)

cc_binary(
    name="task_tests",
    testonly=1,
    srcs=["src/common/test/task_tests.cc"],
    deps=[
        ":common",
        ":test_common",
    ],
)

cc_binary(
    name="task_table_tests",
    testonly=1,
    srcs=["src/common/test/task_table_tests.cc"],
    deps=[
        "THIRDPARTY:ae",
        ":common",
        ":test_common",
    ],
)

cc_binary(
    name="libray_redis_module.so",
    srcs=[
        "src/common/redis_module/ray_redis_module.cc",
        "src/common/redis_module/redis_string.h",
        "src/common/redis_module/redismodule.h",
    ],
    linkshared=True,
    deps=[
        ":common",
        ":common_fbs",
        ":gcs_fbs",
        ":ray_util",
    ],
)

cc_binary(
    name="object_table_tests",
    testonly=1,
    srcs=["src/common/test/object_table_tests.cc"],
    deps=[
        ":common",
        ":test_common",
    ],
)

cc_binary(
    name="redis_tests",
    testonly=1,
    srcs=["src/common/test/redis_tests.cc"],
    deps=[
        ":common",
        ":test_common",
    ],
)

cc_library(
    name="test_common",
    testonly=1,
    hdrs=[
        "src/common/test/example_task.h",
        "src/common/test/test_common.h",
        "src/common/thirdparty/greatest.h",
    ],
    includes=["src/common/thirdparty"],
    deps=[
        ":common",
        "THIRDPARTY:hiredis",
    ],
)

cc_library(
    name="ray_util",
    srcs=glob(
        [
            "src/ray/*.cc",
            "src/ray/util/*.cc",
        ],
        exclude=[
            "src/ray/util/logging_test.cc",
        ],
    ),
    hdrs=glob([
        "src/ray/*.h",
        "src/ray/util/*.h",
    ]) + [
        "src/common/common.h",
        "src/common/state/ray_config.h",
    ],
    includes=[
        "src",
    ],
    deps=[
        ":sha256",
        "THIRDPARTY:arrow",
        "THIRDPARTY:plasma",
        "THIRDPARTY:boost",
    ],
)

cc_library(
    name="ray_common",
    srcs=["src/ray/common/client_connection.cc"],
    hdrs=["src/ray/common/client_connection.h"],
    includes=[
        "src/ray/gcs/format",
    ],
    deps=[
        ":common",
        ":gcs_fbs",
        ":node_manager_fbs",
        ":ray_util",
        "THIRDPARTY:boost",
    ],
)

cc_library(
    name="gcs",
    srcs=glob(
        [
            "src/ray/gcs/*.cc",
        ],
        exclude=[
            "src/ray/gcs/*_test.cc",
        ],
    ),
    hdrs=glob([
        "src/ray/gcs/*.h",
        "src/ray/gcs/format/*.h",
    ]),
    includes=[
        "src/ray/gcs/format",
    ],
    deps=[
        ":common",
        ":gcs_fbs",
        "THIRDPARTY:hiredis",
        ":node_manager_fbs",
        ":ray_util",
        "THIRDPARTY:plasma",
        "THIRDPARTY:boost",
    ],
)

sh_binary(
    name="redis_server",
    srcs=[
        "test/redis_server.sh",
    ],
    data=[
        ":libray_redis_module.so",
        "THIRDPARTY:redis_server",
    ],
)

cc_binary(
    name="gcs_client_test",
    testonly=1,
    srcs=["src/ray/gcs/client_test.cc"],
    deps=[
        ":gcs",
        "THIRDPARTY:gtest",
        "THIRDPARTY:hiredis",
        "THIRDPARTY:gunit_main",
        "THIRDPARTY:boost",
    ],
)

cc_binary(
    name="asio_test",
    testonly=1,
    srcs=["src/ray/gcs/asio_test.cc"],
    deps=[
        ":gcs",
        "THIRDPARTY:gtest",
        ":ray_util",
        "THIRDPARTY:gunit_main",
        "THIRDPARTY:boost",
    ],
)

FLATC_ARGS = [
    "--gen-object-api",
    "--gen-mutable",
    "--scoped-enums",
]

flatbuffer_cc_library(
    name="gcs_fbs",
    srcs=["src/ray/gcs/format/gcs.fbs"],
    flatc_args=FLATC_ARGS,
    out_prefix="src/ray/gcs/format/",
)

flatbuffer_py_library(
    name="gcs_fbs_py",
    srcs=["src/ray/gcs/format/gcs.fbs"],
)

flatbuffer_cc_library(
    name="common_fbs",
    srcs=["src/common/format/common.fbs"],
    flatc_args=[
        "--gen-object-api",
        "--scoped-enums",
    ],
    out_prefix="src/common/format/",
)

flatbuffer_py_library(
    name="common_fbs_py",
    srcs=["src/common/format/common.fbs"],
)

flatbuffer_cc_library(
    name="local_scheduler_fbs",
    srcs=["src/local_scheduler/format/local_scheduler.fbs"],
    flatc_args=FLATC_ARGS,
    out_prefix="src/local_scheduler/format/",
)

flatbuffer_cc_library(
    name="node_manager_fbs",
    srcs=["src/ray/raylet/format/node_manager.fbs"],
    flatc_args=FLATC_ARGS,
    include_paths=["src/ray/gcs/format"],
    includes=[":gcs_fbs_includes"],
    out_prefix="src/ray/raylet/format/",
)

flatbuffer_cc_library(
    name="object_manager_fbs",
    srcs=["src/ray/object_manager/format/object_manager.fbs"],
    flatc_args=FLATC_ARGS,
    out_prefix="src/ray/object_manager/format/",
)

sh_test(
    name="tests_with_server",
    srcs=["test/tests_with_server.sh"],
    data=[
        ":asio_test",
        ":db_tests",
        ":gcs_client_test",
        ":io_tests",
        ":object_manager_stress_test",
        ":object_manager_test",
        ":object_table_tests",
        ":plasma_client_tests",
        ":plasma_manager",
        ":plasma_manager_tests",
        ":redis_server",
        ":redis_tests",
        ":task_table_tests",
        ":task_tests",
        "THIRDPARTY:plasma_store",
        "THIRDPARTY:redis_cli",
    ],
    deps=[
    ],
)

cc_binary(
    name="local_scheduler",
    deps=[":local_scheduler_lib"],
)

LOCAL_SCHEDULER_DEPS = [
    ":common",
    ":gcs",
    ":local_scheduler_fbs",
    ":node_manager_fbs",
    ":raylet_lib",
    "THIRDPARTY:plasma",
]

LOCAL_SCHEDULER_SRCS = glob([
    "src/local_scheduler/*.cc",
],)

LOCAL_SCHEDULER_HDRS = glob([
    "src/local_scheduler/*.h",
],)

cc_library(
    name="local_scheduler_lib",
    srcs=LOCAL_SCHEDULER_SRCS,
    hdrs=LOCAL_SCHEDULER_HDRS,
    includes=["src/local_scheduler"],
    deps=LOCAL_SCHEDULER_DEPS,
)

cc_library(
    name="python_extension_lib",
    srcs=glob([
        "src/common/lib/python/*.cc",
    ]),
    hdrs=glob([
        "src/common/lib/python/*.h",
    ]),
    deps=[
        ":common",
        ":raylet_lib",
        "THIRDPARTY:numpy_headers",
        "THIRDPARTY:python_headers",
    ],
)

cc_binary(
    name="liblocal_scheduler_library_python.so",
    srcs=["src/local_scheduler/lib/python/local_scheduler_extension.cc"],
    includes=["src/common/lib/python"],
    deps=[
        ":common",
        ":local_scheduler_lib",
        ":python_extension_lib",
        "THIRDPARTY:python_headers",
    ],
)

cc_library(
    name="local_scheduler_test_lib",
    testonly=1,
    srcs=LOCAL_SCHEDULER_SRCS,
    hdrs=LOCAL_SCHEDULER_HDRS,
    defines=["LOCAL_SCHEDULER_TEST"],
    includes=["src/local_scheduler"],
    deps=LOCAL_SCHEDULER_DEPS,
)

cc_binary(
    name="global_scheduler",
    srcs=glob([
        "src/global_scheduler/*.cc",
        "src/global_scheduler/*.h",
    ]),
    deps=[
        ":common",
        ":gcs",
    ],
)

cc_binary(
    name="local_scheduler_tests",
    testonly=1,
    srcs=[
        "src/local_scheduler/test/local_scheduler_tests.cc",
    ],
    defines=["LOCAL_SCHEDULER_TEST"],
    deps=[
        ":common",
        ":local_scheduler_test_lib",
        ":test_common",
    ],
)

cc_binary(
    name="plasma_client_tests",
    testonly=1,
    srcs=["src/plasma/test/client_tests.cc"],
    deps=[
        ":test_common",
        "THIRDPARTY:plasma",
    ],
)

cc_binary(
    name="plasma_manager_tests",
    testonly=1,
    srcs=["src/plasma/test/manager_tests.cc"],
    deps=[
        ":common",
        ":plasma_manager_lib",
        ":plasma_manager_nomain",
        ":test_common",
        "THIRDPARTY:plasma",
    ],
)

plasma_manager_deps = [
    ":common",
    ":gcs",
    ":plasma_manager_lib",
    "THIRDPARTY:plasma",
    "THIRDPARTY:plasma_common_fbs",
    "THIRDPARTY:plasma_plasma_fbs",
]

cc_library(
    name="plasma_manager_nomain",
    testonly=1,
    srcs=["src/plasma/plasma_manager.cc"],
    defines=["PLASMA_TEST"],
    includes=["src/plasma"],
    deps=plasma_manager_deps,
)

cc_binary(
    name="plasma_manager",
    srcs=["src/plasma/plasma_manager.cc"],
    includes=["src/plasma"],
    deps=plasma_manager_deps,
)

cc_library(
    name="plasma_manager_lib",
    textual_hdrs=[
        "src/plasma/plasma_manager.h",
        "src/plasma/protocol.h",
    ],
)
