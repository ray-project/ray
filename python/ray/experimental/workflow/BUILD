# --------------------------------------------------------------------
# Tests from the python/ray/experimental/workflow/tests directory.
# Covers all tests starting with `test_`.
# Please keep these sorted alphabetically.
# --------------------------------------------------------------------
load("//bazel:python.bzl", "py_test_module_list")

SRCS = [] + select({
    "@bazel_tools//src/conditions:windows": glob([
        # TODO(mehrdadn): This should be added for all platforms once resulting errors are fixed
        "**/conftest.py",
    ]),
    "//conditions:default": [],
})

py_test_module_list(
  files = [
      "tests/test_object_deref.py",
      "tests/test_storage.py",
      "tests/test_variable_mutable.py",
      "tests/test_large_intermediate.py",
      "tests/test_signature_check.py",
      "tests/test_basic_workflows_2.py",
  ],
  size = "small",
  extra_srcs = SRCS,
  tags = ["exclusive"],
  deps = ["//:ray_lib"],
)

py_test_module_list(
  files = [
      "tests/test_basic_workflows.py",
      "tests/test_recovery.py",
      "tests/test_lifetime.py",
      "tests/test_workflow_manager.py",
      "tests/test_virtual_actor.py",
  ],
  size = "medium",
  extra_srcs = SRCS,
  tags = ["exclusive"],
  deps = ["//:ray_lib"],
)
