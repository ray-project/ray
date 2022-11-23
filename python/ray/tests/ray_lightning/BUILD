py_test(
 name = "simple_example",
 size = "small",
 srcs = ["simple_example.py"],
 deps = ["//:ray_lib"],
 tags = ["team:ml", "compat", "exclusive"],
)

py_test(
 name = "simple_tune",
 size="small",
 srcs = ["simple_tune.py"],
 deps = ["//:ray_lib"],
 tags = ["team:ml", "compat", "exclusive"]
)

py_test(
 name = "simple_tune_distributed",
 size="small",
 srcs = ["simple_tune.py"],
 main = "simple_tune.py",
 args = ["--distributed"],
 deps = ["//:ray_lib"],
 tags = ["team:ml", "compat", "exclusive"]
)