filegroup(
  name = "example_pkg",
  data = [
    "ray_pkg.yaml",
  ] + glob([
    "my_pkg/**/*.py",
  ]),
  visibility = ["//python/ray/tests:__pkg__"],
)
