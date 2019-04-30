load("@com_github_ray_project_ray//java:dependencies.bzl", "gen_java_deps")
load("@com_github_nelhage_rules_boost//:boost/boost.bzl", "boost_deps")
load("@com_github_jupp0r_prometheus_cpp//:repositories.bzl", "prometheus_cpp_repositories")
load("@com_github_ray_project_ray//bazel:python_configure.bzl", "python_configure")
load("@com_github_checkstyle_java//:repo.bzl", "checkstyle_deps")

def ray_deps_build_all():
  gen_java_deps()
  checkstyle_deps()
  boost_deps()
  prometheus_cpp_repositories()
  python_configure(name = "local_config_python")
