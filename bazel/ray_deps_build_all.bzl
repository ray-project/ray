load("@bazel_skylib//:workspace.bzl", "bazel_skylib_workspace")
load("@com_github_ray_project_ray//java:dependencies.bzl", "gen_java_deps")
load("@com_github_nelhage_rules_boost//:boost/boost.bzl", "boost_deps")
load("@com_github_jupp0r_prometheus_cpp//bazel:repositories.bzl", "prometheus_cpp_repositories")
load("@com_github_grpc_grpc//third_party/py:python_configure.bzl", "python_configure")
load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")
load("@rules_proto_grpc//:repositories.bzl", "rules_proto_grpc_toolchains")
load("@com_github_johnynek_bazel_jar_jar//:jar_jar.bzl", "jar_jar_repositories")
load("@rules_foreign_cc//foreign_cc:repositories.bzl", "rules_foreign_cc_dependencies")
load("@rules_foreign_cc_thirdparty//openssl:openssl_setup.bzl", "openssl_setup")



def ray_deps_build_all():
  bazel_skylib_workspace()
  gen_java_deps()
  boost_deps()
  prometheus_cpp_repositories()
  python_configure(name = "local_config_python")
  grpc_deps()
  rules_proto_grpc_toolchains()
  jar_jar_repositories()
  rules_foreign_cc_dependencies()
  openssl_setup()
