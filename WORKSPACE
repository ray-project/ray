workspace(name = "com_github_ray_project_ray")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")

load("//bazel:ray_deps_setup.bzl", "ray_deps_setup")
ray_deps_setup()



load("@bazel_skylib//:workspace.bzl", "bazel_skylib_workspace")

bazel_skylib_workspace()

load("//java:dependencies.bzl", "gen_java_deps")

gen_java_deps()

load("//streaming/java:dependencies.bzl", "gen_streaming_java_deps")

gen_streaming_java_deps()

load("@com_github_checkstyle_java//:repo.bzl", "checkstyle_deps")

checkstyle_deps()

load("@com_github_nelhage_rules_boost//:boost/boost.bzl", "boost_deps")

boost_deps()

load("@com_github_jupp0r_prometheus_cpp//bazel:repositories.bzl", "prometheus_cpp_repositories")

prometheus_cpp_repositories()

load("//bazel/grpc_python:python_configure.bzl", "python_configure")

python_configure(name = "local_config_python")

load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")

grpc_deps()

load("@rules_proto_grpc//:repositories.bzl", "rules_proto_grpc_toolchains", "rules_proto_grpc_repos")

rules_proto_grpc_toolchains()
#rules_proto_grpc_repos()

#load("@rules_proto_grpc//python:repositories.bzl", rules_proto_grpc_python_repos="python_repos")
#rules_proto_grpc_python_repos()

#load("@rules_proto_grpc//cpp:repositories.bzl", rules_proto_grpc_cpp_repos="cpp_repos")

#rules_proto_grpc_cpp_repos()



# This needs to be run after grpc_deps() in ray_deps_build_all() to make
# sure all the packages loaded by grpc_deps() are available. However a
# load() statement cannot be in a function so we put it here.


load("@com_github_grpc_grpc//bazel:grpc_extra_deps.bzl", "grpc_extra_deps")
grpc_extra_deps()
