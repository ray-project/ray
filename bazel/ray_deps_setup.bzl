load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository", "new_git_repository")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def ray_deps_setup():
    RULES_JVM_EXTERNAL_TAG = "1.2"

    RULES_JVM_EXTERNAL_SHA = "e5c68b87f750309a79f59c2b69ead5c3221ffa54ff9496306937bfa1c9c8c86b"

    http_archive(
        name = "rules_jvm_external",
        sha256 = RULES_JVM_EXTERNAL_SHA,
        strip_prefix = "rules_jvm_external-%s" % RULES_JVM_EXTERNAL_TAG,
        url = "https://github.com/bazelbuild/rules_jvm_external/archive/%s.zip" % RULES_JVM_EXTERNAL_TAG,
    )

    http_archive(
        name = "bazel_common",
        strip_prefix = "bazel-common-f1115e0f777f08c3cdb115526c4e663005bec69b",
        url = "https://github.com/google/bazel-common/archive/f1115e0f777f08c3cdb115526c4e663005bec69b.zip",
    )

    BAZEL_SKYLIB_TAG = "0.6.0"

    http_archive(
        name = "bazel_skylib",
        strip_prefix = "bazel-skylib-%s" % BAZEL_SKYLIB_TAG,
        url = "https://github.com/bazelbuild/bazel-skylib/archive/%s.tar.gz" % BAZEL_SKYLIB_TAG,
    )

    git_repository(
        name = "com_github_checkstyle_java",
        commit = "85f37871ca03b9d3fee63c69c8107f167e24e77b",
        remote = "https://github.com/ruifangChen/checkstyle_java",
    )

    git_repository(
        name = "com_github_nelhage_rules_boost",
        commit = "5171b9724fbb39c5fdad37b9ca9b544e8858d8ac",
        remote = "https://github.com/ray-project/rules_boost",
    )

    git_repository(
        name = "com_github_google_flatbuffers",
        commit = "63d51afd1196336a7d1f56a988091ef05deb1c62",
        remote = "https://github.com/google/flatbuffers.git",
    )

    git_repository(
        name = "com_google_googletest",
        commit = "3306848f697568aacf4bcca330f6bdd5ce671899",
        remote = "https://github.com/google/googletest",
    )

    git_repository(
        name = "com_github_gflags_gflags",
        remote = "https://github.com/gflags/gflags.git",
        tag = "v2.2.2",
    )

    new_git_repository(
        name = "com_github_google_glog",
        build_file = "@//bazel:BUILD.glog",
        commit = "5c576f78c49b28d89b23fbb1fc80f54c879ec02e",
        remote = "https://github.com/google/glog",
    )

    new_git_repository(
        name = "plasma",
        build_file = "@//bazel:BUILD.plasma",
        commit = "f976629a54f5518f6285a311c45c5957281b1ee7",
        remote = "https://github.com/apache/arrow",
    )

    new_git_repository(
        name = "cython",
        build_file = "@//bazel:BUILD.cython",
        commit = "49414dbc7ddc2ca2979d6dbe1e44714b10d72e7e",
        remote = "https://github.com/cython/cython",
    )

    http_archive(
        name = "io_opencensus_cpp",
        strip_prefix = "opencensus-cpp-3aa11f20dd610cb8d2f7c62e58d1e69196aadf11",
        urls = ["https://github.com/census-instrumentation/opencensus-cpp/archive/3aa11f20dd610cb8d2f7c62e58d1e69196aadf11.zip"],
    )

    # OpenCensus depends on Abseil so we have to explicitly pull it in.
    # This is how diamond dependencies are prevented.
    git_repository(
        name = "com_google_absl",
        commit = "5b65c4af5107176555b23a638e5947686410ac1f",
        remote = "https://github.com/abseil/abseil-cpp.git",
    )

    # OpenCensus depends on jupp0r/prometheus-cpp
    http_archive(
        name = "com_github_jupp0r_prometheus_cpp",
        strip_prefix = "prometheus-cpp-master",

        # TODO(qwang): We should use the repository of `jupp0r` here when this PR
        # `https://github.com/jupp0r/prometheus-cpp/pull/225` getting merged.
        urls = ["https://github.com/jovany-wang/prometheus-cpp/archive/master.zip"],
    )

    http_archive(
        name = "com_github_grpc_grpc",
        urls = [
            "https://github.com/grpc/grpc/archive/76a381869413834692b8ed305fbe923c0f9c4472.tar.gz",
        ],
        strip_prefix = "grpc-76a381869413834692b8ed305fbe923c0f9c4472",
    )

    http_archive(
        name = "build_stack_rules_proto",
        urls = ["https://github.com/stackb/rules_proto/archive/b93b544f851fdcd3fc5c3d47aee3b7ca158a8841.tar.gz"],
        sha256 = "c62f0b442e82a6152fcd5b1c0b7c4028233a9e314078952b6b04253421d56d61",
        strip_prefix = "rules_proto-b93b544f851fdcd3fc5c3d47aee3b7ca158a8841",
    )
