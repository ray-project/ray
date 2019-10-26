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
	sha256 = "1e05a4791cc3470d3ecf7edb556f796b1d340359f1c4d293f175d4d0946cf84c",
    )

    BAZEL_SKYLIB_TAG = "0.6.0"

    http_archive(
        name = "bazel_skylib",
        strip_prefix = "bazel-skylib-%s" % BAZEL_SKYLIB_TAG,
        url = "https://github.com/bazelbuild/bazel-skylib/archive/%s.tar.gz" % BAZEL_SKYLIB_TAG,
	sha256 = "eb5c57e4c12e68c0c20bc774bfbc60a568e800d025557bc4ea022c6479acc867",
    )

    git_repository(
        name = "com_github_checkstyle_java",
        commit = "85f37871ca03b9d3fee63c69c8107f167e24e77b",
        remote = "https://github.com/ruifangChen/checkstyle_java",
	shallow_since = "1552542575 +0800",
    )

    git_repository(
        name = "com_github_nelhage_rules_boost",
        commit = "5171b9724fbb39c5fdad37b9ca9b544e8858d8ac",
        remote = "https://github.com/ray-project/rules_boost",
	shallow_since = "1556014830 +0800",
    )

    git_repository(
        name = "com_github_google_flatbuffers",
        commit = "63d51afd1196336a7d1f56a988091ef05deb1c62",
        remote = "https://github.com/google/flatbuffers.git",
	shallow_since = "1547755012 -0800",
    )

    git_repository(
        name = "com_google_googletest",
        commit = "3306848f697568aacf4bcca330f6bdd5ce671899",
        remote = "https://github.com/google/googletest",
	shallow_since = "1534270723 -0700",
    )

    git_repository(
        name = "com_github_gflags_gflags",
        remote = "https://github.com/gflags/gflags.git",
	commit = "e171aa2d15ed9eb17054558e0b3a6a413bb01067",
	shallow_since = "1541971260 +0000",
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
        commit = "141a213a54f4979ab0b94b94928739359a2ee9ad",
        remote = "https://github.com/apache/arrow",
    )

    new_git_repository(
        name = "cython",
        build_file = "@//bazel:BUILD.cython",
        commit = "49414dbc7ddc2ca2979d6dbe1e44714b10d72e7e",
        remote = "https://github.com/cython/cython",
	shallow_since = "1547888711 +0100",
    )

    http_archive(
        name = "io_opencensus_cpp",
        strip_prefix = "opencensus-cpp-3aa11f20dd610cb8d2f7c62e58d1e69196aadf11",
        urls = ["https://github.com/census-instrumentation/opencensus-cpp/archive/3aa11f20dd610cb8d2f7c62e58d1e69196aadf11.zip"],
	sha256 = "92eef77c44d01e8472f68a2f1329919a1bb59317a4bb1e4d76081ab5c13a56d6",
    )

    # OpenCensus depends on Abseil so we have to explicitly pull it in.
    # This is how diamond dependencies are prevented.
    git_repository(
        name = "com_google_absl",
        commit = "aa844899c937bde5d2b24f276b59997e5b668bde",
        remote = "https://github.com/abseil/abseil-cpp.git",
	shallow_since = "1565288385 -0400",
    )

    # OpenCensus depends on jupp0r/prometheus-cpp
    http_archive(
        name = "com_github_jupp0r_prometheus_cpp",
        strip_prefix = "prometheus-cpp-master",

        # TODO(qwang): We should use the repository of `jupp0r` here when this PR
        # `https://github.com/jupp0r/prometheus-cpp/pull/225` getting merged.
        urls = ["https://github.com/jovany-wang/prometheus-cpp/archive/master.zip"],
	sha256 = "d0c773da8af3db99c543dd0413f4427d835170eddfd517bfeba104236a8d2c07",
    )

    http_archive(
        name = "com_github_grpc_grpc",
        urls = [
            "https://github.com/grpc/grpc/archive/76a381869413834692b8ed305fbe923c0f9c4472.tar.gz",
        ],
        strip_prefix = "grpc-76a381869413834692b8ed305fbe923c0f9c4472",
	sha256 = "b5efbe086b9a00826a3f830094312e6d1647157b5a5e7954a8ac4179bce3de8b",
    )

    http_archive(
        name = "build_stack_rules_proto",
        urls = ["https://github.com/stackb/rules_proto/archive/b93b544f851fdcd3fc5c3d47aee3b7ca158a8841.tar.gz"],
        sha256 = "c62f0b442e82a6152fcd5b1c0b7c4028233a9e314078952b6b04253421d56d61",
        strip_prefix = "rules_proto-b93b544f851fdcd3fc5c3d47aee3b7ca158a8841",
    )
