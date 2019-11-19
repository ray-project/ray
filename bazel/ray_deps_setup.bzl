load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository", "new_git_repository")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def github_repository(**kwargs):
    """
    Conveniently chooses between ZIP, git, etc. GitHub repositories.
    We prefer ZIP downloads, since they're smaller and faster
    due to the lack of history.

    The remote parameter is required.
    One of {commit, tag, branch} is also required.
    The name parameter is optional but recommended.
    Set sha256 = True if the ZIP archive hash is unknown, then update it ASAP
    to allow caching & avoid repeated downloads on every build.

    If build_file is a string, it refers to a custom build file.
    If build_file is absent, the project is assumed to contain a build file.
    If build_file == True, then the build file name is auto-deduced.

    If path   is present , local repository is assumed.
    If sha256 == <string>, obtains ZIP download with the given hash (fast)
    If sha256 == True    , obtains ZIP download without knowing the hash (fast)
    If sha256 is absent  , obtains clone via git (slow and NOT recommended)

    strip_prefix is automatic by default.
    Set strip_prefix=<string> (or False) to override.

    Some common parameters (like shallow_since)
    are automatically stripped when not applicable.
    Others are passed through directly.
    """

    # Extract well-known parameters
    remote = kwargs.pop("remote", None)  # required
    name = kwargs.get("name", None)  # recommended
    commit = kwargs.pop("commit", None)  # recommended
    tag = kwargs.pop("tag", None)
    branch = kwargs.pop("branch", None)
    build_file = kwargs.pop("build_file", None)
    sha256 = kwargs.pop("sha256", None)  # recommended (for archive)
    shallow_since = kwargs.pop("shallow_since", None)  # recommended (for git)
    strip_prefix = kwargs.pop("strip_prefix", None)  # avoid, unless necessary
    url = kwargs.pop("url", None)  # avoid, unless necessary

    # Validate arguments
    treeish = commit or tag or branch
    if treeish == None: fail("Missing commit, tag, or branch argument")
    if remote == None: fail("Missing remote argument")

    GIT_SUFFIX = ".git"  # We need to strip this
    if remote.endswith(GIT_SUFFIX):
        remote = remote[:len(remote) - len(GIT_SUFFIX)]
    project = remote.split("//", 1)[1].split("/")[2]

    if name == None:
        name = project.replace("-", "_")
        kwargs.setdefault("name", name)

    if url == None: url = "%s/archive/%s.zip" % (remote, treeish)

    if build_file == True:
        # Auto-generate build file name
        kwargs.setdefault(
            "build_file",
            "@//%s:%s" % ("bazel", "BUILD." + name))

    if "path" in kwargs:
        native.local_repository(**kwargs)
    else:
        if not sha256:
            # git clone
            if commit != None: kwargs.setdefault("commit", commit)
            if tag != None: kwargs.setdefault("tag", tag)
            if branch != None: kwargs.setdefault("branch", branch)
            kwargs.setdefault("shallow_since", shallow_since)
            kwargs.setdefault("remote", remote)
            if "build_file" in kwargs:
                new_git_repository(**kwargs)
            else:
                git_repository(**kwargs)
        else:
            if strip_prefix in (True, None):
                kwargs.setdefault("strip_prefix", "%s-%s" % (project, treeish))
            if sha256 != True: kwargs.setdefault("sha256", sha256)
            # Archive download
            kwargs.setdefault("url", url)
            http_archive(**kwargs)

def ray_deps_setup():
    github_repository(
        name = "rules_jvm_external",
        tag = "2.10",
        remote = "https://github.com/bazelbuild/rules_jvm_external",
        sha256 = "1bbf2e48d07686707dd85357e9a94da775e1dbd7c464272b3664283c9c716d26",
    )

    github_repository(
        name = "bazel_common",
        commit = "f1115e0f777f08c3cdb115526c4e663005bec69b",
        remote = "https://github.com/google/bazel-common",
        sha256 = "1e05a4791cc3470d3ecf7edb556f796b1d340359f1c4d293f175d4d0946cf84c",
    )

    github_repository(
        name = "bazel_skylib",
        tag = "0.6.0",
        remote = "https://github.com/bazelbuild/bazel-skylib",
        sha256 = "54ee22e5b9f0dd2b42eb8a6c1878dee592cfe8eb33223a7dbbc583a383f6ee1a",
    )

    github_repository(
        name = "com_github_checkstyle_java",
        commit = "ef367030d1433877a3360bbfceca18a5d0791bdd",
        remote = "https://github.com/ray-project/checkstyle_java",
        sha256 = "2fc33ec804011a03106e76ae77d7f1b09091b0f830f8e2a0408f079a032ed716",
    )

    github_repository(
        name = "com_github_nelhage_rules_boost",
        commit = "5171b9724fbb39c5fdad37b9ca9b544e8858d8ac",
        remote = "https://github.com/ray-project/rules_boost",
        sha256 = "14fa5cb327a3df811aa8713bbb7c5a63a89286868e7ec874c4a335829bf9c018",
    )

    github_repository(
        name = "com_github_google_flatbuffers",
        commit = "63d51afd1196336a7d1f56a988091ef05deb1c62",
        remote = "https://github.com/google/flatbuffers",
        sha256 = "dd87be0acf932c9b0d9b5d7bb49aec23e1c98bbd3327254bd90cb4af198f9332",
    )

    github_repository(
        name = "com_google_googletest",
        commit = "3306848f697568aacf4bcca330f6bdd5ce671899",
        remote = "https://github.com/google/googletest",
        sha256 = "2625a1d301cd658514e297002170c2fc83a87beb0f495f943601df17d966511d",
    )

    github_repository(
        name = "com_github_gflags_gflags",
        commit = "e171aa2d15ed9eb17054558e0b3a6a413bb01067",
        remote = "https://github.com/gflags/gflags",
        sha256 = "da72f0dce8e3422d0ab2fea8d03a63a64227b0376b3558fd9762e88de73b780b",
    )

    github_repository(
        name = "com_github_google_glog",
        build_file = "@//bazel:BUILD.glog",
        commit = "96a2f23dca4cc7180821ca5f32e526314395d26a",
        remote = "https://github.com/google/glog",
        sha256 = "6281aa4eeecb9e932d7091f99872e7b26fa6aacece49c15ce5b14af2b7ec050f",
    )

    github_repository(
        name = "plasma",
        build_file = True,
        commit = "86f34aa07e611787d9cc98c6a33b0a0a536dce57",
        remote = "https://github.com/apache/arrow",
        sha256 = "4f1956e74188fa15078c8ad560bbc298624320d2aafd21fe7a2511afee7ea841",
    )

    github_repository(
        name = "cython",
        build_file = True,
        commit = "49414dbc7ddc2ca2979d6dbe1e44714b10d72e7e",
        remote = "https://github.com/cython/cython",
        sha256 = "aaee5dec23165ee10c189d8b40f19861e2c6929c015cee3d2b4e56d8a1bdc422",
    )

    github_repository(
        name = "io_opencensus_cpp",
        commit = "3aa11f20dd610cb8d2f7c62e58d1e69196aadf11",
        remote = "https://github.com/census-instrumentation/opencensus-cpp",
        sha256 = "92eef77c44d01e8472f68a2f1329919a1bb59317a4bb1e4d76081ab5c13a56d6",
    )

    # OpenCensus depends on Abseil so we have to explicitly pull it in.
    # This is how diamond dependencies are prevented.
    github_repository(
        name = "com_google_absl",
        commit = "aa844899c937bde5d2b24f276b59997e5b668bde",
        remote = "https://github.com/abseil/abseil-cpp",
        sha256 = "f1a959a2144f0482b9bd61e67a9897df02234fff6edf82294579a4276f2f4b97",
    )

    # OpenCensus depends on jupp0r/prometheus-cpp
    github_repository(
        name = "com_github_jupp0r_prometheus_cpp",
        commit = "5c45ba7ddc0585d765a43d136764dd2a542bd495",
        remote = "https://github.com/ray-project/prometheus-cpp",
        # TODO(qwang): We should use the repository of `jupp0r` here when this PR
        # `https://github.com/jupp0r/prometheus-cpp/pull/225` getting merged.
        sha256 = "c80293276166d405188b1af62cd11178fbcec0f1a8ab0dbece19d4bdc79d45e7",
    )

    github_repository(
        name = "com_github_grpc_grpc",
        commit = "93e8830070e9afcbaa992c75817009ee3f4b63a0",
        remote = "https://github.com/grpc/grpc",
        sha256 = "b391a327429279f6f29b9ae7e5317cd80d5e9d49cc100e6d682221af73d984a6",
        patches = [
            "//thirdparty/patches:grpc-cython-copts.patch",
        ],
    )

    github_repository(
        name = "rules_proto_grpc",
        commit = "a74fef39c5fe636580083545f76d1eab74f6450d",
        remote = "https://github.com/rules-proto-grpc/rules_proto_grpc",
        sha256 = "53561ecacaebe58916dfdb962d889a56394d3fae6956e0bcd63c4353f813284a",
    )
