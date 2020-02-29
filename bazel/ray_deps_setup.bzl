load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository", "new_git_repository")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")

def auto_http_archive(*, name=None, url=None, urls=None,
                      build_file=None, build_file_content=None, strip_prefix=True, **kwargs):
    """
    Allows intelligent choice of mirrors based on the given URL for the download.

    name and url must be provided.

    If urls         == None , mirrors are automatically chosen.
    If build_file   == True , it is auto-deduced.
    If strip_prefix == True , it is auto-deduced.
    """
    mirror_prefixes = [
        "https://mirror.bazel.build/",
    ]
    hosts_preferred_before_mirrors = [
        "github.com",
    ]

    if name != None and build_file == True:
        build_file = "@//%s:%s" % ("bazel", "BUILD." + name)

    if url != None:
        if urls == None:
            delim = "://"
            (i, j) = (url.find(delim), 0)
            if i < 0:
                i = 0
            else:
                if not url[:i].isalpha(): fail("invalid url scheme")
                j = i + len(delim)
            url_except_scheme = url[j:]
            urls = [(mirror_prefix + url[j:] if len(mirror_prefix) > 0 else url)
                    for mirror_prefix in mirror_prefixes
                    if len(mirror_prefix) == 0 or not url[j:].startswith(mirror_prefix)]
            url_host = url_except_scheme.split("/")[0]
            if url_host in hosts_preferred_before_mirrors:
                urls.insert(0, url)
            else:
                urls.append(url)
        else:
            print("No implicit mirrors used because urls were explicitly provided")

    if strip_prefix == True:
        i = url.rfind("/")
        strip_prefix = url[i + 1:] if i >= 0 else ""
        i = strip_prefix.rfind(".")
        if i >= 0:
            strip_prefix = strip_prefix[:i]  # Handle single suffixes
        i = strip_prefix.rfind(".")
        if i >= 0 and strip_prefix.endswith(".tar"):
            strip_prefix = strip_prefix[:i]  # Handle double-suffixes (like .tar.*)

    return http_archive(name=name, url=url, urls=urls, build_file=build_file,
                        build_file_content=build_file_content, strip_prefix=strip_prefix, **kwargs)

def github_repository(*, name=None, remote=None, commit=None, tag=None,
                      branch=None, build_file=None, build_file_content=None,
                      sha256=None, archive_suffix=".tar.gz", shallow_since=None,
                      strip_prefix=True, url=None, urls=None, path=None, **kwargs):
    """
    Conveniently chooses between archive, git, etc. GitHub repositories.
    Prefer archives, as they're smaller and faster due to the lack of history.

    One of {commit, tag, branch} must also be provided (as usual).

    sha256 should be omitted (or None) when the archive hash is unknown, then
    updated ASAP to allow caching & avoid repeated downloads on every build.

    If remote       == None , it is an error.
    If name         == None , it is auto-deduced, but this is NOT recommended.
    If build_file   == True , it is auto-deduced.
    If strip_prefix == True , it is auto-deduced.
    If url          == None , it is auto-deduced, unless urls is provided.
    If sha256       != False, uses archive download (recommended; fast).
    If sha256       == False, uses git clone (NOT recommended; slow).
    If path         != None , local repository is assumed at the given path.
    """
    GIT_SUFFIX = ".git"

    treeish = commit or tag or branch
    if path == None and not treeish: fail("Missing commit, tag, or branch argument")
    if path == None and remote == None: fail("Missing remote argument")

    if path == None and remote.endswith(GIT_SUFFIX):
        remote_no_suffix = remote[:len(remote) - len(GIT_SUFFIX)]
    else:
        remote_no_suffix = remote
    project = remote_no_suffix.split("//", 1)[1].split("/")[2] if remote_no_suffix else None

    if project != None and name == None:
        name = project.replace("-", "_")
    if project != None and strip_prefix == True:
        strip_prefix = "%s-%s" % (project, treeish)
    if url == None and urls == None:
        url = "%s/archive/%s%s" % (remote_no_suffix, treeish, archive_suffix)
    if name != None and build_file == True:
        build_file = "@//%s:%s" % ("bazel", "BUILD." + name)

    if path != None:
        if build_file or build_file_content:
            native.new_local_repository(name=name, path=path,
                                        build_file=build_file,
                                        build_file_content=build_file_content,
                                        **kwargs)
        else:
            native.local_repository(name=name, path=path, **kwargs)
    elif sha256 == False:
        if build_file or build_file_content:
            new_git_repository(name=name, remote=remote, build_file=build_file,
                               commit=commit, tag=tag, branch=branch,
                               shallow_since=shallow_since,
                               build_file_content=build_file_content,
                               strip_prefix=strip_prefix, **kwargs)
        else:
            git_repository(name=name, remote=remote, strip_prefix=strip_prefix,
                           commit=commit, tag=tag, branch=branch,
                           shallow_since=shallow_since, **kwargs)
    else:
        auto_http_archive(name=name, url=url, urls=urls, sha256=sha256,
                          build_file=build_file, strip_prefix=strip_prefix,
                          build_file_content=build_file_content, **kwargs)

def ray_deps_setup():
    github_repository(
        name = "redis",
        build_file = True,
        tag = "5.0.3",
        remote = "https://github.com/antirez/redis",
        sha256 = "7084e8bd9e5dedf2dbb2a1e1d862d0c46e66cc0872654bdc677f4470d28d84c5",
        patches = [
            "//thirdparty/patches:hiredis-async-include-dict.patch",
            "//thirdparty/patches:hiredis-casts.patch",
            "//thirdparty/patches:hiredis-connect-rename.patch",
            "//thirdparty/patches:hiredis-windows-sigpipe.patch",
            "//thirdparty/patches:hiredis-windows-sockets.patch",
            "//thirdparty/patches:hiredis-windows-strerror.patch",
            "//thirdparty/patches:hiredis-windows-poll.patch",
            "//thirdparty/patches:redis-windows-poll.patch",
        ],
    )

    http_file(
        name = "win-redis-bin",
        sha256 = "6fac443543244c803311de5883b714a7ae3c4fa0594cad51d75b24c4ef45b353",
        urls = ["https://github.com/tporadowski/redis/releases/download/v4.0.14.2/Redis-x64-4.0.14.2.zip"],
    )

    http_file(
        name = "redis-src",
        sha256 = "7084e8bd9e5dedf2dbb2a1e1d862d0c46e66cc0872654bdc677f4470d28d84c5",
        urls = ["https://github.com/antirez/redis/archive/5.0.3.tar.bz2"],
    )

    github_repository(
        name = "rules_jvm_external",
        tag = "2.10",
        remote = "https://github.com/bazelbuild/rules_jvm_external",
        sha256 = "5c1b22eab26807d5286ada7392d796cbc8425d3ef9a57d114b79c5f8ef8aca7c",
    )

    github_repository(
        name = "bazel_common",
        commit = "f1115e0f777f08c3cdb115526c4e663005bec69b",
        remote = "https://github.com/google/bazel-common",
        sha256 = "50dea89af2e1334e18742f18c91c860446de8d1596947fe87e3cdb0d27b6f8f3",
    )

    github_repository(
        name = "com_github_checkstyle_java",
        commit = "ef367030d1433877a3360bbfceca18a5d0791bdd",
        remote = "https://github.com/ray-project/checkstyle_java",
        sha256 = "847d391156d7dcc9424e6a8ba06ff23ea2914c725b18d92028074b2ed8de3da9",
    )

    auto_http_archive(
        # This rule is used by @com_github_nelhage_rules_boost and
        # declaring it here allows us to avoid patching the latter.
        name = "boost",
        build_file = "@com_github_nelhage_rules_boost//:BUILD.boost",
        sha256 = "d73a8da01e8bf8c7eda40b4c84915071a8c8a0df4a6734537ddde4a8580524ee",
        url = "https://dl.bintray.com/boostorg/release/1.71.0/source/boost_1_71_0.tar.bz2",
        patches = [
            "//thirdparty/patches:boost-exception-no_warn_typeid_evaluated.patch",
        ],
    )

    github_repository(
        name = "com_github_nelhage_rules_boost",
        # If you update the Boost version, remember to update the 'boost' rule.
        commit = "5b53112431ef916381d6969f114727cc4f83960b",
        remote = "https://github.com/nelhage/rules_boost",
        sha256 = "32080749fdb8e4015815694a5c7d009f479e5f6a4da443d262bd7f28b8bd1b55",
        patches = [
            "//thirdparty/patches:rules_boost-undefine-boost_fallthrough.patch",
            "//thirdparty/patches:rules_boost-windows-linkopts.patch",
        ],
    )

    github_repository(
        name = "com_github_google_flatbuffers",
        commit = "63d51afd1196336a7d1f56a988091ef05deb1c62",
        remote = "https://github.com/google/flatbuffers",
        sha256 = "3f469032571d324eabea88d7014c05fec8565a5877dbe49b2a52d8d1a0f18e63",
    )

    github_repository(
        name = "com_google_googletest",
        commit = "3306848f697568aacf4bcca330f6bdd5ce671899",
        remote = "https://github.com/google/googletest",
        sha256 = "79ae337dab8e9ee6bd97a9f7134929bb1ddc7f83be9a564295b895865efe7dba",
    )

    github_repository(
        name = "com_github_gflags_gflags",
        commit = "e171aa2d15ed9eb17054558e0b3a6a413bb01067",
        remote = "https://github.com/gflags/gflags",
        sha256 = "b20f58e7f210ceb0e768eb1476073d0748af9b19dfbbf53f4fd16e3fb49c5ac8",
    )

    github_repository(
        name = "com_github_google_glog",
        commit = "925858d9969d8ee22aabc3635af00a37891f4e25",
        remote = "https://github.com/google/glog",
        sha256 = "fb86eca661497ac6f9ce2a106782a30215801bb8a7c8724c6ec38af05a90acf3",
        patches = [
            "//thirdparty/patches:glog-stack-trace.patch",
        ],
    )

    github_repository(
        name = "plasma",
        build_file = True,
        commit = "86f34aa07e611787d9cc98c6a33b0a0a536dce57",
        remote = "https://github.com/apache/arrow",
        sha256 = "6b5f55d10681a3938bbf8f07eee52c4eb6e761da6ba27490f55ccb89ce645ac8",
        patches = [
            "//thirdparty/patches:arrow-headers-unused.patch",
            "//thirdparty/patches:arrow-windows-export.patch",
            "//thirdparty/patches:arrow-windows-poll.patch",
            "//thirdparty/patches:arrow-windows-sigpipe.patch",
            "//thirdparty/patches:arrow-windows-socket.patch",
            "//thirdparty/patches:arrow-windows-dlmalloc.patch",
        ],
    )

    github_repository(
        name = "cython",
        build_file = True,
        commit = "49414dbc7ddc2ca2979d6dbe1e44714b10d72e7e",
        remote = "https://github.com/cython/cython",
        sha256 = "0b697ac90d1e46842c7cbbf5f4a1bde5b7b41037c611167417115337e3756eaa",
    )

    github_repository(
        name = "io_opencensus_cpp",
        commit = "3aa11f20dd610cb8d2f7c62e58d1e69196aadf11",
        remote = "https://github.com/census-instrumentation/opencensus-cpp",
        sha256 = "a0b4e2d3c4479cc343c003f0c31f48e9e05461cb232815e348fc0358bfa8bb79",
    )

    # OpenCensus depends on Abseil so we have to explicitly pull it in.
    # This is how diamond dependencies are prevented.
    github_repository(
        name = "com_google_absl",
        commit = "aa844899c937bde5d2b24f276b59997e5b668bde",
        remote = "https://github.com/abseil/abseil-cpp",
        sha256 = "327a3883d24cf5d81954b8b8713867ecf2289092c7a39a9dc25a9947cf5b8b78",
    )

    # OpenCensus depends on jupp0r/prometheus-cpp
    github_repository(
        name = "com_github_jupp0r_prometheus_cpp",
        commit = "60eaa4ea47b16751a8e8740b05fe70914c68a480",
        remote = "https://github.com/jupp0r/prometheus-cpp",
        sha256 = "ec825b802487ac18b0d98e2e8b7961487b12562f8f82e424521d0a891d9e1373",
        patches = [
            # https://github.com/jupp0r/prometheus-cpp/pull/225
            "//thirdparty/patches:prometheus-windows-zlib.patch",
            "//thirdparty/patches:prometheus-windows-pollfd.patch",
        ]
    )

    github_repository(
        name = "com_github_grpc_grpc",
        # NOTE: If you update this, also update @boringssl's hash.
        commit = "4790ab6d97e634a1ede983be393f3bb3c132b2f7",
        remote = "https://github.com/grpc/grpc",
        sha256 = "df83bd8a08975870b8b254c34afbecc94c51a55198e6e3a5aab61d62f40b7274",
        patches = [
            "//thirdparty/patches:grpc-command-quoting.patch",
            "//thirdparty/patches:grpc-cython-copts.patch",
        ],
    )

    github_repository(
        # This rule is used by @com_github_grpc_grpc, and using a GitHub mirror
        # provides a deterministic archive hash for caching. Explanation here:
        # https://github.com/grpc/grpc/blob/4790ab6d97e634a1ede983be393f3bb3c132b2f7/bazel/grpc_deps.bzl#L102
        name = "boringssl",
        # Ensure this matches the commit used by grpc's bazel/grpc_deps.bzl
        commit = "83da28a68f32023fd3b95a8ae94991a07b1f6c62",
        remote = "https://github.com/google/boringssl",
        sha256 = "781fa39693ec2984c71213cd633e9f6589eaaed75e3a9ac413237edec96fd3b9",
    )

    github_repository(
        name = "rules_proto_grpc",
        commit = "a74fef39c5fe636580083545f76d1eab74f6450d",
        remote = "https://github.com/rules-proto-grpc/rules_proto_grpc",
        sha256 = "2f6606151ec042e23396f07de9e7dcf6ca9a5db1d2b09f0cc93a7fc7f4008d1b",
    )
