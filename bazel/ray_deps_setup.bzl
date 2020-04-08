load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository", "new_git_repository")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")

def urlsplit(url):
    """ Splits a URL like "https://example.com/a/b?c=d&e#f" into a tuple:
        ("https", ["example", "com"], ["a", "b"], ["c=d", "e"], "f")
    A trailing slash will result in a correspondingly empty final path component.
    """
    split_on_anchor = url.split("#", 1)
    split_on_query = split_on_anchor[0].split("?", 1)
    split_on_scheme = split_on_query[0].split("://", 1)
    if len(split_on_scheme) <= 1:  # Scheme is optional
        split_on_scheme = [None] + split_on_scheme[:1]
    split_on_path = split_on_scheme[1].split("/")
    return {
        "scheme": split_on_scheme[0],
        "netloc": split_on_path[0].split("."),
        "path": split_on_path[1:],
        "query": split_on_query[1].split("&") if len(split_on_query) > 1 else None,
        "fragment": split_on_anchor[1] if len(split_on_anchor) > 1 else None,
    }

def auto_http_archive(*, name=None, url=None, urls=True,
                      build_file=None, build_file_content=None,
                      strip_prefix=True, **kwargs):
    """ Intelligently choose mirrors based on the given URL for the download.

    Either url or urls is required.

    If name         == None , it is auto-deduced, but this is NOT recommended.
    If urls         == True , mirrors are automatically chosen.
    If build_file   == True , it is auto-deduced.
    If strip_prefix == True , it is auto-deduced.
    """
    DOUBLE_SUFFIXES_LOWERCASE = [("tar", "bz2"), ("tar", "gz"), ("tar", "xz")]
    mirror_prefixes = ["https://mirror.bazel.build/"]

    canonical_url = url if url != None else urls[0]
    url_parts = urlsplit(canonical_url)
    url_except_scheme = (canonical_url.replace(url_parts["scheme"] + "://", "")
                         if url_parts["scheme"] != None else canonical_url)
    url_path_parts = url_parts["path"]
    url_filename = url_path_parts[-1]
    url_filename_parts = (url_filename.rsplit(".", 2)
                          if (tuple(url_filename.lower().rsplit(".", 2)[-2:])
                              in DOUBLE_SUFFIXES_LOWERCASE)
                          else url_filename.rsplit(".", 1))
    is_github = url_parts["netloc"] == ["github", "com"]

    if name == None:  # Deduce "com_github_user_project_name" from "https://github.com/user/project-name/..."
        name = "_".join(url_parts["netloc"][::-1] + url_path_parts[:2]).replace("-", "_")

    if build_file == True:
        build_file = "@//%s:%s" % ("bazel", "BUILD." + name)

    if urls == True:
        prefer_url_over_mirrors = is_github
        urls = [mirror_prefix + url_except_scheme
                for mirror_prefix in mirror_prefixes
                if not canonical_url.startswith(mirror_prefix)]
        urls.insert(0 if prefer_url_over_mirrors else len(urls), canonical_url)
    else:
        print("No implicit mirrors used because urls were explicitly provided")

    if strip_prefix == True:
        strip_prefix = (url_path_parts[1] + "-" + url_filename_parts[0]
                        if is_github and url_path_parts[2:3] == ["archive"]
                        else url_filename_parts[0])

    return http_archive(name=name, url=url, urls=urls, build_file=build_file,
                        build_file_content=build_file_content,
                        strip_prefix=strip_prefix, **kwargs)

def ray_deps_setup():
    auto_http_archive(
        name = "com_github_antirez_redis",
        build_file = "//bazel:BUILD.redis",
        url = "https://github.com/antirez/redis/archive/5.0.3.tar.gz",
        sha256 = "7084e8bd9e5dedf2dbb2a1e1d862d0c46e66cc0872654bdc677f4470d28d84c5",
        patches = [
            "//thirdparty/patches:hiredis-connect-rename.patch",
            "//thirdparty/patches:hiredis-windows-sigpipe.patch",
            "//thirdparty/patches:hiredis-windows-sockets.patch",
            "//thirdparty/patches:hiredis-windows-strerror.patch",
        ],
    )

    http_file(
        name = "com_github_tporadowski_redis_bin",
        sha256 = "6fac443543244c803311de5883b714a7ae3c4fa0594cad51d75b24c4ef45b353",
        urls = ["https://github.com/tporadowski/redis/releases/download/v4.0.14.2/Redis-x64-4.0.14.2.zip"],
    )

    auto_http_archive(
        name = "rules_jvm_external",
        url = "https://github.com/bazelbuild/rules_jvm_external/archive/2.10.tar.gz",
        sha256 = "5c1b22eab26807d5286ada7392d796cbc8425d3ef9a57d114b79c5f8ef8aca7c",
    )

    auto_http_archive(
        name = "bazel_common",
        url = "https://github.com/google/bazel-common/archive/084aadd3b854cad5d5e754a7e7d958ac531e6801.tar.gz",
        sha256 = "a6e372118bc961b182a3a86344c0385b6b509882929c6b12dc03bb5084c775d5",
    )

    auto_http_archive(
        name = "com_github_checkstyle_java",
        url = "https://github.com/ray-project/checkstyle_java/archive/ef367030d1433877a3360bbfceca18a5d0791bdd.tar.gz",
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

    auto_http_archive(
        name = "com_github_nelhage_rules_boost",
        # If you update the Boost version, remember to update the 'boost' rule.
        url = "https://github.com/nelhage/rules_boost/archive/5b53112431ef916381d6969f114727cc4f83960b.tar.gz",
        sha256 = "32080749fdb8e4015815694a5c7d009f479e5f6a4da443d262bd7f28b8bd1b55",
        patches = [
            "//thirdparty/patches:rules_boost-undefine-boost_fallthrough.patch",
            "//thirdparty/patches:rules_boost-windows-linkopts.patch",
        ],
    )

    auto_http_archive(
        name = "com_github_google_flatbuffers",
        url = "https://github.com/google/flatbuffers/archive/63d51afd1196336a7d1f56a988091ef05deb1c62.tar.gz",
        sha256 = "3f469032571d324eabea88d7014c05fec8565a5877dbe49b2a52d8d1a0f18e63",
    )

    auto_http_archive(
        name = "com_google_googletest",
        url = "https://github.com/google/googletest/archive/3306848f697568aacf4bcca330f6bdd5ce671899.tar.gz",
        sha256 = "79ae337dab8e9ee6bd97a9f7134929bb1ddc7f83be9a564295b895865efe7dba",
    )

    auto_http_archive(
        name = "com_github_gflags_gflags",
        url = "https://github.com/gflags/gflags/archive/e171aa2d15ed9eb17054558e0b3a6a413bb01067.tar.gz",
        sha256 = "b20f58e7f210ceb0e768eb1476073d0748af9b19dfbbf53f4fd16e3fb49c5ac8",
    )

    auto_http_archive(
        name = "com_github_google_glog",
        url = "https://github.com/google/glog/archive/925858d9969d8ee22aabc3635af00a37891f4e25.tar.gz",
        sha256 = "fb86eca661497ac6f9ce2a106782a30215801bb8a7c8724c6ec38af05a90acf3",
        patches = [
            "//thirdparty/patches:glog-stack-trace.patch",
        ],
    )

    auto_http_archive(
        name = "plasma",
        build_file = True,
        url = "https://github.com/apache/arrow/archive/af45b9212156980f55c399e2e88b4e19b4bb8ec1.tar.gz",
        sha256 = "2f0aaa50053792aa274b402f2530e63c1542085021cfef83beee9281412c12f6",
        patches = [
            "//thirdparty/patches:arrow-headers-unused.patch",
            "//thirdparty/patches:arrow-windows-export.patch",
            "//thirdparty/patches:arrow-windows-nonstdc.patch",
            "//thirdparty/patches:arrow-windows-sigpipe.patch",
            "//thirdparty/patches:arrow-windows-socket.patch",
            "//thirdparty/patches:arrow-windows-dlmalloc.patch",
            "//thirdparty/patches:arrow-windows-tcp.patch",
        ],
    )

    auto_http_archive(
        name = "cython",
        build_file = True,
        url = "https://github.com/cython/cython/archive/26cb654dcf4ed1b1858daf16b39fd13406b1ac64.tar.gz",
        sha256 = "d21e155ac9a455831f81608bb06620e4a1d75012a630faf11f4c25ad10cfc9bb",
    )

    auto_http_archive(
        name = "io_opencensus_cpp",
        url = "https://github.com/census-instrumentation/opencensus-cpp/archive/3aa11f20dd610cb8d2f7c62e58d1e69196aadf11.tar.gz",
        sha256 = "a0b4e2d3c4479cc343c003f0c31f48e9e05461cb232815e348fc0358bfa8bb79",
    )

    # OpenCensus depends on Abseil so we have to explicitly pull it in.
    # This is how diamond dependencies are prevented.
    auto_http_archive(
        name = "com_google_absl",
        url = "https://github.com/abseil/abseil-cpp/archive/aa844899c937bde5d2b24f276b59997e5b668bde.tar.gz",
        sha256 = "327a3883d24cf5d81954b8b8713867ecf2289092c7a39a9dc25a9947cf5b8b78",
    )

    # OpenCensus depends on jupp0r/prometheus-cpp
    auto_http_archive(
        name = "com_github_jupp0r_prometheus_cpp",
        url = "https://github.com/jupp0r/prometheus-cpp/archive/60eaa4ea47b16751a8e8740b05fe70914c68a480.tar.gz",
        sha256 = "ec825b802487ac18b0d98e2e8b7961487b12562f8f82e424521d0a891d9e1373",
        patches = [
            # https://github.com/jupp0r/prometheus-cpp/pull/225
            "//thirdparty/patches:prometheus-windows-zlib.patch",
            "//thirdparty/patches:prometheus-windows-pollfd.patch",
        ]
    )

    auto_http_archive(
        name = "com_github_grpc_grpc",
        # NOTE: If you update this, also update @boringssl's hash.
        url = "https://github.com/grpc/grpc/archive/4790ab6d97e634a1ede983be393f3bb3c132b2f7.tar.gz",
        sha256 = "df83bd8a08975870b8b254c34afbecc94c51a55198e6e3a5aab61d62f40b7274",
        patches = [
            "//thirdparty/patches:grpc-command-quoting.patch",
            "//thirdparty/patches:grpc-cython-copts.patch",
        ],
    )

    auto_http_archive(
        # This rule is used by @com_github_grpc_grpc, and using a GitHub mirror
        # provides a deterministic archive hash for caching. Explanation here:
        # https://github.com/grpc/grpc/blob/4790ab6d97e634a1ede983be393f3bb3c132b2f7/bazel/grpc_deps.bzl#L102
        name = "boringssl",
        # Ensure this matches the commit used by grpc's bazel/grpc_deps.bzl
        url = "https://github.com/google/boringssl/archive/83da28a68f32023fd3b95a8ae94991a07b1f6c62.tar.gz",
        sha256 = "781fa39693ec2984c71213cd633e9f6589eaaed75e3a9ac413237edec96fd3b9",
    )

    auto_http_archive(
        name = "rules_proto_grpc",
        url = "https://github.com/rules-proto-grpc/rules_proto_grpc/archive/a74fef39c5fe636580083545f76d1eab74f6450d.tar.gz",
        sha256 = "2f6606151ec042e23396f07de9e7dcf6ca9a5db1d2b09f0cc93a7fc7f4008d1b",
    )

    auto_http_archive(
        name = "msgpack",
        build_file = True,
        url = "https://github.com/msgpack/msgpack-c/archive/8085ab8721090a447cf98bb802d1406ad7afe420.tar.gz",
        sha256 = "83c37c9ad926bbee68d564d9f53c6cbb057c1f755c264043ddd87d89e36d15bb",
        patches = [
            "//thirdparty/patches:msgpack-windows-iovec.patch",
        ],
    )
