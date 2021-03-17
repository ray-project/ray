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
        prefix_without_v = url_filename_parts[0]
        if prefix_without_v.startswith("v") and prefix_without_v[1:2].isdigit():
            # GitHub automatically strips a leading 'v' in version numbers
            prefix_without_v = prefix_without_v[1:]
        strip_prefix = (url_path_parts[1] + "-" + prefix_without_v
                        if is_github and url_path_parts[2:3] == ["archive"]
                        else url_filename_parts[0])

    return http_archive(name=name, url=url, urls=urls, build_file=build_file,
                        build_file_content=build_file_content,
                        strip_prefix=strip_prefix, **kwargs)

def ray_deps_setup():

    auto_http_archive(
        name = "com_github_antirez_redis",
        build_file = "//bazel:BUILD.redis",
        url = "https://github.com/redis/redis/archive/6.0.10.tar.gz",
        sha256 = "900cb82227bac58242c9b7668e7113cd952253b256fe04bbdab1b78979cf255a",
        patches = [
            "//thirdparty/patches:redis-quiet.patch",
        ],
    )

    auto_http_archive(
        name = "com_github_redis_hiredis",
        build_file = "//bazel:BUILD.hiredis",
        url = "https://github.com/redis/hiredis/archive/392de5d7f97353485df1237872cb682842e8d83f.tar.gz",
        sha256 = "2101650d39a8f13293f263e9da242d2c6dee0cda08d343b2939ffe3d95cf3b8b",
        patches = [
            "//thirdparty/patches:hiredis-windows-msvc.patch",
        ],
    )

    auto_http_archive(
        name = "com_github_spdlog",
        build_file = "//bazel:BUILD.spdlog",
        urls = ["https://github.com/gabime/spdlog/archive/v1.7.0.zip"],
        sha256 = "c8f1e1103e0b148eb8832275d8e68036f2fdd3975a1199af0e844908c56f6ea5",
    )
    
    auto_http_archive(
        name = "com_github_tporadowski_redis_bin",
        build_file = "//bazel:BUILD.redis",
        strip_prefix = None,
        url = "https://github.com/tporadowski/redis/releases/download/v5.0.9/Redis-x64-5.0.9.zip",
      sha256 = "b09565b22b50c505a5faa86a7e40b6683afb22f3c17c5e6a5e35fc9b7c03f4c2",
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
        name = "bazel_skylib",
        strip_prefix = None,
        url = "https://github.com/bazelbuild/bazel-skylib/releases/download/1.0.2/bazel-skylib-1.0.2.tar.gz",
        sha256 = "97e70364e9249702246c0e9444bccdc4b847bed1eb03c5a3ece4f83dfe6abc44",
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
        url = "https://github.com/nelhage/rules_boost/archive/2613d04ab3d22dfc4543ea0a083d9adeaa0daf09.tar.gz",
        sha256 = "512f913240e026099d4ca4a98b1ce8048c99de77fdc8e8584e9e2539ee119ca2",
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
            "//thirdparty/patches:glog-log-pid-tid.patch",
            "//thirdparty/patches:glog-stack-trace.patch",
            "//thirdparty/patches:glog-suffix-log.patch",
            "//thirdparty/patches:glog-dump-stacktrack.patch",
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
        url = "https://github.com/census-instrumentation/opencensus-cpp/archive/b14a5c0dcc2da8a7fc438fab637845c73438b703.zip",
        sha256 = "6592e07672e7f7980687f6c1abda81974d8d379e273fea3b54b6c4d855489b9d",
        patches = [
            "//thirdparty/patches:opencensus-cpp-harvest-interval.patch",
            "//thirdparty/patches:opencensus-cpp-shutdown-api.patch",
        ]
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
            "//thirdparty/patches:prometheus-windows-headers.patch",
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
            "//thirdparty/patches:grpc-cython-copts.patch",
            "//thirdparty/patches:grpc-python.patch",
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

    http_archive(
        name = "io_opencensus_proto",
        strip_prefix = "opencensus-proto-0.3.0/src",
        urls = ["https://github.com/census-instrumentation/opencensus-proto/archive/v0.3.0.tar.gz"],
        sha256 = "b7e13f0b4259e80c3070b583c2f39e53153085a6918718b1c710caf7037572b0",
    )

    auto_http_archive(
        name = "rules_proto",
        sha256 = "d8992e6eeec276d49f1d4e63cfa05bbed6d4a26cfe6ca63c972827a0d141ea3b",
        strip_prefix = "rules_proto-cfdc2fa31879c0aebe31ce7702b1a9c8a4be02d2",
        url = "https://github.com/bazelbuild/rules_proto/archive/cfdc2fa31879c0aebe31ce7702b1a9c8a4be02d2.tar.gz",
    )

