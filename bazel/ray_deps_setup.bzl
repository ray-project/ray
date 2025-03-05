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

def auto_http_archive(
        *,
        name = None,
        url = None,
        urls = True,
        build_file = None,
        build_file_content = None,
        strip_prefix = True,
        **kwargs):
    """ Intelligently choose mirrors based on the given URL for the download.
    Either url or urls is required.
    If name         == None , it is auto-deduced, but this is NOT recommended.
    If urls         == True , mirrors are automatically chosen.
    If build_file   == True , it is auto-deduced.
    If strip_prefix == True , it is auto-deduced.
    """
    DOUBLE_SUFFIXES_LOWERCASE = [("tar", "bz2"), ("tar", "gz"), ("tar", "xz")]
    mirror_prefixes = ["https://mirror.bazel.build/", "https://storage.googleapis.com/bazel-mirror"]

    canonical_url = url if url != None else urls[0]
    url_parts = urlsplit(canonical_url)
    url_except_scheme = (canonical_url.replace(url_parts["scheme"] + "://", "") if url_parts["scheme"] != None else canonical_url)
    url_path_parts = url_parts["path"]
    url_filename = url_path_parts[-1]
    url_filename_parts = (url_filename.rsplit(".", 2) if (tuple(url_filename.lower().rsplit(".", 2)[-2:]) in
                                                          DOUBLE_SUFFIXES_LOWERCASE) else url_filename.rsplit(".", 1))
    is_github = url_parts["netloc"] == ["github", "com"]

    if name == None:  # Deduce "com_github_user_project_name" from "https://github.com/user/project-name/..."
        name = "_".join(url_parts["netloc"][::-1] + url_path_parts[:2]).replace("-", "_")

    # auto appending ray project namespace prefix for 3rd party library reusing.
    if build_file == True:
        build_file = "@com_github_ray_project_ray//%s:%s" % ("bazel", name + ".BUILD")

    if urls == True:
        prefer_url_over_mirrors = is_github
        urls = [
            mirror_prefix + url_except_scheme
            for mirror_prefix in mirror_prefixes
            if not canonical_url.startswith(mirror_prefix)
        ]
        urls.insert(0 if prefer_url_over_mirrors else len(urls), canonical_url)
    else:
        print("No implicit mirrors used for %s because urls were explicitly provided" % name)

    if strip_prefix == True:
        prefix_without_v = url_filename_parts[0]
        if prefix_without_v.startswith("v") and prefix_without_v[1:2].isdigit():
            # GitHub automatically strips a leading 'v' in version numbers
            prefix_without_v = prefix_without_v[1:]
        strip_prefix = (url_path_parts[1] + "-" + prefix_without_v if is_github and url_path_parts[2:3] == ["archive"] else url_filename_parts[0])

    return http_archive(
        name = name,
        url = url,
        urls = urls,
        build_file = build_file,
        build_file_content = build_file_content,
        strip_prefix = strip_prefix,
        **kwargs
    )

def ray_deps_setup():
    # Explicitly bring in protobuf dependency to work around
    # https://github.com/ray-project/ray/issues/14117
    # This is copied from grpc's bazel/grpc_deps.bzl
    #
    # Pinned grpc version: v23.4
    http_archive(
        name = "com_google_protobuf",
        sha256 = "76a33e2136f23971ce46c72fd697cd94dc9f73d56ab23b753c3e16854c90ddfd",
        strip_prefix = "protobuf-2c5fa078d8e86e5f4bd34e6f4c9ea9e8d7d4d44a",
        urls = [
            "https://github.com/protocolbuffers/protobuf/archive/2c5fa078d8e86e5f4bd34e6f4c9ea9e8d7d4d44a.tar.gz",
        ],
        patches = [
            "@com_github_grpc_grpc//third_party:protobuf.patch",
        ],
        patch_args = ["-p1"],
    )

    # NOTE(lingxuan.zlx): 3rd party dependencies could be accessed, so it suggests
    # all of http/git_repository should add prefix for patches defined in ray directory.
    auto_http_archive(
        name = "com_github_antirez_redis",
        build_file = "@com_github_ray_project_ray//bazel:redis.BUILD",
        patch_args = ["-p1"],
        url = "https://github.com/redis/redis/archive/refs/tags/7.2.3.tar.gz",
        sha256 = "afd656dbc18a886f9a1cc08a550bf5eb89de0d431e713eba3ae243391fb008a6",
        patches = [
            "@com_github_ray_project_ray//thirdparty/patches:redis-quiet.patch",
        ],
        workspace_file_content = 'workspace(name = "com_github_antirez_redis")',
    )

    auto_http_archive(
        name = "com_github_redis_hiredis",
        build_file = "@com_github_ray_project_ray//bazel:hiredis.BUILD",
        url = "https://github.com/redis/hiredis/archive/60e5075d4ac77424809f855ba3e398df7aacefe8.tar.gz",
        sha256 = "b6d6f799b7714d85316f9ebfb76a35a78744f42ea3b6774289d882d13a2f0383",
        patches = [
            "@com_github_ray_project_ray//thirdparty/patches:hiredis-windows-msvc.patch",
        ],
    )

    auto_http_archive(
        name = "com_github_spdlog",
        build_file = "@com_github_ray_project_ray//bazel:spdlog.BUILD",
        urls = ["https://github.com/gabime/spdlog/archive/v1.12.0.zip"],
        sha256 = "6174bf8885287422a6c6a0312eb8a30e8d22bcfcee7c48a6d02d1835d7769232",
        # spdlog rotation filename format conflict with ray, update the format.
        patches = [
            "@com_github_ray_project_ray//thirdparty/patches:spdlog-rotation-file-format.patch",
        ],
        patch_args = ["-p1"],
    )

    auto_http_archive(
        name = "com_github_tporadowski_redis_bin",
        build_file = "@com_github_ray_project_ray//bazel:redis.BUILD",
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

    http_archive(
        name = "bazel_skylib",
        sha256 = "9f38886a40548c6e96c106b752f242130ee11aaa068a56ba7e56f4511f33e4f2",
        urls = [
            "https://mirror.bazel.build/github.com/bazelbuild/bazel-skylib/releases/download/1.6.1/bazel-skylib-1.6.1.tar.gz",
            "https://github.com/bazelbuild/bazel-skylib/releases/download/1.6.1/bazel-skylib-1.6.1.tar.gz",
        ],
    )

    auto_http_archive(
        name = "com_github_nelhage_rules_boost",
        # If you update the Boost version, remember to update the 'boost' rule.
        url = "https://github.com/nelhage/rules_boost/archive/57c99395e15720e287471d79178d36a85b64d6f6.tar.gz",
        sha256 = "490d11425393eed068966a4990ead1ff07c658f823fd982fddac67006ccc44ab",
    )

    auto_http_archive(
        name = "com_github_google_flatbuffers",
        url = "https://github.com/google/flatbuffers/archive/63d51afd1196336a7d1f56a988091ef05deb1c62.tar.gz",
        sha256 = "3f469032571d324eabea88d7014c05fec8565a5877dbe49b2a52d8d1a0f18e63",
    )

    auto_http_archive(
        name = "com_google_googletest",
        url = "https://github.com/google/googletest/archive/refs/tags/v1.14.0.tar.gz",
        sha256 = "8ad598c73ad796e0d8280b082cebd82a630d73e73cd3c70057938a6501bba5d7",
    )

    auto_http_archive(
        name = "com_github_gflags_gflags",
        url = "https://github.com/gflags/gflags/archive/e171aa2d15ed9eb17054558e0b3a6a413bb01067.tar.gz",
        sha256 = "b20f58e7f210ceb0e768eb1476073d0748af9b19dfbbf53f4fd16e3fb49c5ac8",
    )

    auto_http_archive(
        name = "cython",
        build_file = True,
        url = "https://github.com/cython/cython/archive/refs/tags/3.0.12.tar.gz",
        sha256 = "a156fff948c2013f2c8c398612c018e2b52314fdf0228af8fbdb5585e13699c2",
    )

    auto_http_archive(
        name = "com_github_johnynek_bazel_jar_jar",
        url = "https://github.com/johnynek/bazel_jar_jar/archive/171f268569384c57c19474b04aebe574d85fde0d.tar.gz",
        sha256 = "97c5f862482a05f385bd8f9d28a9bbf684b0cf3fae93112ee96f3fb04d34b193",
    )

    auto_http_archive(
        name = "io_opencensus_cpp",
        url = "https://github.com/census-instrumentation/opencensus-cpp/archive/5e5f2632c84e2230fb7ccb8e336f603d2ec6aa1b.zip",
        sha256 = "1b88d6663f05c6a56c1604eb2afad22831d5f28a76f6fab8f37187f1e4ace425",
        patches = [
            "@com_github_ray_project_ray//thirdparty/patches:opencensus-cpp-harvest-interval.patch",
            "@com_github_ray_project_ray//thirdparty/patches:opencensus-cpp-shutdown-api.patch",
        ],
        patch_args = ["-p1"],
    )

    auto_http_archive(
        name = "io_opentelemetry_cpp",
        url = "https://github.com/open-telemetry/opentelemetry-cpp/archive/refs/tags/v1.19.0.zip",
        sha256 = "8ef0a63f4959d5dfc3d8190d62229ef018ce41eef36e1f3198312d47ab2de05a",
    )

    # OpenCensus depends on Abseil so we have to explicitly pull it in.
    # This is how diamond dependencies are prevented.
    #
    # TODO(owner): Upgrade abseil to latest version after protobuf updated, which requires to upgrade `rules_cc` first.
    auto_http_archive(
        name = "com_google_absl",
        sha256 = "987ce98f02eefbaf930d6e38ab16aa05737234d7afbab2d5c4ea7adbe50c28ed",
        strip_prefix = "abseil-cpp-20230802.1",
        urls = [
            "https://github.com/abseil/abseil-cpp/archive/refs/tags/20230802.1.tar.gz",
        ],
    )

    # OpenCensus depends on jupp0r/prometheus-cpp
    auto_http_archive(
        name = "com_github_jupp0r_prometheus_cpp",
        url = "https://github.com/jupp0r/prometheus-cpp/archive/60eaa4ea47b16751a8e8740b05fe70914c68a480.tar.gz",
        sha256 = "ec825b802487ac18b0d98e2e8b7961487b12562f8f82e424521d0a891d9e1373",
        patches = [
            "@com_github_ray_project_ray//thirdparty/patches:prometheus-windows-headers.patch",
            # https://github.com/jupp0r/prometheus-cpp/pull/225
            "@com_github_ray_project_ray//thirdparty/patches:prometheus-windows-zlib.patch",
            "@com_github_ray_project_ray//thirdparty/patches:prometheus-windows-pollfd.patch",
        ],
    )

    auto_http_archive(
        name = "com_github_grpc_grpc",
        # NOTE: If you update this, also update @boringssl's hash.
        url = "https://github.com/grpc/grpc/archive/refs/tags/v1.57.1.tar.gz",
        sha256 = "0762f809b9de845e6a7c809cabccad6aa4143479fd43b396611fe5a086c0aeeb",
        patches = [
            "@com_github_ray_project_ray//thirdparty/patches:grpc-cython-copts.patch",
        ],
    )

    http_archive(
        name = "openssl",
        strip_prefix = "openssl-1.1.1f",
        sha256 = "186c6bfe6ecfba7a5b48c47f8a1673d0f3b0e5ba2e25602dd23b629975da3f35",
        urls = [
            "https://openssl.org/source/old/1.1.1/openssl-1.1.1f.tar.gz",
        ],
        build_file = "@rules_foreign_cc_thirdparty//openssl:BUILD.openssl.bazel",
    )

    http_archive(
        name = "rules_foreign_cc",
        sha256 = "2a4d07cd64b0719b39a7c12218a3e507672b82a97b98c6a89d38565894cf7c51",
        strip_prefix = "rules_foreign_cc-0.9.0",
        url = "https://github.com/bazelbuild/rules_foreign_cc/archive/refs/tags/0.9.0.tar.gz",
    )

    # Using shallow_since allows the rule to clone down fewer commits.
    # Reference:  https://bazel.build/rules/lib/repo/git
    git_repository(
        name = "rules_perl",
        remote = "https://github.com/bazelbuild/rules_perl.git",
        commit = "022b8daf2bb4836ac7a50e4a1d8ea056a3e1e403",
        shallow_since = "1663780239 -0700",
    )

    http_archive(
        name = "rules_foreign_cc_thirdparty",
        sha256 = "2a4d07cd64b0719b39a7c12218a3e507672b82a97b98c6a89d38565894cf7c51",
        strip_prefix = "rules_foreign_cc-0.9.0/examples/third_party",
        url = "https://github.com/bazelbuild/rules_foreign_cc/archive/refs/tags/0.9.0.tar.gz",
    )

    http_archive(
        # This rule is used by @com_github_grpc_grpc, and using a GitHub mirror
        # provides a deterministic archive hash for caching. Explanation here:
        # https://github.com/grpc/grpc/blob/1ff1feaa83e071d87c07827b0a317ffac673794f/bazel/grpc_deps.bzl#L189
        # Ensure this rule matches the rule used by grpc's bazel/grpc_deps.bzl
        name = "boringssl",
        sha256 = "0675a4f86ce5e959703425d6f9063eaadf6b61b7f3399e77a154c0e85bad46b1",
        strip_prefix = "boringssl-342e805bc1f5dfdd650e3f031686d6c939b095d9",
        urls = [
            "https://github.com/google/boringssl/archive/342e805bc1f5dfdd650e3f031686d6c939b095d9.tar.gz",
        ],
    )

    # The protobuf version we use to auto generate python and java code.
    # This can be different from the protobuf version that Ray core uses internally.
    # Generally this should be a lower version since protobuf guarantees that
    # code generated by protoc of version X can be used with protobuf library of version >= X.
    # So the version here effectively determines the lower bound of python/java
    # protobuf library that Ray supports.
    http_archive(
        name = "com_google_protobuf_rules_proto_grpc",
        strip_prefix = "protobuf-3.19.4",
        urls = ["https://github.com/protocolbuffers/protobuf/archive/v3.19.4.tar.gz"],
        sha256 = "3bd7828aa5af4b13b99c191e8b1e884ebfa9ad371b0ce264605d347f135d2568",
    )
    auto_http_archive(
        name = "rules_proto_grpc",
        url = "https://github.com/rules-proto-grpc/rules_proto_grpc/archive/a74fef39c5fe636580083545f76d1eab74f6450d.tar.gz",
        sha256 = "2f6606151ec042e23396f07de9e7dcf6ca9a5db1d2b09f0cc93a7fc7f4008d1b",
        repo_mapping = {
            "@com_google_protobuf": "@com_google_protobuf_rules_proto_grpc",
        },
    )

    auto_http_archive(
        name = "msgpack",
        build_file = True,
        url = "https://github.com/msgpack/msgpack-c/archive/8085ab8721090a447cf98bb802d1406ad7afe420.tar.gz",
        sha256 = "83c37c9ad926bbee68d564d9f53c6cbb057c1f755c264043ddd87d89e36d15bb",
        patches = [
            "@com_github_ray_project_ray//thirdparty/patches:msgpack-windows-iovec.patch",
        ],
    )

    http_archive(
        name = "io_opencensus_proto",
        strip_prefix = "opencensus-proto-0.3.0/src",
        urls = ["https://github.com/census-instrumentation/opencensus-proto/archive/v0.3.0.tar.gz"],
        sha256 = "b7e13f0b4259e80c3070b583c2f39e53153085a6918718b1c710caf7037572b0",
    )

    http_archive(
        name = "nlohmann_json",
        strip_prefix = "json-3.9.1",
        urls = ["https://github.com/nlohmann/json/archive/v3.9.1.tar.gz"],
        sha256 = "4cf0df69731494668bdd6460ed8cb269b68de9c19ad8c27abc24cd72605b2d5b",
        build_file = "@com_github_ray_project_ray//bazel:nlohmann_json.BUILD",
    )

    auto_http_archive(
        name = "rapidjson",
        url = "https://github.com/Tencent/rapidjson/archive/v1.1.0.zip",
        build_file = True,
        sha256 = "8e00c38829d6785a2dfb951bb87c6974fa07dfe488aa5b25deec4b8bc0f6a3ab",
    )

    # Hedron's Compile Commands Extractor for Bazel
    # https://github.com/hedronvision/bazel-compile-commands-extractor
    http_archive(
        name = "hedron_compile_commands",

        # Replace the commit hash in both places (below) with the latest, rather than using the stale one here.
        # Even better, set up Renovate and let it do the work for you (see "Suggestion: Updates" in the README).
        url = "https://github.com/hedronvision/bazel-compile-commands-extractor/archive/2e8b7654fa10c44b9937453fa4974ed2229d5366.tar.gz",
        strip_prefix = "bazel-compile-commands-extractor-2e8b7654fa10c44b9937453fa4974ed2229d5366",
        # When you first run this tool, it'll recommend a sha256 hash to put here with a message like: "DEBUG: Rule 'hedron_compile_commands' indicated that a canonical reproducible form can be obtained by modifying arguments sha256 = ..."
        sha256 = "7fbbbc05c112c44e9b406612e6a7a7f4789a6918d7aacefef4c35c105286930c",
    )

    http_archive(
        name = "jemalloc",
        urls = ["https://github.com/jemalloc/jemalloc/releases/download/5.3.0/jemalloc-5.3.0.tar.bz2"],
        build_file = "@com_github_ray_project_ray//bazel:jemalloc.BUILD",
        sha256 = "2db82d1e7119df3e71b7640219b6dfe84789bc0537983c3b7ac4f7189aecfeaa",
        strip_prefix = "jemalloc-5.3.0",
    )
