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
        build_file = "@com_github_ray_project_ray//%s:%s" % ("bazel", "BUILD." + name)

    if urls == True:
        prefer_url_over_mirrors = is_github
        urls = [
            mirror_prefix + url_except_scheme
            for mirror_prefix in mirror_prefixes
            if not canonical_url.startswith(mirror_prefix)
        ]
        urls.insert(0 if prefer_url_over_mirrors else len(urls), canonical_url)
    else:
        print("No implicit mirrors used because urls were explicitly provided")

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
    http_archive(
        name = "com_google_protobuf",
        strip_prefix = "protobuf-3.19.4",
        urls = ["https://github.com/protocolbuffers/protobuf/archive/v3.19.4.tar.gz"],
        sha256 = "3bd7828aa5af4b13b99c191e8b1e884ebfa9ad371b0ce264605d347f135d2568",
    )

    # NOTE(lingxuan.zlx): 3rd party dependencies could be accessed, so it suggests
    # all of http/git_repository should add prefix for patches defined in ray directory.
    auto_http_archive(
        name = "com_github_antirez_redis",
        build_file = "@com_github_ray_project_ray//bazel:BUILD.redis",
        patch_args = ["-p1"],
        url = "https://github.com/redis/redis/archive/refs/tags/7.0.5.tar.gz",
        sha256 = "40827fcaf188456ad9b3be8e27a4f403c43672b6bb6201192dc15756af6f1eae",
        patches = [
            "@com_github_ray_project_ray//thirdparty/patches:redis-quiet.patch",
        ],
        workspace_file_content = 'workspace(name = "com_github_antirez_redis")'
    )

    auto_http_archive(
        name = "com_github_redis_hiredis",
        build_file = "@com_github_ray_project_ray//bazel:BUILD.hiredis",
        url = "https://github.com/redis/hiredis/archive/refs/tags/v1.1.0.tar.gz",
        strip_prefix= "hiredis-1.1.0",
        sha256 = "fe6d21741ec7f3fc9df409d921f47dfc73a4d8ff64f4ac6f1d95f951bf7f53d6",
    )

    auto_http_archive(
        name = "com_github_spdlog",
        build_file = "@com_github_ray_project_ray//bazel:BUILD.spdlog",
        urls = ["https://github.com/gabime/spdlog/archive/v1.7.0.zip"],
        sha256 = "c8f1e1103e0b148eb8832275d8e68036f2fdd3975a1199af0e844908c56f6ea5",
    )

    auto_http_archive(
        name = "com_github_tporadowski_redis_bin",
        build_file = "@com_github_ray_project_ray//bazel:BUILD.redis",
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
        name = "libuv",
        strip_prefix = "libuv-1.44.2",
        url = "https://github.com/libuv/libuv/archive/refs/tags/v1.44.2.tar.gz",
        sha256 = "e6e2ba8b4c349a4182a33370bb9be5e23c51b32efb9b9e209d0e8556b73a48da",
        build_file = "@com_github_ray_project_ray//bazel:BUILD.libuv",
    )

    auto_http_archive(
        name = "redispp",
        strip_prefix = "redis-plus-plus-1.3.6",
        url = "https://github.com/sewenew/redis-plus-plus/archive/refs/tags/1.3.6.tar.gz",
        sha256 = "87dcadca50c6f0403cde47eb1f79af7ac8dd5a19c3cad2bb54ba5a34f9173a3e",
        build_file = "@com_github_ray_project_ray//bazel:BUILD.redispp",
    )

    auto_http_archive(
        # This rule is used by @com_github_nelhage_rules_boost and
        # declaring it here allows us to avoid patching the latter.
        name = "boost",
        build_file = "@com_github_nelhage_rules_boost//:BUILD.boost",
        sha256 = "83bfc1507731a0906e387fc28b7ef5417d591429e51e788417fe9ff025e116b1",
        url = "https://boostorg.jfrog.io/artifactory/main/release/1.74.0/source/boost_1_74_0.tar.bz2",
        patches = [
            "@com_github_ray_project_ray//thirdparty/patches:boost-exception-no_warn_typeid_evaluated.patch",
        ],
    )

    auto_http_archive(
        name = "com_github_nelhage_rules_boost",
        # If you update the Boost version, remember to update the 'boost' rule.
        url = "https://github.com/nelhage/rules_boost/archive/652b21e35e4eeed5579e696da0facbe8dba52b1f.tar.gz",
        sha256 = "c1b8b2adc3b4201683cf94dda7eef3fc0f4f4c0ea5caa3ed3feffe07e1fb5b15",
        patches = [
            "@com_github_ray_project_ray//thirdparty/patches:rules_boost-windows-linkopts.patch",
        ],
    )

    auto_http_archive(
        name = "com_github_google_flatbuffers",
        url = "https://github.com/google/flatbuffers/archive/63d51afd1196336a7d1f56a988091ef05deb1c62.tar.gz",
        sha256 = "3f469032571d324eabea88d7014c05fec8565a5877dbe49b2a52d8d1a0f18e63",
    )

    auto_http_archive(
        name = "com_google_googletest",
        url = "https://github.com/google/googletest/archive/refs/tags/release-1.12.1.tar.gz",
        sha256 = "81964fe578e9bd7c94dfdb09c8e4d6e6759e19967e397dbea48d1c10e45d0df2",
    )

    auto_http_archive(
        name = "com_github_gflags_gflags",
        url = "https://github.com/gflags/gflags/archive/e171aa2d15ed9eb17054558e0b3a6a413bb01067.tar.gz",
        sha256 = "b20f58e7f210ceb0e768eb1476073d0748af9b19dfbbf53f4fd16e3fb49c5ac8",
    )

    auto_http_archive(
        name = "cython",
        build_file = True,
        url = "https://github.com/cython/cython/archive/3028e8c7ac296bc848d996e397c3354b3dbbd431.tar.gz",
        sha256 = "31ea23c2231ddee8572a2a5effd54952e16a1b44e9a4cb3eb645418f8accf20d",
    )

    auto_http_archive(
        name = "com_github_johnynek_bazel_jar_jar",
        url = "https://github.com/johnynek/bazel_jar_jar/archive/171f268569384c57c19474b04aebe574d85fde0d.tar.gz",
        sha256 = "97c5f862482a05f385bd8f9d28a9bbf684b0cf3fae93112ee96f3fb04d34b193",
    )

    auto_http_archive(
        name = "io_opencensus_cpp",
        url = "https://github.com/census-instrumentation/opencensus-cpp/archive/b14a5c0dcc2da8a7fc438fab637845c73438b703.zip",
        sha256 = "6592e07672e7f7980687f6c1abda81974d8d379e273fea3b54b6c4d855489b9d",
        patches = [
            "@com_github_ray_project_ray//thirdparty/patches:opencensus-cpp-harvest-interval.patch",
            "@com_github_ray_project_ray//thirdparty/patches:opencensus-cpp-shutdown-api.patch",
        ],
    )

    # OpenCensus depends on Abseil so we have to explicitly pull it in.
    # This is how diamond dependencies are prevented.
    auto_http_archive(
        name = "com_google_absl",
        url = "https://github.com/abseil/abseil-cpp/archive/refs/tags/20220623.1.tar.gz",
        sha256 = "91ac87d30cc6d79f9ab974c51874a704de9c2647c40f6932597329a282217ba8",
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
        url = "https://github.com/grpc/grpc/archive/refs/tags/v1.45.2.tar.gz",
        sha256 = "e18b16f7976aab9a36c14c38180f042bb0fd196b75c9fd6a20a2b5f934876ad6",
        patches = [
            "@com_github_ray_project_ray//thirdparty/patches:grpc-cython-copts.patch",
            "@com_github_ray_project_ray//thirdparty/patches:grpc-python.patch",
        ],
    )
    
    http_archive(
        name = "openssl",
        strip_prefix = "openssl-1.1.1f",
        sha256 = "186c6bfe6ecfba7a5b48c47f8a1673d0f3b0e5ba2e25602dd23b629975da3f35",
        urls = [
            "https://www.openssl.org/source/openssl-1.1.1f.tar.gz",
        ],
        build_file = "@rules_foreign_cc_thirdparty//openssl:BUILD.openssl.bazel",
    )
    
    http_archive(
        name = "rules_foreign_cc",
        sha256 = "2a4d07cd64b0719b39a7c12218a3e507672b82a97b98c6a89d38565894cf7c51",
        strip_prefix = "rules_foreign_cc-0.9.0",
        url = "https://github.com/bazelbuild/rules_foreign_cc/archive/refs/tags/0.9.0.tar.gz",
    )

    git_repository(
        name = "rules_perl",
        remote = "https://github.com/bazelbuild/rules_perl.git",
        commit = "022b8daf2bb4836ac7a50e4a1d8ea056a3e1e403",
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
        sha256 = "e168777eb0fc14ea5a65749a2f53c095935a6ea65f38899a289808fb0c221dc4",
        strip_prefix = "boringssl-4fb158925f7753d80fb858cb0239dff893ef9f15",
        urls = [
            "https://storage.googleapis.com/grpc-bazel-mirror/github.com/google/boringssl/archive/4fb158925f7753d80fb858cb0239dff893ef9f15.tar.gz",
            "https://github.com/google/boringssl/archive/4fb158925f7753d80fb858cb0239dff893ef9f15.tar.gz",
        ],
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
        build_file = "@com_github_ray_project_ray//bazel:BUILD.nlohmann_json",
    )

    auto_http_archive(
        name = "rapidjson",
        url = "https://github.com/Tencent/rapidjson/archive/v1.1.0.zip",
        build_file = True,
        sha256 = "8e00c38829d6785a2dfb951bb87c6974fa07dfe488aa5b25deec4b8bc0f6a3ab",
    )

    # The following should be removed after this commit
    # (https://github.com/bazelbuild/bazel/commit/676a0c8dea0e7782e47a386396e386a51566087f) released.
    http_archive(
        name = "platforms",
        urls = [
            "https://mirror.bazel.build/github.com/bazelbuild/platforms/releases/download/0.0.5/platforms-0.0.5.tar.gz",
            "https://github.com/bazelbuild/platforms/releases/download/0.0.5/platforms-0.0.5.tar.gz",
        ],
        sha256 = "379113459b0feaf6bfbb584a91874c065078aa673222846ac765f86661c27407",
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
