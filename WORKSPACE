workspace(name = "io_ray")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "platforms",
    sha256 = "5eda539c841265031c2f82d8ae7a3a6490bd62176e0c038fc469eabf91f6149b",
    urls = [
        "https://github.com/bazelbuild/platforms/releases/download/0.0.9/platforms-0.0.9.tar.gz",
    ],
)

http_archive(
    name = "rules_java",
    sha256 = "302bcd9592377bf9befc8e41aa97ec02df12813d47af9979e4764f3ffdcc5da8",
    urls = [
        "https://github.com/bazelbuild/rules_java/releases/download/7.12.4/rules_java-7.12.4.tar.gz",
    ],
)

load("@rules_java//java:repositories.bzl", "rules_java_dependencies", "rules_java_toolchains")

rules_java_dependencies()

rules_java_toolchains()

load("//bazel:ray_deps_setup.bzl", "ray_deps_setup")

ray_deps_setup()

load("//bazel:ray_deps_build_all.bzl", "ray_deps_build_all")

ray_deps_build_all()

# This needs to be run after grpc_deps() in ray_deps_build_all() to make
# sure all the packages loaded by grpc_deps() are available. However a
# load() statement cannot be in a function so we put it here.
load("@com_github_grpc_grpc//bazel:grpc_extra_deps.bzl", "grpc_extra_deps")

grpc_extra_deps()

load("@bazel_skylib//lib:versions.bzl", "versions")

# Please keep this in sync with the .bazelversion file.
versions.check(
    maximum_bazel_version = "7.5.0",
    minimum_bazel_version = "7.5.0",
)

load("@hedron_compile_commands//:workspace_setup.bzl", "hedron_compile_commands_setup")

hedron_compile_commands_setup()

load("@rules_python//python:repositories.bzl", "python_register_toolchains")

python_register_toolchains(
    name = "python3_10",
    python_version = "3.10",
    register_toolchains = False,
)

load("@python3_10//:defs.bzl", python310 = "interpreter")
load("@rules_python//python/pip_install:repositories.bzl", "pip_install_dependencies")

# rules_python's whl_library shells out to pip rather than using Bazel's downloader, so
# wheel fetches are invisible to Bazel's retry and mirroring settings. The pip that
# rules_python pins (22.0.4) leaves 502 out of urllib3's status_forcelist, so a
# transient PyPI/Fastly 502 fails a repository fetch on the first try with no retry at
# all, breaking the whole build. pip 24.0 added 502 to that list upstream.
#
# pip 24 also drops pip._internal.cli.progress_bars.BAR_TYPES, which the pip-tools 6.6.0
# that rules_python pins alongside it imports, so pip-tools moves to 7.4.1 here as well.
# 7.4.1 replaced pep517 with build + pyproject_hooks and needs packaging, none of which
# rules_python provides, so those three are declared below too and wired into
# //release:requirements_py310.update via _requirements_py310_deps. Versions are the set
# rules_python itself pairs with pip 24.0 / pip-tools 7.4.1 upstream, rather than an
# ad-hoc combination.
#
# The pypi__pip and pypi__pip_tools entries are declared before
# pip_install_dependencies() so that its maybe() calls become no-ops and these win. Keep
# the url/sha256 pairs in sync with _RULE_DEPS in
# @rules_python//python/pip_install:repositories.bzl.
#
# All five entries exist only because rules_python here is 0.9.0, from July 2022, which
# is where the pip 22.0.4 pin comes from. Ray never declares rules_python: it arrives
# transitively via ray_deps_build_all() -> rules_foreign_cc_dependencies(), which pins it
# at rules_foreign_cc/foreign_cc/repositories.bzl:85. That is also a maybe(), so pinning
# rules_python explicitly before ray_deps_build_all() would take precedence and is the
# real fix - a modern rules_python pins pip 24.0 itself, making all five entries below
# unnecessary, and adds the experimental_index_url path that fetches wheels through
# Bazel's downloader, where --experimental_repository_downloader_retries and downloader
# URL rewriting would finally apply to them. That upgrade rewrites whl_library, repo
# naming and the generated requirements.bzl, so it needs to be its own change; delete
# this block as part of it, including the pip patch - once Bazel does the fetching,
# pip's own retry schedule stops being what governs a wheel download.
#
# Note that upgrading pip for the hermetic interpreter is not an alternative. The
# python_register_toolchains() CPython build ships its own pip (also 22.0.4), but
# whl_library puts the pypi__* repositories on PYTHONPATH, which precedes site-packages
# on sys.path, so pypi__pip shadows it and the interpreter's copy is never used.
_PYPI_WHEEL_BUILD_FILE = """\
package(default_visibility = ["//visibility:public"])

load("@rules_python//python:defs.bzl", "py_library")

py_library(
    name = "lib",
    srcs = glob(["**/*.py"]),
    data = glob(["**/*"], exclude=["**/*.py", "**/* *", "BUILD", "WORKSPACE"]),
    # This makes this directory a top-level in the python import
    # search path for anything that depends on this.
    imports = ["."],
)
"""

http_archive(
    name = "pypi__pip",
    build_file_content = _PYPI_WHEEL_BUILD_FILE,
    patch_args = ["-p1"],
    # The patch changes two things pip does not let a caller configure, which between
    # them decide whether retrying can succeed at all.
    #
    # When to retry. pip hardcodes backoff_factor=0.25 where it builds its urllib3.Retry
    # and exposes no flag or environment variable for the shape of the backoff, and the
    # urllib3 it vendors (1.26.17) implements exponential only - jitter arrived in
    # urllib3 2.x. A constant jittered schedule goes in instead, so the --retries count
    # below buys a predictable amount of wall-clock; see the comment on extra_pip_args.
    #
    # Where to retry. urllib3 drains a failed response specifically in order to keep the
    # socket alive, and Fastly selects the edge node per connection, so every retry is
    # pinned to the node that just failed. Sending Connection: close makes each attempt
    # redial and re-draw a node. Without it no retry count helps against a single bad
    # node, which is the failure mode described in pypi/support#11876.
    patches = ["//thirdparty/patches:pip-retry-backoff-and-redial.patch"],
    sha256 = "ba0d021a166865d2265246961bec0152ff124de910c5cc39f1156ce3fa7c69dc",
    type = "zip",
    url = "https://files.pythonhosted.org/packages/8a/6a/19e9fe04fca059ccf770861c7d5721ab4c2aebc539889e97c7977528a53b/pip-24.0-py3-none-any.whl",
)

http_archive(
    name = "pypi__pip_tools",
    build_file_content = _PYPI_WHEEL_BUILD_FILE,
    sha256 = "4c690e5fbae2f21e87843e89c26191f0d9454f362d8acdbd695716493ec8b3a9",
    type = "zip",
    url = "https://files.pythonhosted.org/packages/0d/dc/38f4ce065e92c66f058ea7a368a9c5de4e702272b479c0992059f7693941/pip_tools-7.4.1-py3-none-any.whl",
)

http_archive(
    name = "pypi__build",
    build_file_content = _PYPI_WHEEL_BUILD_FILE,
    sha256 = "75e10f767a433d9a86e50d83f418e83efc18ede923ee5ff7df93b6cb0306c5d4",
    type = "zip",
    url = "https://files.pythonhosted.org/packages/e2/03/f3c8ba0a6b6e30d7d18c40faab90807c9bb5e9a1e3b2fe2008af624a9c97/build-1.2.1-py3-none-any.whl",
)

http_archive(
    name = "pypi__packaging",
    build_file_content = _PYPI_WHEEL_BUILD_FILE,
    sha256 = "2ddfb553fdf02fb784c234c7ba6ccc288296ceabec964ad2eae3777778130bc5",
    type = "zip",
    url = "https://files.pythonhosted.org/packages/49/df/1fceb2f8900f8639e278b056416d49134fb8d84c5942ffaa01ad34782422/packaging-24.0-py3-none-any.whl",
)

http_archive(
    name = "pypi__pyproject_hooks",
    build_file_content = _PYPI_WHEEL_BUILD_FILE,
    sha256 = "7ceeefe9aec63a1064c18d939bdc3adf2d8aa1988a510afec15151578b232aa2",
    type = "zip",
    url = "https://files.pythonhosted.org/packages/ae/f3/431b9d5fe7d14af7a32340792ef43b8a714e7726f1d7b69cc4e8e7a3f1d7/pyproject_hooks-1.1.0-py3-none-any.whl",
)

pip_install_dependencies()

load("@rules_python//python:pip.bzl", "pip_parse")

# For CI scripts use only; not for ray testing.
pip_parse(
    name = "py_deps_py310",
    # The retries above are only usable if the fetch is allowed to last long enough to
    # spend them. timeout defaults to 600s and pip_parse forwards it to every generated
    # whl_library, so it bounds each package's pip invocation, not just this rule. The
    # retry budget alone reaches 588s at worst, and each of the 50 attempts can burn up
    # to pip's 15s socket timeout on top of that when the failure mode is a hang rather
    # than a fast 502, so the default cannot hold the schedule. 1800s covers the full
    # backoff, the per-attempt timeouts and a slow transfer afterwards.
    timeout = 1800,
    # pip retries 5 times by default, and the schedule is exponential, so it gives up
    # after ~8 seconds (0, 0.5, 1, 2, 4). That is far shorter than a PyPI incident,
    # and simply raising the count does not fix it: exponential backoff doubles until
    # it saturates urllib3's 120s per-sleep cap, so most of a larger budget is spent
    # asleep in 2-minute blocks rather than covering the outage. At 50 retries the
    # upstream schedule would run for ~5050s, which no sane fetch timeout accommodates.
    #
    # The patch on pypi__pip above replaces that with a constant schedule jittered over
    # 6-12s per sleep, which makes the count buy wall-clock linearly: 50 retries is 49
    # sleeps, so 294-588s, ~440s typical, bounded under 600s. The jitter also spreads
    # out the many wheel fetches Bazel runs in parallel, which would otherwise retry in
    # lockstep and hit an already-degraded origin all at once.
    #
    # Both settings are needed because two different pip processes fetch from PyPI
    # here. PIP_RETRIES covers the pip that setuptools spawns for setup_requires when
    # a locked package ships only an sdist; that one is not isolated, so it reads the
    # environment. --retries covers this pip invocation, which runs with --isolated
    # and therefore ignores PIP_* variables. Neither helps unless 502 is retryable in
    # the first place, which is what the pip 24.0 pin above provides.
    environment = {"PIP_RETRIES": "50"},
    extra_pip_args = ["--retries=50"],
    python_interpreter_target = python310,
    requirements_lock = "//release:requirements_py310.txt",
)

load("@py_deps_py310//:requirements.bzl", install_py_deps_py310 = "install_deps")

install_py_deps_py310()

register_toolchains("//bazel:py310_toolchain")

register_execution_platforms(
    "@local_config_platform//:host",
    "//bazel:py310_platform",
)

http_archive(
    name = "crane_linux_x86_64",
    build_file_content = """
filegroup(
    name = "file",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)
""",
    sha256 = "daa629648e1d1d10fc8bde5e6ce4176cbc0cd48a32211b28c3fd806e0fa5f29b",
    urls = ["https://github.com/google/go-containerregistry/releases/download/v0.19.0/go-containerregistry_Linux_x86_64.tar.gz"],
)

http_archive(
    name = "registry_x86_64",
    build_file_content = """
filegroup(
    name = "file",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)
""",
    sha256 = "61c9a2c0d5981a78482025b6b69728521fbc78506d68b223d4a2eb825de5ca3d",
    urls = ["https://github.com/distribution/distribution/releases/download/v3.0.0/registry_3.0.0_linux_amd64.tar.gz"],
)

http_archive(
    name = "uv_x86_64-linux",
    build_file_content = """
filegroup(
    name = "file",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)
""",
    sha256 = "30ccbf0a66dc8727a02b0e245c583ee970bdafecf3a443c1686e1b30ec4939e8",
    urls = ["https://github.com/astral-sh/uv/releases/download/0.9.26/uv-x86_64-unknown-linux-gnu.tar.gz"],
)

http_archive(
    name = "uv_aarch64-darwin",
    build_file_content = """
filegroup(
    name = "file",
    srcs = glob(["**"]),
    visibility = ["//visibility:public"],
)
""",
    sha256 = "fcf0a9ea6599c6ae28a4c854ac6da76f2c889354d7c36ce136ef071f7ab9721f",
    urls = ["https://github.com/astral-sh/uv/releases/download/0.9.26/uv-aarch64-apple-darwin.tar.gz"],
)

http_archive(
    name = "com_github_storypku_bazel_iwyu",
    sha256 = "aa78c331a2cb139f73f7d74eeb4d5ab29794af82023ef5d6d5194f76b7d37449",
    strip_prefix = "bazel_iwyu-0.19.2",
    urls = [
        "https://github.com/storypku/bazel_iwyu/archive/0.19.2.tar.gz",
    ],
)

http_archive(
    name = "redis_linux_x86_64",
    build_file_content = """exports_files(["redis-server", "redis-cli"])""",
    sha256 = "4ae33c10059ed52202a12929d269deea46fac81b8e02e722d30cb22ceb3ed678",
    urls = ["https://github.com/ray-project/redis/releases/download/7.2.3/redis-linux-x86_64.tar.gz"],
)

http_archive(
    name = "redis_linux_arm64",
    build_file_content = """exports_files(["redis-server", "redis-cli"])""",
    sha256 = "2d1085a4f69477e1f44cbddd531e593f0712532b1ade9beab0b221a0cb01f298",
    urls = ["https://github.com/ray-project/redis/releases/download/7.2.3/redis-linux-arm64.tar.gz"],
)

http_archive(
    name = "redis_osx_arm64",
    build_file_content = """exports_files(["redis-server", "redis-cli"])""",
    sha256 = "74b76099c3600b538252cdd1731278e087e8e85eecc6c64318c860f3e9462506",
    urls = ["https://github.com/ray-project/redis/releases/download/7.2.3/redis-osx-arm64.tar.gz"],
)

load("@com_github_storypku_bazel_iwyu//bazel:dependencies.bzl", "bazel_iwyu_dependencies")

bazel_iwyu_dependencies()
