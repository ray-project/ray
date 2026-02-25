# === @begin: General ===
project(
    'ray-meson',
    'c', 'cpp', 'cython',
    version: '3.0.0.dev0',
    default_options: [
        'c_std=gnu17', # @TODO+: When rebuilding for Fedora 43, change this to `gnu23`
        'cpp_std=gnu++23',
        'buildtype=release',
    ],
    meson_version: '>=1.3.0'
)
# === @end: General ===


# === @begin: Meson modules imports ===
fs = import('fs')
py = import('python').find_installation(pure: false)
# === @end: Meson modules imports ===


# === @begin: Project-wide settings ===
# I don't want to bring the entire GTest, portions of which Ray requires even for non-test code.
# Instead, I've vendored a few GTest headers it really uses and add them into search paths, used by all compiler invocations.
add_project_arguments(
    '-I' + meson.project_build_root() / 'eugo' / 'include',
    '-I' + meson.project_source_root() / 'eugo' / 'include',
    language: ['c', 'cpp']
)

# Required to make generated protobuf headers visible to the files relying on them.
# These generated headers are included using not their names but their paths starting from the repo root, so we have to
# use `project_build_root` as they are generated dynamically.
#
# @Important:
# Some of Ray internal headers have extremely generic names like `array.h`, `compat.h`, or `event.h`, which potentially may lead to collisions w/ system-provided headers, including both installed from DNF (like `native/glibc`) and Eugo-built (like `native/libevent`).
# However, Ray includes their own headers via fully-qualified paths, relative to either `@SOURCE_ROOT@` or `@SOURCE_ROOT@/src`, so this fix originally introduced for `.proto`-generated files also helps us here by making these includes globally visible to all Ray components.
add_project_arguments(
    '-I' + meson.project_build_root(),
    '-I' + meson.project_build_root() / 'src',
    '-I' + meson.project_source_root(),
    '-I' + meson.project_source_root() / 'src',
    language: ['c', 'cpp']
)

# @Important:
# Some of Ray internal headers have extremely generic names like `array.h`, `compat.h`, or `event.h`, which potentially may lead to collisions w/ system-provided headers, including both installed from DNF (like `native/glibc`) and Eugo-built (like `native/libevent`).
#
# @Prompt: In which order Meson passes the `-I` flags to the compiler based on their sources? E.g., CXXFLAGS / CFLAGS, `add_project_arguments(...)`, `include_directories : `, and the like.
#
# The Meson docs don't have a dedicated page on `-I` ordering, but the ordering is well-defined from Meson internals and inspecting generated Ninja files. Here is the order for C/C++ compilation:
#
# 1. **`add_global_arguments()` / `add_project_arguments()`** — first, in declaration order. Your raw `-I` strings land here.
# 2. **`include_directories:` from transitive `dependencies:`** — paths from `declare_dependency(include_directories: ...)`, pkg-config, and cmake deps, in dependency-list order. System deps (or `is_system: true`) use `-isystem` instead of `-I`.
# 3. **`include_directories:` directly on the target** — after all dependency paths.
# 4. **Implicit include directories** — the current source subdir and build subdir (`implicit_include_directories: true`, the default). These come last among Meson-controlled paths.
# 5. **`CFLAGS`/`CXXFLAGS`** — appended at the very end, unless `c_args`/`cpp_args` is set in `default_options` (in which case the env vars are ignored entirely).
#
# **Practical consequence for this codebase:** Since `add_project_arguments` runs first, Ray's own headers (e.g., `event.h`, `array.h`) win over any system header with the same name. The collision risk is the reverse: if a Ray `.cc` file intends to include a *system* header like `<event.h>` (e.g., from libevent), it will accidentally pick up Ray's event.h first because the `add_project_arguments('-I' + project_source_root() / 'src')` path appears before libevent's system path. Using `-isystem` (via `is_system: true` in `include_directories()`) for third-party deps would push them after `-I` paths and partially mitigate this, but it doesn't fully solve same-named header shadowing from within Ray's own tree.
#
# @Prompt #2:
# What if we're passing -I flags via `clang.conf` configuration files, too?
#
# `-I` flags from Clang config files (`.cfg`) are inserted at the **very beginning** of argument processing — before Meson's `add_project_arguments`, before dependencies, before everything. Clang expands config file contents as if they were prepended to the driver command line before any other argument is processed.
#
# So the full ordering is:
#
# | Priority | Source | Note |
# |---|---|---|
# | **0** (highest) | `clang.cfg` / `--config` flags | Prepended before Meson's command line |
# | 1 | `add_global_arguments()` / `add_project_arguments()` | |
# | 2 | `include_directories:` from `dependencies:` | `-isystem` if `is_system: true` |
# | 3 | `include_directories:` directly on target | |
# | 4 | Implicit source/build subdirs | `implicit_include_directories: true` |
# | 5 (lowest) | `CFLAGS`/`CXXFLAGS` env vars | Ignored if `c_args`/`cpp_args` set in `default_options` |
#
# **Practical implication here:** If the build environment has a `clang.cfg` that injects `-I/usr/include` or any other system path containing generic names like `event.h`, those would shadow Ray's own headers unexpectedly — even before `add_project_arguments` gets a chance to establish Ray's paths first. This is an environment-level hazard that can't be controlled from meson.build.

# === @end: Project-wide settings ===


# === @begin: eugo/ ===
subdir('eugo/')
# === @end: eugo/ ===


# === @begin: ray_commit_sha ===
ray_commit_sha = run_command(git, 'rev-parse', 'HEAD', check: true).stdout().strip()
# === @end: ray_commit_sha ===


# === @begin: src/ ===
subdir('src/')
# === @end: src/ ===


# === @begin: python/ ===
subdir('python/')
# === @end: python/ ===
