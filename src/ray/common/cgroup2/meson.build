
# === @begin: cgroup_manager_factory_cpp_lib (@original: cgroup_manager_factory) ===
# Bazel uses select() for linux vs non-linux srcs. We build the linux variant
# (linux_cgroup_manager_factory.cc). noop_cgroup_manager_factory.cc is the
# non-linux variant and is intentionally excluded to avoid duplicate symbols.
cgroup_manager_factory_cpp_lib_dependencies = [
    cgroup_manager_interface_cpp_lib_dep,
    noop_cgroup_manager_cpp_lib_dep,
    logging_cpp_lib_dep,

    # Linux-only deps (from select()):
    cgroup_driver_interface_cpp_lib_dep,
    cgroup_manager_cpp_lib_dep,
    sysfs_cgroup_driver_cpp_lib_dep,
    status_cpp_lib_dep,
    status_or_cpp_lib_dep,
    absl_strings,
]

cgroup_manager_factory_cpp_lib = static_library(
    'cgroup_manager_factory_cpp_lib',
    ['linux_cgroup_manager_factory.cc'],
    install: false,
    dependencies: cgroup_manager_factory_cpp_lib_dependencies
)

cgroup_manager_factory_cpp_lib_dep = declare_dependency(
    link_with: [cgroup_manager_factory_cpp_lib],
    dependencies: cgroup_manager_factory_cpp_lib_dependencies
)
# === @end: cgroup_manager_factory_cpp_lib ===


# === @begin: cgroup_manager_interface_cpp_lib (@original: cgroup_manager_interface) ===
cgroup_manager_interface_cpp_lib_dependencies = [
    cgroup_driver_interface_cpp_lib_dep,
    status_cpp_lib_dep,
    status_or_cpp_lib_dep,
]

cgroup_manager_interface_cpp_lib_dep = declare_dependency(
    sources: ['cgroup_manager_interface.h'],
    dependencies: cgroup_manager_interface_cpp_lib_dependencies
)
# === @end: cgroup_manager_interface_cpp_lib ===


# === @begin: cgroup_manager_cpp_lib (@original: cgroup_manager) ===
cgroup_manager_cpp_lib_dependencies = [
    cgroup_driver_interface_cpp_lib_dep,
    cgroup_manager_interface_cpp_lib_dep,
    status_cpp_lib_dep,
    status_or_cpp_lib_dep,
    logging_cpp_lib_dep,
]

cgroup_manager_cpp_lib = static_library(
    'cgroup_manager_cpp_lib',
    ['cgroup_manager.cc'],
    install: false,
    dependencies: cgroup_manager_cpp_lib_dependencies
)

cgroup_manager_cpp_lib_dep = declare_dependency(
    link_with: [cgroup_manager_cpp_lib],
    dependencies: cgroup_manager_cpp_lib_dependencies
)
# === @end: cgroup_manager_cpp_lib ===


# === @begin: noop_cgroup_manager_cpp_lib (@original: noop_cgroup_manager) ===
# Header-only — noop_cgroup_manager.h is used on all platforms when cgroups2
# support isn't enabled (enable_resource_isolation). It replaces the real
# cgroup_manager with a dummy class that returns OK for all operations.
# Alternative approach to header-only, suitable for public headers (the above is much more resilient to header names collisions):
# noop_cgroup_manager_cpp_lib_dep = declare_dependency(
#     #include_directories: ['.'], - implicit default
#     dependencies: noop_cgroup_manager_cpp_lib_dependencies
# )

# NOTE: This is an example and intentionally commented out here and left:
# We need to skip the actual `static_library` in this case to avoid duplicate symbols and general confusion as usually header-only library people don't want to be compiled neither could they be compiled.
# ray_cc_library(
#     name = "noop_cgroup_manager",
#     hdrs = [
#         "noop_cgroup_manager.h",
#     ],
#     visibility = [":__subpackages__"],
#     deps = [
#         ":cgroup_driver_interface",
#         ":cgroup_manager_interface",
#         "//src/ray/common:status",
#         "//src/ray/common:status_or",
#     ],
# )
# Other `noop_*` files we usually exclude as they will collide w/ the same-named `linux_*` files.
# Here, however, we need to include this header as it's used on Linux as well - if `cgroups2` support isn't enabled in Ray (`enable_resource_isolation`), it replaces the `cgroup_manager` w/ a dummy class which literally does nothing and returns `OK` for all operations.

# Alternative approach to header-only, suitable for public headers (the above is much more resilient to header names collisions):
# noop_cgroup_manager_cpp_lib_dep = declare_dependency(
#     #include_directories: ['.'], - implicit default
#     dependencies: noop_cgroup_manager_cpp_lib_dependencies
# )

noop_cgroup_manager_cpp_lib_dependencies = [
    cgroup_driver_interface_cpp_lib_dep,
    cgroup_manager_interface_cpp_lib_dep,
    status_cpp_lib_dep,
    status_or_cpp_lib_dep,
]

noop_cgroup_manager_cpp_lib_dep = declare_dependency(
    sources: ['noop_cgroup_manager.h'],
    dependencies: noop_cgroup_manager_cpp_lib_dependencies
)
# === @end: noop_cgroup_manager_cpp_lib ===


# === @begin: cgroup_driver_interface_cpp_lib (@original: cgroup_driver_interface) ===
cgroup_driver_interface_cpp_lib_dependencies = [
    status_cpp_lib_dep,
    status_or_cpp_lib_dep
]

cgroup_driver_interface_cpp_lib_dep = declare_dependency(
    sources: ['cgroup_driver_interface.h'],
    dependencies: cgroup_driver_interface_cpp_lib_dependencies
)
# === @end: cgroup_driver_interface_cpp_lib ===


# === @begin: sysfs_cgroup_driver_cpp_lib (@original: sysfs_cgroup_driver) ===
sysfs_cgroup_driver_cpp_lib_dependencies = [
    cgroup_driver_interface_cpp_lib_dep,
    status_cpp_lib_dep,
    status_or_cpp_lib_dep,
    logging_cpp_lib_dep,
    absl_strings,
]

sysfs_cgroup_driver_cpp_lib = static_library(
    'sysfs_cgroup_driver_cpp_lib',
    ['sysfs_cgroup_driver.cc'],
    install: false,
    dependencies: sysfs_cgroup_driver_cpp_lib_dependencies
)

sysfs_cgroup_driver_cpp_lib_dep = declare_dependency(
    # NOTE: include_directories: ['.'], - implicit default (leaving for just as an example)
    link_with: [sysfs_cgroup_driver_cpp_lib],
    dependencies: sysfs_cgroup_driver_cpp_lib_dependencies
)
# === @end: sysfs_cgroup_driver_cpp_lib ===


# === @begin: fake_cgroup_driver_cpp_lib (@original: fake_cgroup_driver) ===
fake_cgroup_driver_cpp_lib_dependencies = [
    cgroup_driver_interface_cpp_lib_dep,
    status_cpp_lib_dep,
]

# Header-only — test helper for faking cgroup driver behavior (linux only in Bazel).
fake_cgroup_driver_cpp_lib_dep = declare_dependency(
    sources: ['fake_cgroup_driver.h'],
    dependencies: fake_cgroup_driver_cpp_lib_dependencies
)
# === @end: fake_cgroup_driver_cpp_lib ===
