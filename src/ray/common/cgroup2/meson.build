# IMPORTANT: Ordering within this file respects the internal dependency graph.
# You will see things "out of order" from a BUILD.bazel perspective because
# Meson requires define-before-use: if target A depends on target B and both
# are in this file, B must appear first regardless of Bazel ordering.
# Specifically:
#   - cgroup_driver_interface has no same-file deps, so it must appear first
#     (it is used by cgroup_manager_interface, cgroup_manager, noop_cgroup_manager).
#   - cgroup_manager_factory depends on ALL other targets in this file,
#     so it must appear last despite being first in BUILD.bazel.


# === @begin: cgroup_driver_interface_cpp_lib (@original: cgroup_driver_interface) ===
# Header-only
cgroup_driver_interface_cpp_lib_dependencies = [
    # Package-managed
    status_cpp_lib_dep,
    status_or_cpp_lib_dep,
]

cgroup_driver_interface_cpp_lib_dep = declare_dependency(
    sources: ['cgroup_driver_interface.h'],
    dependencies: cgroup_driver_interface_cpp_lib_dependencies
)
# === @end: cgroup_driver_interface_cpp_lib ===


# === @begin: cgroup_manager_interface_cpp_lib (@original: cgroup_manager_interface) ===
# Header-only
cgroup_manager_interface_cpp_lib_dependencies = [
    # Package-managed
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
    # Package-managed
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

noop_cgroup_manager_cpp_lib_dependencies = [
    # Package-managed
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


# === @begin: sysfs_cgroup_driver_cpp_lib (@original: sysfs_cgroup_driver) ===
sysfs_cgroup_driver_cpp_lib_dependencies = [
    # Package-managed
    cgroup_driver_interface_cpp_lib_dep,
    status_cpp_lib_dep,
    status_or_cpp_lib_dep,
    logging_cpp_lib_dep,

    # Eugo-managed
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
# Not needed - test helper
# Header-only
# fake_cgroup_driver_cpp_lib_dependencies = [
#     # Package-managed
#     cgroup_driver_interface_cpp_lib_dep,
#     status_cpp_lib_dep,
# ]

# # Header-only — test helper for faking cgroup driver behavior (linux only in Bazel).
# fake_cgroup_driver_cpp_lib_dep = declare_dependency(
#     sources: ['fake_cgroup_driver.h'],
#     dependencies: fake_cgroup_driver_cpp_lib_dependencies
# )
# === @end: fake_cgroup_driver_cpp_lib ===


# === @begin: cgroup_manager_factory_cpp_lib (@original: cgroup_manager_factory) ===
# Bazel uses select() for linux vs non-linux srcs. We build the linux variant
# (linux_cgroup_manager_factory.cc). noop_cgroup_manager_factory.cc is the
# non-linux variant and is intentionally excluded to avoid duplicate symbols.
cgroup_manager_factory_cpp_lib_dependencies = [
    # Package-managed
    cgroup_manager_interface_cpp_lib_dep,
    noop_cgroup_manager_cpp_lib_dep,
    logging_cpp_lib_dep,
    # Linux-only deps (from select()):
    cgroup_driver_interface_cpp_lib_dep,
    cgroup_manager_cpp_lib_dep,
    sysfs_cgroup_driver_cpp_lib_dep,
    status_cpp_lib_dep,
    status_or_cpp_lib_dep,

    # Eugo-managed
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
