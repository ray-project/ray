# @IMPORTANT: Several util targets depend on `common/` targets (`status`, `status_or`, `id`,
# `ray_config`) but `common/` is processed AFTER `util/` in src/ray/meson.build
# (circular dependency). These deps are documented as comments but cannot be
# declared as Meson deps here. This works because headers resolve via global include paths (src/);
# symbols resolve at final link time when all static libraries link into `_raylet`.
#
# Why it is safe (today): Meson compiles targets in parallel based on the declared dependency
# graph. Since `util/` targets don't declare deps on `common/` targets, Meson won't enforce an
# ordering between them. This is safe ONLY because status.h, status_or.h, id.h, and
# ray_config.h are plain source files on disk — the compiler can read them at any time without
# waiting for a code generation step.
#
# The risk: if any of those `common/` headers ever transitively #include a generated file
# (e.g., a `.pb.h` proto header), a parallel build could fail: Meson might compile a `util/`
# target before the relevant proto codegen has run, because there is no declared dependency
# chain requiring that ordering. As of 2026-02-22, `status.h` / `status_or.h` / `id.h` have no
# transitive proto includes, so the build is correct. Monitor upstream changes to these headers.
#
# Long-term fix: Step 2 (single shared_library) eliminates all circular deps / ordering issues by merging
# everything into one compilation unit, making this entire workaround unnecessary.


# ============================================================
# Level 0: Sink Nodes — no internal dependencies
# ============================================================


# === @begin: visibility_cpp_lib (@original: visibility) ===
# Header-only
visibility_cpp_lib_dep = declare_dependency(
    sources: ['visibility.h']
)
# === @end: visibility_cpp_lib (@original: visibility) ===


# === @begin: macros_cpp_lib (@original: macros) ===
# Header-only
macros_cpp_lib_dep = declare_dependency(
    sources: ['macros.h']
)
# === @end: macros_cpp_lib (@original: macros) ===


# === @begin: array_cpp_lib (@original: array) ===
# Header-only
array_cpp_lib_dep = declare_dependency(
    sources: ['array.h']
)
# === @end: array_cpp_lib (@original: array) ===


# === @begin: type_traits_cpp_lib (@original: type_traits) ===
# Header-only
type_traits_cpp_lib_dep = declare_dependency(
    sources: ['type_traits.h']
)
# === @end: type_traits_cpp_lib (@original: type_traits) ===


# === @begin: raii_cpp_lib (@original: raii) ===
# Header-only
raii_cpp_lib_dep = declare_dependency(
    sources: ['raii.h']
)
# === @end: raii_cpp_lib (@original: raii) ===


# === @begin: size_literals_cpp_lib (@original: size_literals) ===
# Header-only
size_literals_cpp_lib_dep = declare_dependency(
    sources: ['size_literals.h']
)
# === @end: size_literals_cpp_lib (@original: size_literals) ===


# === @begin: map_utils_cpp_lib (@original: map_utils) ===
# Header-only
map_utils_cpp_lib_dep = declare_dependency(
    sources: ['map_utils.h']
)
# === @end: map_utils_cpp_lib (@original: map_utils) ===


# === @begin: stream_redirection_options_cpp_lib (@original: stream_redirection_options) ===
# Header-only
stream_redirection_options_cpp_lib_dep = declare_dependency(
    sources: ['stream_redirection_options.h']
)
# === @end: stream_redirection_options_cpp_lib (@original: stream_redirection_options) ===


# === @begin: random_cpp_lib (@original: random) ===
# Header-only
random_cpp_lib_dependencies = [
    # Eugo-managed
    absl_random_random,
]

random_cpp_lib_dep = declare_dependency(
    sources: ['random.h'],
    dependencies: random_cpp_lib_dependencies
)
# === @end: random_cpp_lib (@original: random) ===


# === @begin: mutex_protected_cpp_lib (@original: mutex_protected) ===
# Header-only
mutex_protected_cpp_lib_dependencies = [
    # Eugo-managed
    absl_synchronization,
]

mutex_protected_cpp_lib_dep = declare_dependency(
    sources: ['mutex_protected.h'],
    dependencies: mutex_protected_cpp_lib_dependencies
)
# === @end: mutex_protected_cpp_lib (@original: mutex_protected) ===


# === @begin: sequencer_cpp_lib (@original: sequencer) ===
# Header-only
sequencer_cpp_lib_dependencies = [
    # Eugo-managed
    absl_container_flat_hash_map,
    absl_synchronization,
]

sequencer_cpp_lib_dep = declare_dependency(
    sources: ['sequencer.h'],
    dependencies: sequencer_cpp_lib_dependencies
)
# === @end: sequencer_cpp_lib (@original: sequencer) ===


# === @begin: time_cpp_lib (@original: time) ===
time_cpp_lib = static_library(
    'time_cpp_lib',
    ['time.cc'],
    install: false
)

time_cpp_lib_dep = declare_dependency(
    link_with: [time_cpp_lib]
)
# === @end: time_cpp_lib (@original: time) ===


# === @begin: memory_cpp_lib (@original: memory) ===
memory_cpp_lib = static_library(
    'memory_cpp_lib',
    ['memory.cc'],
    install: false
)

memory_cpp_lib_dep = declare_dependency(
    link_with: [memory_cpp_lib]
)
# === @end: memory_cpp_lib (@original: memory) ===


# === @begin: thread_checker_cpp_lib (@original: thread_checker) ===
thread_checker_cpp_lib = static_library(
    'thread_checker_cpp_lib',
    ['thread_checker.cc'],
    install: false
)

thread_checker_cpp_lib_dep = declare_dependency(
    link_with: [thread_checker_cpp_lib]
)
# === @end: thread_checker_cpp_lib (@original: thread_checker) ===


# ============================================================
# Level 1: Depends on Level 0 targets only
# ============================================================


# === @begin: thread_utils_cpp_lib (@original: thread_utils) ===
# Header-only
thread_utils_cpp_lib_dependencies = [
    # Package-managed
    thread_checker_cpp_lib_dep,
]

thread_utils_cpp_lib_dep = declare_dependency(
    sources: ['thread_utils.h'],
    dependencies: thread_utils_cpp_lib_dependencies
)
# === @end: thread_utils_cpp_lib (@original: thread_utils) ===


# === @begin: concurrent_flat_map_cpp_lib (@original: concurrent_flat_map) ===
# Header-only
concurrent_flat_map_cpp_lib_dependencies = [
    # Package-managed
    mutex_protected_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
]

concurrent_flat_map_cpp_lib_dep = declare_dependency(
    sources: ['concurrent_flat_map.h'],
    dependencies: concurrent_flat_map_cpp_lib_dependencies
)
# === @end: concurrent_flat_map_cpp_lib (@original: concurrent_flat_map) ===


# ============================================================
# Level 2: logging — most util targets depend on this
# ============================================================


# === @begin: logging_cpp_lib (@original: logging) ===
logging_cpp_lib_dependencies = [
    # Package-managed
    macros_cpp_lib_dep,
    thread_utils_cpp_lib_dep,

    # Eugo-managed
    spdlog,
    absl_debugging_failure_signal_handler,
    absl_strings,  # covers both absl/strings and absl/strings:str_format (same Meson variable per AGENTS.md §10)
    # @com_google_googletest//:gtest_prod
]

logging_cpp_lib = static_library(
    'logging_cpp_lib',
    ['logging.cc'],
    dependencies: logging_cpp_lib_dependencies,
    install: false
)

logging_cpp_lib_dep = declare_dependency(
    link_with: [logging_cpp_lib],
    dependencies: logging_cpp_lib_dependencies
)
# === @end: logging_cpp_lib (@original: logging) ===


# ============================================================
# Level 3: Targets depending on logging and/or externals
# ============================================================

# --- Header-only targets depending on logging ---

# === @begin: graceful_shutdown_cpp_lib (@original: graceful_shutdown) ===
# Header-only
graceful_shutdown_cpp_lib_dependencies = [
    # Package-managed
    logging_cpp_lib_dep,

    # Eugo-managed
    absl_time,
]

graceful_shutdown_cpp_lib_dep = declare_dependency(
    sources: ['graceful_shutdown.h'],
    dependencies: graceful_shutdown_cpp_lib_dependencies
)
# === @end: graceful_shutdown_cpp_lib (@original: graceful_shutdown) ===


# === @begin: container_util_cpp_lib (@original: container_util) ===
# Header-only
container_util_cpp_lib_dependencies = [
    # Package-managed
    logging_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
    absl_container_flat_hash_set,
    absl_container_inlined_vector,
]

container_util_cpp_lib_dep = declare_dependency(
    sources: ['container_util.h'],
    dependencies: container_util_cpp_lib_dependencies
)
# === @end: container_util_cpp_lib (@original: container_util) ===


# === @begin: counter_map_cpp_lib (@original: counter_map) ===
# Header-only
counter_map_cpp_lib_dependencies = [
    # Package-managed
    logging_cpp_lib_dep,
    mutex_protected_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
    absl_container_flat_hash_set,
]

counter_map_cpp_lib_dep = declare_dependency(
    sources: ['counter_map.h'],
    dependencies: counter_map_cpp_lib_dependencies
)
# === @end: counter_map_cpp_lib (@original: counter_map) ===


# === @begin: shared_lru_cpp_lib (@original: shared_lru) ===
# Header-only
shared_lru_cpp_lib_dependencies = [
    # Package-managed
    logging_cpp_lib_dep,
    map_utils_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
]

shared_lru_cpp_lib_dep = declare_dependency(
    sources: ['shared_lru.h'],
    dependencies: shared_lru_cpp_lib_dependencies
)
# === @end: shared_lru_cpp_lib (@original: shared_lru) ===


# --- Static library targets (no common/ deps) ---

# === @begin: env_cpp_lib (@original: env) ===
env_cpp_lib_dependencies = [
    # Package-managed
    logging_cpp_lib_dep,

    # Eugo-managed
    absl_strings,
]

env_cpp_lib = static_library(
    'env_cpp_lib',
    ['env.cc'],
    dependencies: env_cpp_lib_dependencies,
    install: false
)

env_cpp_lib_dep = declare_dependency(
    link_with: [env_cpp_lib],
    dependencies: env_cpp_lib_dependencies
)
# === @end: env_cpp_lib (@original: env) ===


# === @begin: exponential_backoff_cpp_lib (@original: exponential_backoff) ===
exponential_backoff_cpp_lib_dependencies = [
    # Package-managed
    logging_cpp_lib_dep,
]

exponential_backoff_cpp_lib = static_library(
    'exponential_backoff_cpp_lib',
    ['exponential_backoff.cc'],
    dependencies: exponential_backoff_cpp_lib_dependencies,
    install: false
)

exponential_backoff_cpp_lib_dep = declare_dependency(
    link_with: [exponential_backoff_cpp_lib],
    dependencies: exponential_backoff_cpp_lib_dependencies
)
# === @end: exponential_backoff_cpp_lib (@original: exponential_backoff) ===


# === @begin: scoped_env_setter_cpp_lib (@original: scoped_env_setter) ===
scoped_env_setter_cpp_lib_dependencies = [
    # Package-managed
    env_cpp_lib_dep,
]

scoped_env_setter_cpp_lib = static_library(
    'scoped_env_setter_cpp_lib',
    ['scoped_env_setter.cc'],
    dependencies: scoped_env_setter_cpp_lib_dependencies,
    install: false
)

scoped_env_setter_cpp_lib_dep = declare_dependency(
    link_with: [scoped_env_setter_cpp_lib],
    dependencies: scoped_env_setter_cpp_lib_dependencies
)
# === @end: scoped_env_setter_cpp_lib (@original: scoped_env_setter) ===


# --- Static library targets WITH common/ dependencies ---
# NOTE: common/ is processed AFTER util/ due to circular dependency.
# Common deps (status, status_or, id, ray_config) are listed in comments.
# Headers resolve via global include path; symbols resolve at link time.

# === @begin: filesystem_cpp_lib (@original: filesystem) ===
# Bazel deps: //src/ray/common:status_or
filesystem_cpp_lib_dependencies = [
    # Package-managed
    # status_or_cpp_lib_dep,  # `status_or_cpp_lib_dep` is NOT AVAILABLE (processed after): common/ processed after util/. See comments at the top of this file.
]

filesystem_cpp_lib = static_library(
    'filesystem_cpp_lib',
    ['filesystem.cc'],
    dependencies: filesystem_cpp_lib_dependencies,
    install: false
)

filesystem_cpp_lib_dep = declare_dependency(
    link_with: [filesystem_cpp_lib],
    dependencies: filesystem_cpp_lib_dependencies
)
# === @end: filesystem_cpp_lib (@original: filesystem) ===


# === @begin: file_persistence_cpp_lib (@original: file_persistence) ===
# Bazel deps: //src/ray/common:status, //src/ray/common:status_or
file_persistence_cpp_lib_dependencies = [
    # Package-managed
    # status_cpp_lib_dep,    `status_cpp_lib_dep`  # is NOT AVAILABLE (processed after): common/ processed after util/. See comments at the top of this file.
    # status_or_cpp_lib_dep,  # `status_or_cpp_lib_dep` is NOT AVAILABLE (processed after): common/ processed after util/. See comments at the top of this file.
]

file_persistence_cpp_lib = static_library(
    'file_persistence_cpp_lib',
    ['file_persistence.cc'],
    dependencies: file_persistence_cpp_lib_dependencies,
    install: false
)

file_persistence_cpp_lib_dep = declare_dependency(
    link_with: [file_persistence_cpp_lib],
    dependencies: file_persistence_cpp_lib_dependencies
)
# === @end: file_persistence_cpp_lib (@original: file_persistence) ===


# === @begin: string_utils_cpp_lib (@original: string_utils) ===
# Bazel deps: //src/ray/common:status_or
string_utils_cpp_lib_dependencies = [
    # Package-managed
    # status_or_cpp_lib_dep,  # `status_or_cpp_lib_dep` is NOT AVAILABLE (processed after): common/ processed after util/. See comments at the top of this file.

    # Eugo-managed
    absl_strings,  # covers both absl/strings and absl/strings:str_format (same Meson variable per AGENTS.md §10)
]

string_utils_cpp_lib = static_library(
    'string_utils_cpp_lib',
    ['string_utils.cc'],
    dependencies: string_utils_cpp_lib_dependencies,
    install: false
)

string_utils_cpp_lib_dep = declare_dependency(
    link_with: [string_utils_cpp_lib],
    dependencies: string_utils_cpp_lib_dependencies
)
# === @end: string_utils_cpp_lib (@original: string_utils) ===


# === @begin: compat_cpp_lib (@original: compat) ===
# Bazel deps: //src/ray/common:status
compat_cpp_lib_dependencies = [
    # Package-managed
    logging_cpp_lib_dep,
    # status_cpp_lib_dep,  # `status_cpp_lib_dep` is NOT AVAILABLE (processed after) `common/` processed after `util/`. See comments at the top of this file.
]

compat_cpp_lib = static_library(
    'compat_cpp_lib',
    ['compat.cc'],
    dependencies: compat_cpp_lib_dependencies,
    install: false
)

compat_cpp_lib_dep = declare_dependency(
    link_with: [compat_cpp_lib],
    dependencies: compat_cpp_lib_dependencies
)
# === @end: compat_cpp_lib (@original: compat) ===


# === @begin: cmd_line_utils_cpp_lib (@original: cmd_line_utils) ===
cmd_line_utils_cpp_lib_dependencies = [
    # Package-managed
    logging_cpp_lib_dep,
    string_utils_cpp_lib_dep,
]

cmd_line_utils_cpp_lib = static_library(
    'cmd_line_utils_cpp_lib',
    ['cmd_line_utils.cc'],
    dependencies: cmd_line_utils_cpp_lib_dependencies,
    install: false
)

cmd_line_utils_cpp_lib_dep = declare_dependency(
    link_with: [cmd_line_utils_cpp_lib],
    dependencies: cmd_line_utils_cpp_lib_dependencies
)
# === @end: cmd_line_utils_cpp_lib (@original: cmd_line_utils) ===


# === @begin: path_utils_cpp_lib (@original: path_utils) ===
path_utils_cpp_lib_dependencies = [
    # Eugo-managed
    absl_strings,
]

path_utils_cpp_lib = static_library(
    'path_utils_cpp_lib',
    ['path_utils.cc'],
    dependencies: path_utils_cpp_lib_dependencies,
    install: false
)

path_utils_cpp_lib_dep = declare_dependency(
    link_with: [path_utils_cpp_lib],
    dependencies: path_utils_cpp_lib_dependencies
)
# === @end: path_utils_cpp_lib (@original: path_utils) ===


# ============================================================
# Level 4: Targets depending on Level 3 targets
# ============================================================

# --- Header-only targets depending on compat ---

# === @begin: process_interface_cpp_lib (@original: process_interface) ===
# Header-only
process_interface_cpp_lib_dependencies = [
    # Package-managed
    compat_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
]

process_interface_cpp_lib_dep = declare_dependency(
    sources: ['process_interface.h'],
    dependencies: process_interface_cpp_lib_dependencies
)
# === @end: process_interface_cpp_lib (@original: process_interface) ===


# === @begin: spdlog_fd_sink_cpp_lib (@original: spdlog_fd_sink) ===
# Header-only
# Bazel deps: //src/ray/common:status
spdlog_fd_sink_cpp_lib_dependencies = [
    # Package-managed
    compat_cpp_lib_dep,
    # status_cpp_lib_dep,  # `status_cpp_lib_dep` is NOT AVAILABLE (processed after): common/ processed after util/. See comments at the top of this file.

    # Eugo-managed
    spdlog,
]

spdlog_fd_sink_cpp_lib_dep = declare_dependency(
    sources: ['spdlog_fd_sink.h'],
    dependencies: spdlog_fd_sink_cpp_lib_dependencies
)
# === @end: spdlog_fd_sink_cpp_lib (@original: spdlog_fd_sink) ===


# === @begin: spdlog_newliner_sink_cpp_lib (@original: spdlog_newliner_sink) ===
# Header-only
spdlog_newliner_sink_cpp_lib_dependencies = [
    # Package-managed
    compat_cpp_lib_dep,

    # Eugo-managed
    spdlog,
]

spdlog_newliner_sink_cpp_lib_dep = declare_dependency(
    sources: ['spdlog_newliner_sink.h'],
    dependencies: spdlog_newliner_sink_cpp_lib_dependencies
)
# === @end: spdlog_newliner_sink_cpp_lib (@original: spdlog_newliner_sink) ===


# --- Static library targets ---

# === @begin: scoped_dup2_wrapper_cpp_lib (@original: scoped_dup2_wrapper) ===
scoped_dup2_wrapper_cpp_lib_dependencies = [
    # Package-managed
    compat_cpp_lib_dep,
    logging_cpp_lib_dep,
]

scoped_dup2_wrapper_cpp_lib = static_library(
    'scoped_dup2_wrapper_cpp_lib',
    ['scoped_dup2_wrapper.cc'],
    dependencies: scoped_dup2_wrapper_cpp_lib_dependencies,
    install: false
)

scoped_dup2_wrapper_cpp_lib_dep = declare_dependency(
    link_with: [scoped_dup2_wrapper_cpp_lib],
    dependencies: scoped_dup2_wrapper_cpp_lib_dependencies
)
# === @end: scoped_dup2_wrapper_cpp_lib (@original: scoped_dup2_wrapper) ===


# === @begin: network_util_cpp_lib (@original: network_util) ===
network_util_cpp_lib_dependencies = [
    # Package-managed
    filesystem_cpp_lib_dep,
    string_utils_cpp_lib_dep,

    # Eugo-managed
    boost_asio,
    gflags,
    absl_container_flat_hash_map,
    absl_strings, # covers both absl/strings and absl/strings:str_format (same Meson variable per AGENTS.md §10)
]

network_util_cpp_lib = static_library(
    'network_util_cpp_lib',
    ['network_util.cc'],
    dependencies: network_util_cpp_lib_dependencies,
    install: false
)

network_util_cpp_lib_dep = declare_dependency(
    link_with: [network_util_cpp_lib],
    dependencies: network_util_cpp_lib_dependencies
)
# === @end: network_util_cpp_lib (@original: network_util) ===


# === @begin: process_utils_cpp_lib (@original: process_utils) ===
# Bazel deps: select linux: //src/ray/util:filesystem
process_utils_cpp_lib_dependencies = [
    # Package-managed
    compat_cpp_lib_dep,
    logging_cpp_lib_dep,
    filesystem_cpp_lib_dep,  # Linux-only, but no need for a platform-specific target since it has no transitive deps and we only care about Eugo-env

    # Eugo-managed
    absl_strings,  # covers both absl/strings and absl/strings:str_format (same Meson variable per AGENTS.md §10)
]

process_utils_cpp_lib = static_library(
    'process_utils_cpp_lib',
    ['process_utils.cc'],
    dependencies: process_utils_cpp_lib_dependencies,
    install: false
)

process_utils_cpp_lib_dep = declare_dependency(
    link_with: [process_utils_cpp_lib],
    dependencies: process_utils_cpp_lib_dependencies
)
# === @end: process_utils_cpp_lib (@original: process_utils) ===


# === @begin: port_persistence_cpp_lib (@original: port_persistence) ===
# Bazel deps: //src/ray/common:id, //src/ray/common:status, //src/ray/common:status_or
port_persistence_cpp_lib_dependencies = [
    # Package-managed
    file_persistence_cpp_lib_dep,
    # id_cpp_lib_dep,    `id_cpp_lib_dep`      # is NOT AVAILABLE (processed after): common/ processed after util/. See comments at the top of this file.
    # status_cpp_lib_dep,    `status_cpp_lib_dep`  # is NOT AVAILABLE (processed after): common/ processed after util/. See comments at the top of this file.
    # status_or_cpp_lib_dep,  # `status_or_cpp_lib_dep` is NOT AVAILABLE (processed after): common/ processed after util/. See comments at the top of this file.
]

port_persistence_cpp_lib = static_library(
    'port_persistence_cpp_lib',
    ['port_persistence.cc'],
    dependencies: port_persistence_cpp_lib_dependencies,
    install: false
)

port_persistence_cpp_lib_dep = declare_dependency(
    link_with: [port_persistence_cpp_lib],
    dependencies: port_persistence_cpp_lib_dependencies
)
# === @end: port_persistence_cpp_lib (@original: port_persistence) ===


# === @begin: temporary_directory_cpp_lib (@original: temporary_directory) ===
# Bazel deps: //src/ray/common:id
temporary_directory_cpp_lib_dependencies = [
    # Package-managed
    # id_cpp_lib_dep,  # `id_cpp_lib_dep` is NOT AVAILABLE (processed after): common/ processed after util/. See comments at the top of this file.

    # Eugo-managed
    absl_strings,  # covers both absl/strings and absl/strings:str_format (same Meson variable per AGENTS.md §10)
]

temporary_directory_cpp_lib = static_library(
    'temporary_directory_cpp_lib',
    ['temporary_directory.cc'],
    dependencies: temporary_directory_cpp_lib_dependencies,
    install: false
)

temporary_directory_cpp_lib_dep = declare_dependency(
    link_with: [temporary_directory_cpp_lib],
    dependencies: temporary_directory_cpp_lib_dependencies
)
# === @end: temporary_directory_cpp_lib (@original: temporary_directory) ===


# ============================================================
# Level 5: Higher-level targets
# ============================================================


# === @begin: fake_process_cpp_lib (@original: fake_process) ===
# Header-only
fake_process_cpp_lib_dependencies = [
    # Package-managed
    process_interface_cpp_lib_dep,
]

fake_process_cpp_lib_dep = declare_dependency(
    sources: ['fake_process.h'],
    dependencies: fake_process_cpp_lib_dependencies
)
# === @end: fake_process_cpp_lib (@original: fake_process) ===


# === @begin: process_cpp_lib (@original: process) ===
# Bazel deps: //src/ray/common:status, //src/ray/common:status_or
process_cpp_lib_dependencies = [
    # Package-managed
    cmd_line_utils_cpp_lib_dep,
    compat_cpp_lib_dep,
    filesystem_cpp_lib_dep,
    logging_cpp_lib_dep,
    macros_cpp_lib_dep,
    process_interface_cpp_lib_dep,
    process_utils_cpp_lib_dep,
    # status_cpp_lib_dep,    `status_cpp_lib_dep`  # is NOT AVAILABLE (processed after): common/ processed after util/. See comments at the top of this file.
    # status_or_cpp_lib_dep,  # `status_or_cpp_lib_dep` is NOT AVAILABLE (processed after): common/ processed after util/. See comments at the top of this file.

    # Eugo-managed
    boost_asio,
    absl_container_flat_hash_set,
    absl_strings,
    absl_synchronization,
]

process_cpp_lib = static_library(
    'process_cpp_lib',
    [
        'process.cc',
        'subreaper.cc'
    ],
    dependencies: process_cpp_lib_dependencies,
    install: false
)

process_cpp_lib_dep = declare_dependency(
    link_with: [process_cpp_lib],
    dependencies: process_cpp_lib_dependencies
)
# === @end: process_cpp_lib (@original: process) ===


# === @begin: pipe_logger_cpp_lib (@original: pipe_logger) ===
# Bazel deps: //src/ray/common:ray_config, //src/ray/common:status
pipe_logger_cpp_lib_dependencies = [
    # Package-managed
    compat_cpp_lib_dep,
    logging_cpp_lib_dep,
    spdlog_fd_sink_cpp_lib_dep,
    spdlog_newliner_sink_cpp_lib_dep,
    stream_redirection_options_cpp_lib_dep,
    thread_utils_cpp_lib_dep,
    # ray_config_cpp_lib_dep,  # `ray_config_cpp_lib_dep` is NOT AVAILABLE (processed after): common/ processed after util/. See comments at the top of this file.
    # status_cpp_lib_dep,    `status_cpp_lib_dep`   # is NOT AVAILABLE (processed after): common/ processed after util/. See comments at the top of this file.

    # Eugo-managed
    boost_iostreams,
    spdlog,
    absl_container_inlined_vector,
    absl_strings, # covers both absl/strings and absl/strings:str_format (same Meson variable per AGENTS.md §10)
    absl_synchronization,
]

pipe_logger_cpp_lib = static_library(
    'pipe_logger_cpp_lib',
    ['pipe_logger.cc'],
    dependencies: pipe_logger_cpp_lib_dependencies,
    install: false
)

pipe_logger_cpp_lib_dep = declare_dependency(
    link_with: [pipe_logger_cpp_lib],
    dependencies: pipe_logger_cpp_lib_dependencies
)
# === @end: pipe_logger_cpp_lib (@original: pipe_logger) ===


# === @begin: event_cpp_lib (@original: event) ===
event_cpp_lib_dependencies = [
    # Package-managed
    logging_cpp_lib_dep,
    random_cpp_lib_dep,
    string_utils_cpp_lib_dep,
    time_cpp_lib_dep,
    event_proto_cpp_lib_dep,
    export_event_proto_cpp_lib_dep,

    # Eugo-managed
    boost_asio,
    spdlog,
    absl_container_flat_hash_map,
    # @com_google_googletest//:gtest_prod
    protobuf,
    nlohmann_json,
]

event_cpp_lib = static_library(
    'event_cpp_lib',
    ['event.cc'],
    dependencies: event_cpp_lib_dependencies,
    install: false
)

event_cpp_lib_dep = declare_dependency(
    link_with: [event_cpp_lib],
    dependencies: event_cpp_lib_dependencies
)
# === @end: event_cpp_lib (@original: event) ===


# ============================================================
# Level 6: internal/ subdirectory
# ============================================================

subdir('internal/')


# ============================================================
# Level 7: Depends on internal/ targets
# ============================================================


# === @begin: stream_redirection_cpp_lib (@original: stream_redirection) ===
stream_redirection_cpp_lib_dependencies = [
    # Package-managed
    pipe_logger_cpp_lib_dep,
    scoped_dup2_wrapper_cpp_lib_dep,
    stream_redirection_options_cpp_lib_dep,
    stream_redirection_handle_cpp_lib_dep,

    # Eugo-managed
    absl_container_flat_hash_map,
]

stream_redirection_cpp_lib = static_library(
    'stream_redirection_cpp_lib',
    ['stream_redirection.cc'],
    dependencies: stream_redirection_cpp_lib_dependencies,
    install: false
)

stream_redirection_cpp_lib_dep = declare_dependency(
    link_with: [stream_redirection_cpp_lib],
    dependencies: stream_redirection_cpp_lib_dependencies
)
# === @end: stream_redirection_cpp_lib (@original: stream_redirection) ===


# ============================================================
# Aggregate: util_cpp_lib_dep — backward-compatible wrapper
# Other meson.build files reference util_cpp_lib_dep; this wraps
# all individual targets into a single convenience dependency.
# ============================================================
util_cpp_lib_dep = declare_dependency(
    dependencies: [
        # Header-only
        visibility_cpp_lib_dep,
        macros_cpp_lib_dep,
        array_cpp_lib_dep,
        type_traits_cpp_lib_dep,
        raii_cpp_lib_dep,
        size_literals_cpp_lib_dep,
        map_utils_cpp_lib_dep,
        stream_redirection_options_cpp_lib_dep,
        random_cpp_lib_dep,
        mutex_protected_cpp_lib_dep,
        sequencer_cpp_lib_dep,
        thread_utils_cpp_lib_dep,
        concurrent_flat_map_cpp_lib_dep,
        graceful_shutdown_cpp_lib_dep,
        container_util_cpp_lib_dep,
        counter_map_cpp_lib_dep,
        shared_lru_cpp_lib_dep,
        process_interface_cpp_lib_dep,
        fake_process_cpp_lib_dep,
        spdlog_fd_sink_cpp_lib_dep,
        spdlog_newliner_sink_cpp_lib_dep,
        # Static libraries
        time_cpp_lib_dep,
        memory_cpp_lib_dep,
        thread_checker_cpp_lib_dep,
        logging_cpp_lib_dep,
        env_cpp_lib_dep,
        exponential_backoff_cpp_lib_dep,
        scoped_env_setter_cpp_lib_dep,
        filesystem_cpp_lib_dep,
        file_persistence_cpp_lib_dep,
        string_utils_cpp_lib_dep,
        compat_cpp_lib_dep,
        cmd_line_utils_cpp_lib_dep,
        path_utils_cpp_lib_dep,
        scoped_dup2_wrapper_cpp_lib_dep,
        network_util_cpp_lib_dep,
        process_utils_cpp_lib_dep,
        process_cpp_lib_dep,
        port_persistence_cpp_lib_dep,
        temporary_directory_cpp_lib_dep,
        pipe_logger_cpp_lib_dep,
        event_cpp_lib_dep,
        stream_redirection_cpp_lib_dep,
        stream_redirection_handle_cpp_lib_dep,
    ]
)
