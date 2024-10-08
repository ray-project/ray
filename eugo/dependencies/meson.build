# === @begin: absl ===
# @Important:
# We import those guys separately to avoid linking every binary to every absl library, as there are hundreds of them.
absl_base_core_headers = dependency('absl', method: 'cmake', modules: ['absl::core_headers'])

absl_time = dependency('absl', method: 'cmake', modules: ['absl::time'])

absl_types_optional = dependency('absl', method: 'cmake', modules: ['absl::optional'])

absl_container_flat_hash_map = dependency('absl', method: 'cmake', modules: ['absl::flat_hash_map'])
absl_container_flat_hash_set = dependency('absl', method: 'cmake', modules: ['absl::flat_hash_set'])
absl_container_btree = dependency('absl', method: 'cmake', modules: ['absl::btree'])

absl_random = dependency('absl', method: 'cmake', modules: ['absl::random_random'])
absl_synchronization = dependency('absl', method: 'cmake', modules: ['absl::synchronization'])

absl_debugging_failure_signal_handler = dependency('absl', method: 'cmake', modules: ['absl::failure_signal_handler'])
absl_debugging_stacktrace = dependency('absl', method: 'cmake', modules: ['absl::stacktrace'])
absl_debugging_symbolize = dependency('absl', method: 'cmake', modules: ['absl::symbolize'])

absl_algorithm = dependency('absl', method: 'cmake', modules: ['absl::algorithm'])
absl_strings = dependency('absl', method: 'cmake', modules: ['absl::strings'])
absl_memory = dependency('absl', method: 'cmake', modules: ['absl::memory'])
# === @end: absl ===


# === @begin: boost ===
# https://github.com/mesonbuild/meson/issues/5438
# For now, modules resolution for Boost is imperfect so we have to bring the entire boost.
# Anyway, bringing standalone parts would bring almost all others so not a big problem
boost = dependency('boost', method: 'cmake')
boost_asio = boost
# === @end: boost ===


# === @begin: spdlog ===
spdlog = dependency('spdlog', method: 'cmake')
# === @end: spdlog ===


# === @begin: msgpack_cxx ===
msgpack_cxx = dependency('msgpack-cxx', method: 'cmake')
# === @end: msgpack_cxx ===


# === @begin: flatbuffers ===
flatbuffers = dependency('flatbuffers', method: 'cmake')
flatc = find_program('flatc')
# === @end: flatbuffers ===


# === @begin: opencensus_cpp ===
opencensus_cpp_prometheus_exporter = dependency('OpenCensus', method: 'cmake', modules: ['opencensus-cpp::exporters_stats_prometheus'])
opencensus_cpp_stdout_exporter = dependency('OpenCensus', method: 'cmake', modules: ['opencensus-cpp::exporters_stats_stdout'])
opencensus_cpp_stats = dependency('OpenCensus', method: 'cmake', modules: ['opencensus-cpp::stats'])
opencensus_cpp_tags = dependency('OpenCensus', method: 'cmake', modules: ['opencensus-cpp::tags'])
# === @end: opencensus_cpp ===


# === @begin: protobuf ===
# If not disabling `pkg-config` search, this causes the dependency cycle and infinite execution
protobuf = dependency('protobuf', modules: ['protobuf::libprotobuf'], method: 'cmake')
protoc = find_program('protoc')
# === @end: protobuf ===


# === @begin: prometheus_cpp ===
prometheus_cpp_pull = dependency('prometheus-cpp', method: 'cmake', modules: ['prometheus-cpp::pull'])
# === @end: prometheus_cpp ===


# === @begin: grpc ===
# Depend on `protobuf`, so the same caveat applies
grpc = dependency('gRPC', method: 'cmake')
# === @end: grpc ===


# === @begin: nlohmann_json ===
nlohmann_json = dependency('nlohmann_json', method: 'cmake')
# === @end: nlohmann_json ===


# === @begin: threads ===
threads = dependency('threads')
# === @begin: threads ===
