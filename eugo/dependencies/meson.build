# === @begin: absl ===
# @Important:
# We import those guys separately to avoid linking every binary to every absl library, as there are hundreds of them.
absl_base_core_headers = dependency('absl', method: 'cmake', modules: ['absl::core_headers'])

absl_time = dependency('absl', method: 'cmake', modules: ['absl::time'])

absl_types_optional = dependency('absl', method: 'cmake', modules: ['absl::optional'])

absl_container_flat_hash_map = dependency('absl', method: 'cmake', modules: ['absl::flat_hash_map'])
absl_container_flat_hash_set = dependency('absl', method: 'cmake', modules: ['absl::flat_hash_set'])
absl_container_node_hash_map = dependency('absl', method: 'cmake', modules: ['absl::node_hash_map'])
absl_container_btree = dependency('absl', method: 'cmake', modules: ['absl::btree'])

absl_random_random = dependency('absl', method: 'cmake', modules: ['absl::random_random'])
absl_random_bit_gen_ref = dependency('absl', method: 'cmake', modules: ['absl::random_bit_gen_ref'])

absl_synchronization = dependency('absl', method: 'cmake', modules: ['absl::synchronization'])

absl_cleanup = dependency('absl', method: 'cmake', modules: ['absl::cleanup'])

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
boost = dependency('boost', modules: ['fiber', 'thread'])
boost_any = boost
boost_asio = boost
boost_beast = boost
boost_bimap = boost
boost_circular_buffer = boost
boost_fiber = boost
boost_system = boost
boost_thread = boost
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
protoc_cpp = find_program('protoc')
# === @end: protobuf ===


# === @begin: prometheus_cpp ===
prometheus_cpp_pull = dependency('prometheus-cpp', method: 'cmake', modules: ['prometheus-cpp::pull'])
# === @end: prometheus_cpp ===


# === @begin: grpc ===
# Depend on `protobuf`, so the same caveat applies
grpc = dependency('gRPC', method: 'cmake', modules: [
  'gRPC::gpr',
  'gRPC::grpc',
  'gRPC::address_sorting',
  'gRPC::grpc++',
  'gRPC::grpc++_reflection',
  'gRPC::grpcpp_channelz',
  'gRPC::grpc_opencensus_plugin'
])
#'utf8_range::utf8_validity',
#'utf8_range::utf8_range',
#'gRPC::grpc_unsecure',
#'gRPC::upb_base_lib',
#'gRPC::upb_json_lib',
#'gRPC::upb_mem_lib',
#'gRPC::upb_message_lib',
#'gRPC::upb_mini_descriptor_lib',
#'gRPC::upb_textformat_lib',
#'gRPC::upb_wire_lib',
#'gRPC::utf8_range_lib',
#'gRPC::grpc++_alts',
#'gRPC::grpc++_error_details',
#'gRPC::grpc++_unsecure',
#'gRPC::grpc_authorization_provider',
#'gRPC::grpc_plugin_support',
#'gRPC::grpc_cpp_plugin',
#'gRPC::grpc_python_plugin'
# === @end: grpc ===


# === @begin: nlohmann_json ===
nlohmann_json = dependency('nlohmann_json', method: 'cmake')
# === @end: nlohmann_json ===


# === @begin: gflags ===
gflags = dependency('gflags', method: 'cmake')
# === @end: gflags ===


# === @begin: hiredis ===
hiredis = dependency('hiredis', method: 'cmake')
hiredis_ssl = dependency('hiredis_ssl', method: 'cmake')
# === @end: gflags ===


# === @begin: threads ===
threads = dependency('threads')
# === @end: threads ===


# === @begin: nodejs ===
npm = find_program('npm')
# === @end: nodejs ===


# === @begin: git ===
git = find_program('git')
# === @end: git ===
