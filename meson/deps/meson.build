#absl = dependency('absl', method: 'cmake', modules: [
#      #'absl::raw_logging_internal', # https://github.com/ray-project/ray/issues/8588, https://github.com/ray-project/ray/issues/17212
#
#      'absl::config', # @TODO+:not sure if it's really used
#
#      # `#include "absl/base/*"`
#      'absl::core_headers', # https://github.com/ray-project/ray/blob/7591f91832872fc4606dd54641bb2b781de170a0/BUILD.bazel#L565
#      #    'absl::base_internal',
#      'absl::base', # https://github.com/ray-project/ray/blob/7591f91832872fc4606dd54641bb2b781de170a0/BUILD.bazel#L565
#
#      # `#include "absl/types/optional.h"`
#      'absl::optional',
#      #    'absl::bad_optional_access',
#
#      # `#include "absl/random/*"`
#      'absl::random_random',
#      #    'absl::random_bit_gen_ref',
#      #    'absl::random_seed_gen_exception',
#      #    'absl::random_seed_sequences',
#      #    'absl::random_distributions',
#      #    'absl::random_internal_mock_helpers',
#      #    'absl::random_internal_traits',
#      #    'absl::random_internal_distribution_caller',
#      #    'absl::random_internal_fast_uniform_bits',
#      #    'absl::random_internal_seed_material',
#      #    'absl::random_internal_pool_urbg',
#      #    'absl::random_internal_salted_seed_seq',
#      #    'absl::random_internal_iostream_state_saver',
#      #    'absl::random_internal_generate_real',
#      #    'absl::random_internal_wide_multiply',
#      #    'absl::random_internal_fastmath',
#      #    'absl::random_internal_nonsecure_base',
#      #    'absl::random_internal_pcg_engine',
#      #    'absl::random_internal_randen_engine',
#      #    'absl::random_internal_platform',
#      #    'absl::random_internal_randen',
#      #    'absl::random_internal_randen_slow',
#      #    'absl::random_internal_randen_hwaes',
#      #    'absl::random_internal_randen_hwaes_impl',
#      #    'absl::random_internal_distribution_test_util',
#      #    'absl::random_internal_uniform_helper',
#
#      # `#include "absl/container/*"`
#      'absl::btree',
#      'absl::flat_hash_map',
#      'absl::flat_hash_set',
#      'absl::node_hash_map',
#      'absl::node_hash_set',
#
#      # `#include "absl/cleanup/*"`
#      'absl::cleanup',
#      #    'absl::cleanup_internal',
#
#      # `#include "absl/memory/*"`
#      'absl::memory',
#
#      # `#include "absl/flags/*"`
#      'absl::flags',
#      #    'absl::flags_path_util',
#      #    'absl::flags_program_name',
#      #    'absl::flags_config',
#      #    'absl::flags_marshalling',
#      #    'absl::flags_commandlineflag_internal',
#      #    'absl::flags_commandlineflag',
#      #    'absl::flags_private_handle_accessor',
#      #    'absl::flags_reflection',
#      #    'absl::flags_internal',
#      #    'absl::flags_usage_internal',
#      #    'absl::flags_usage',
#      #    'absl::flags_parse',
#
#      # `#include "absl/strings/*"`
#      'absl::strings',
#      'absl::strings_internal',
#
#      # `#include "absl/time/*"`
#      'absl::time',
#      #    'absl::civil_time',
#      #    'absl::time_zone'
#
#      # `#include "absl/hash/*"`
#      'absl::hash',
#
#      # `#include "absl/synchronization/*"`
#      'absl::synchronization',
#
#      # `#include "absl/functional/*"`
#
#      #    'absl::endian',
#      #    'absl::scoped_set_env',
#      #    'absl::prefetch',
#      #    'absl::algorithm',
#      #    'absl::algorithm_container',
#      #    'absl::compressed_tuple',
#      #    'absl::fixed_array',
#      #    'absl::inlined_vector_internal',
#      #    'absl::inlined_vector',
#      #    'absl::counting_allocator',
#      #    'absl::container_memory',
#      #    'absl::hash_function_defaults',
#      #    'absl::hash_policy_traits',
#      #    'absl::hashtablez_sampler',
#      #    'absl::hashtable_debug',
#      #    'absl::hashtable_debug_hooks',
#      #    'absl::node_slot_policy',
#      #    'absl::raw_hash_map',
#      #    'absl::container_common',
#      #    'absl::raw_hash_set',
#      #    'absl::layout',
#      #    'absl::stacktrace',
#      #    'absl::symbolize',
#      #    'absl::examine_stack',
#      #    'absl::failure_signal_handler',
#      #    'absl::debugging_internal',
#      #    'absl::demangle_internal',
#      #    'absl::leak_check',
#      #    'absl::debugging',
#      #    'absl::bind_front',
#      #    'absl::function_ref',
#      #    'absl::city',
#      #    'absl::low_level_hash',
#      #    'absl::type_traits',
#      #    'absl::meta',
#      #    'absl::bits',
#      #    'absl::int128',
#      #    'absl::numeric',
#      #    'absl::numeric_representation',
#      #    'absl::sample_recorder',
#      #    'absl::exponential_biased',
#      #    'absl::periodic_sampler',
#      #    'absl::status',
#      #    'absl::statusor',
#      #    'absl::str_format',
#      #    'absl::str_format_internal',
#      #    'absl::graphcycles_internal',
#      #    'absl::kernel_timeout_internal',
#      #    'absl::span',
#      #    'absl::compare',
#      #    'absl::utility'
#])
# Not yet required:
# - 'absl::atomic_hook', 'absl::errno_saver', 'absl::log_severity', 'absl::spinlock_wait', 'absl::dynamic_annotations',
#   'absl::malloc_internal', 'absl::throw_delegate', 'absl::pretty_function', 'absl::strerror', 'absl::fast_type_id',
# - 'absl::cord', 'absl::cord_internal', 'absl::cordz_update_tracker', 'absl::cordz_functions', 'absl::cordz_info',
#   'absl::cordz_statistics', 'absl::cordz_handle', 'absl::cordz_sample_token', 'absl::cordz_update_scope',
# - 'absl::any', 'absl::bad_any_cast', 'absl::bad_any_cast_impl', 'absl::any_invocable',
# - 'absl::variant', 'absl::bad_variant_access',

# @TODO-:Add module-based approach like for `absl`?
#boost = dependency('boost', method: 'cmake')


# === @begin: msgpack_cxx ===
msgpack_cxx = dependency('msgpack-cxx', method: 'cmake')
# === @end: msgpack_cxx ===


# @TODO+: reimplment asap
#opencensus = dependency('OpenCensus', method: 'cmake', modules: [
#                            'opencensus-cpp::common_hostname',
#                            'opencensus-cpp::common_random',
#                            'opencensus-cpp::common_stats_object',
#                            'opencensus-cpp::common_string_vector_hash',
#                            'opencensus-cpp::common_varint',
#                            'opencensus-cpp::context',
#                            'opencensus-cpp::exporters_stats_prometheus',
#                            'opencensus-cpp::exporters_stats_prometheus_utils',
#                            'opencensus-cpp::exporters_stats_stdout',
#                            'opencensus-cpp::exporters_trace_stdout',
#                            'opencensus-cpp::stats',
#                            'opencensus-cpp::stats_test_utils',
#                            'opencensus-cpp::stats_core',
#                            'opencensus-cpp::stats_recording',
#                            'opencensus-cpp::tags',
#                            'opencensus-cpp::tags_context_util',
#                            'opencensus-cpp::tags_grpc_tags_bin',
#                            'opencensus-cpp::tags_with_tag_map',
#                            'opencensus-cpp::trace',
#                            'opencensus-cpp::trace_b3',
#                            'opencensus-cpp::trace_cloud_trace_context',
#                            'opencensus-cpp::trace_context_util',
#                            'opencensus-cpp::trace_grpc_trace_bin',
#                            'opencensus-cpp::trace_span_context',
#                            'opencensus-cpp::trace_trace_context',
#                            'opencensus-cpp::trace_with_span'
#                        ])

# === @begin: protobuf ===
# If not disabling `pkg-config` search, this causes the dependency cycle and infinite execution
protobuf = dependency('protobuf', modules: ['protobuf::libprotobuf'], method: 'cmake')
# === @end: protobuf ===


# === @begin: grpc ===
# Depend on `protobuf`, so the same caveat applies
grpc = dependency('gRPC', method: 'cmake')
# === @end: grpc ===