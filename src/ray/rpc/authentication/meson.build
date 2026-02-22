# === @begin: authentication_mode_cpp_lib (@original: authentication_mode) ===
authentication_mode_cpp_lib_dependencies = [
    # Package-managed
    ray_config_cpp_lib_dep,

    # Eugo-managed
    absl_strings,
]

authentication_mode_cpp_lib = static_library(
    'authentication_mode_cpp_lib',
    ['authentication_mode.cc'],
    install: false,
    dependencies: authentication_mode_cpp_lib_dependencies
)

authentication_mode_cpp_lib_dep = declare_dependency(
    link_with: [authentication_mode_cpp_lib],
    dependencies: authentication_mode_cpp_lib_dependencies
)
# === @end: authentication_mode_cpp_lib ===


# === @begin: authentication_token_cpp_lib (@original: authentication_token) ===
# Header-only
authentication_token_cpp_lib_dependencies = [
    # Package-managed
    constants_cpp_lib_dep,

    # Eugo-managed
    grpc,
]

authentication_token_cpp_lib_dep = declare_dependency(
    sources: ['authentication_token.h'],
    dependencies: authentication_token_cpp_lib_dependencies
)
# === @end: authentication_token_cpp_lib ===


# === @begin: k8s_constants_cpp_lib (@original: k8s_constants) ===
k8s_constants_cpp_lib_dep = declare_dependency(
    sources: ['k8s_constants.h']
)
# === @end: k8s_constants_cpp_lib ===


# === @begin: k8s_util_cpp_lib (@original: k8s_util) ===
k8s_util_cpp_lib_dependencies = [
    # Package-managed
    authentication_token_cpp_lib_dep,
    k8s_constants_cpp_lib_dep,
    logging_cpp_lib_dep,

    # Eugo-managed
    boost_asio_ssl,
    boost_beast,
    nlohmann_json,
]

k8s_util_cpp_lib = static_library(
    'k8s_util_cpp_lib',
    ['k8s_util.cc'],
    install: false,
    dependencies: k8s_util_cpp_lib_dependencies
)

k8s_util_cpp_lib_dep = declare_dependency(
    link_with: [k8s_util_cpp_lib],
    dependencies: k8s_util_cpp_lib_dependencies
)
# === @end: k8s_util_cpp_lib ===


# === @begin: authentication_token_loader_cpp_lib (@original: authentication_token_loader) ===
authentication_token_loader_cpp_lib_dependencies = [
    # Package-managed
    authentication_mode_cpp_lib_dep,
    authentication_token_cpp_lib_dep,
    k8s_constants_cpp_lib_dep,
    logging_cpp_lib_dep,

    # Eugo-managed
    absl_strings,
    nlohmann_json,
]

authentication_token_loader_cpp_lib = static_library(
    'authentication_token_loader_cpp_lib',
    ['authentication_token_loader.cc'],
    install: false,
    dependencies: authentication_token_loader_cpp_lib_dependencies
)

authentication_token_loader_cpp_lib_dep = declare_dependency(
    link_with: [authentication_token_loader_cpp_lib],
    dependencies: authentication_token_loader_cpp_lib_dependencies
)
# === @end: authentication_token_loader_cpp_lib ===


# === @begin: token_auth_client_interceptor_cpp_lib (@original: token_auth_client_interceptor) ===
token_auth_client_interceptor_cpp_lib_dependencies = [
    # Package-managed
    authentication_token_cpp_lib_dep,
    authentication_token_loader_cpp_lib_dep,
    constants_cpp_lib_dep,

    # Eugo-managed
    grpc,
]

token_auth_client_interceptor_cpp_lib = static_library(
    'token_auth_client_interceptor_cpp_lib',
    ['token_auth_client_interceptor.cc'],
    install: false,
    dependencies: token_auth_client_interceptor_cpp_lib_dependencies
)

token_auth_client_interceptor_cpp_lib_dep = declare_dependency(
    link_with: [token_auth_client_interceptor_cpp_lib],
    dependencies: token_auth_client_interceptor_cpp_lib_dependencies
)
# === @end: token_auth_client_interceptor_cpp_lib ===


# === @begin: authentication_token_validator_cpp_lib (@original: authentication_token_validator) ===
authentication_token_validator_cpp_lib_dependencies = [
    # Package-managed
    authentication_mode_cpp_lib_dep,
    authentication_token_cpp_lib_dep,
    k8s_util_cpp_lib_dep,
]

authentication_token_validator_cpp_lib = static_library(
    'authentication_token_validator_cpp_lib',
    ['authentication_token_validator.cc'],
    install: false,
    dependencies: authentication_token_validator_cpp_lib_dependencies
)

authentication_token_validator_cpp_lib_dep = declare_dependency(
    link_with: [authentication_token_validator_cpp_lib],
    dependencies: authentication_token_validator_cpp_lib_dependencies
)
# === @end: authentication_token_validator_cpp_lib ===


# === @begin: Umbrella dependency — backward compatible with consumers using the consolidated target === #
# Umbrella
authentication_cpp_lib_dep = declare_dependency(
    dependencies: [
        authentication_mode_cpp_lib_dep,
        authentication_token_cpp_lib_dep,
        k8s_constants_cpp_lib_dep,
        k8s_util_cpp_lib_dep,
        authentication_token_loader_cpp_lib_dep,
        token_auth_client_interceptor_cpp_lib_dep,
        authentication_token_validator_cpp_lib_dep,
    ]
)
# === @end: Umbrella dependency — backward compatible with consumers using the consolidated target === #