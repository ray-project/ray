# === @begin: stream_redirection_handle_cpp_lib (@original: stream_redirection_handle) ===
stream_redirection_handle_cpp_lib_dependencies = [
    compat_cpp_lib_dep,
    pipe_logger_cpp_lib_dep,
    scoped_dup2_wrapper_cpp_lib_dep,
    stream_redirection_options_cpp_lib_dep,
]

stream_redirection_handle_cpp_lib = static_library('stream_redirection_handle_cpp_lib',
    ['stream_redirection_handle.cc'],
    dependencies: stream_redirection_handle_cpp_lib_dependencies,
    install: false
)

stream_redirection_handle_cpp_lib_dep = declare_dependency(
    link_with: [stream_redirection_handle_cpp_lib],
    dependencies: stream_redirection_handle_cpp_lib_dependencies
)
# === @end: stream_redirection_handle_cpp_lib (@original: stream_redirection_handle) ===
