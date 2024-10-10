# === @begin: Compilable ===
py.extension_module(
    '_raylet',

    # .py, .pxi, and .pxd files aren't needed there in contrast to Bazel nor would they work.
    # Meson treats them not as usual sources but more like C/C++ header, `.h` files.
    # It also results into much cleaner code :)
    ['ray/_raylet.pyx'],

    install: true,
    dependencies: [
        # Package-managed
        python_callbacks_cpp_lib_dep,
        core_worker_cpp_lib_dep,
        exported_internal_cpp_lib_dep,
        gcs_server_cpp_lib_dep,
        global_state_accessor_cpp_lib_dep,
        raylet_cpp_lib_dep,
        redis_client_cpp_lib_dep,
        stats_cpp_lib_dep,
        serialization_proto_cpp_lib_dep,
        util_cpp_lib_dep
    ],
    link_args : ['-Wl,--version-script=' + meson.project_source_root() / 'src/ray/ray_version_script.lds'],
    override_options : ['cython_language=cpp']
)
# === @end: Compilable ===


# === @begin: Pure Python ===
install_subdir(
    'ray/',
    install_dir: py.get_install_dir(),
    install_tag: 'python-runtime',

    follow_symlinks: true,

    exclude_directories: [

    ],

    exclude_files: [
        # Bazel build files
        'BUILD'


    ]
)
# === @end: Pure Python ===
