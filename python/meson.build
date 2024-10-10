# === @begin: Compilable ===
py.extension_module(
    '_raylet',
    [
        #'ray/__init__.py',

        #'ray/_raylet.pxd',
        'ray/_raylet.pyx',

        #'ray/includes/__init__.pxd',
        #'ray/includes/common.pxd',
        #'ray/includes/common.pxi',
        #'ray/includes/metric.pxd',
        #'ray/includes/metric.pxi',
        #'ray/includes/function_descriptor.pxd',
        #'ray/includes/function_descriptor.pxi',
        #'ray/includes/global_state_accessor.pxd',
        #'ray/includes/global_state_accessor.pxi',
        #'ray/includes/unique_ids.pxd',
        #'ray/includes/unique_ids.pxi',
        #'ray/includes/ray_config.pxd',
        #'ray/includes/ray_config.pxi',

        #'ray/includes/optional.pxd',

        #'ray/includes/serialization.pxi',
        #'ray/includes/buffer.pxi',
        #'ray/includes/object_ref.pxi',
        #'ray/includes/gcs_client.pxi',
        #'ray/includes/libcoreworker.pxi',
    ],
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
    link_args : ['-Wl,--version-script=@SOURCE_ROOT@/ray/ray_version_script.lds'],
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
