# === @begin: Compilable ===
py.extension_module(
    '_raylet',

    # .py, .pxi, and .pxd files aren't needed there in contrast to Bazel nor would they work.
    # Meson treats them not as usual sources but more like C/C++ header, `.h` files.
    # It also results into much cleaner code :)
    ['ray/_raylet.pyx'],

    install: true,
    subdir: 'ray/',
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
        'air/tests/',

        'autoscaler/aws/tests/',
        'autoscaler/gcp/tests/',
        'autoscaler/v2/tests/',

        'workflow/examples/',
        'workflow/tests/',

        'util/dask/tests/',
        'util/collective/tests/',
        'util/collective/examples/',
        'util/client/examples/',

        #'tune/examples/',
        'tune/tests/',

        'train/tests/',

        'serve/tests/',

        'experimental/packaging/example_pkg/',

        'data/examples/',
        'data/tests/',

        'dag/tests/',

        'dashboard/tests/',
        'dashboard/client/node_modules/',
        'dashboard/modules/tests/',
        'dashboard/modules/actor/tests/',
        'dashboard/modules/snapshot/tests/',
        'dashboard/modules/serve/tests/',
        'dashboard/modules/reporter/tests/',
        'dashboard/modules/node/tests/',
        'dashboard/modules/job/tests/',
        'dashboard/modules/healthz/tests/',
        'dashboard/modules/event/tests/',
        'dashboard/modules/data/tests/',

        'tests/',

        'dashboard/client/public/',
        'dashboard/client/src/',
    ],

    exclude_files: [
        # Bazel build files - nothing interesting, mostly tests, examples, and docs
        'BUILD',
        '_private/BUILD',
        'air/BUILD',
        'autoscaler/BUILD',
        'autoscaler/aws/BUILD',
        'autoscaler/azure/BUILD',
        'autoscaler/gcp/BUILD',
        'autoscaler/local/BUILD',
        'autoscaler/v2/BUILD',
        'workflow/BUILD',
        'util/BUILD',
        'util/dask/BUILD',
        'tune/BUILD',
        'train/BUILD',
        'serve/BUILD',
        'scripts/BUILD',
        'runtime_env/BUILD',
        'experimental/BUILD',
        'data/BUILD',
        'dag/BUILD',
        'dashboard/BUILD',

        'autoscaler/README.rst',
        'autoscaler/_private/fake_multi_node/example.yaml',
        'autoscaler/_private/fake_multi_node/example_docker.yaml',
        'autoscaler/_private/readonly/example.yaml',
        'autoscaler/_private/vsphere/data/userdata.yaml',
        'autoscaler/_private/vsphere/ARCHITECTURE.md',
        'autoscaler/aliyun/defaults.yaml',
        'autoscaler/aliyun/example-full.yaml',
        'autoscaler/aliyun/example-linux.yaml',
        'autoscaler/aws/cloudwatch/example-cloudwatch-agent-config.json',
        'autoscaler/aws/cloudwatch/example-cloudwatch-alarm-config.json',
        'autoscaler/aws/cloudwatch/example-cloudwatch-dashboard-config.json',
        'autoscaler/aws/development-example.yaml',
        'autoscaler/aws/example-cloudwatch.yaml',
        'autoscaler/aws/example-full.yaml',
        'autoscaler/aws/example-gpu-docker.yaml',
        'autoscaler/aws/example-head-and-worker-security-group.yaml',
        'autoscaler/aws/example-java.yaml',
        'autoscaler/aws/example-launch-templates.yaml',
        'autoscaler/aws/example-minimal.yaml',
        'autoscaler/aws/example-ml.yaml',
        'autoscaler/aws/example-multi-node-type.yaml',
        'autoscaler/aws/example-network-interfaces.yaml',
        'autoscaler/aws/example-security-group.yaml',
        'autoscaler/aws/example-subnets.yaml',
        'autoscaler/azure/example-full.yaml',
        'autoscaler/azure/example-gpu-docker.yaml',
        'autoscaler/azure/example-minimal.yaml',
        'autoscaler/gcp/example-full.yaml',
        'autoscaler/gcp/example-gpu-docker.yaml',
        'autoscaler/gcp/example-minimal.yaml',
        'autoscaler/gcp/example-tpu-pod-topology.yaml',
        'autoscaler/gcp/example-tpu-pod.yaml',
        'autoscaler/gcp/tpu.yaml',
        'autoscaler/kuberay/init-config.sh',
        'autoscaler/kuberay/ray-cluster.complete.yaml',
        'autoscaler/local/development-example.yaml',
        'autoscaler/local/example-full.yaml',
        'autoscaler/local/example-minimal-automatic.yaml',
        'autoscaler/local/example-minimal-manual.yaml',
        'autoscaler/vsphere/example-full.yaml',
        'autoscaler/vsphere/example-minimal.yaml',
        'autoscaler/vsphere/example-vsan-file-service.yaml',
        'autoscaler/vsphere/gpu.yaml',

        'cloudpickle/README.md',

        'util/collective/requirements.txt',
        'util/client/ARCHITECTURE.md/',

        'tune/README.rst',
        'tune/requirements-dev.txt',

        'air/data',

        'runtime_env/ARCHITECTURE.md',

        'includes/buffer.pxi',
        'includes/common.pxi',
        'includes/function_descriptor.pxi',
        'includes/gcs_client.pxi',
        'includes/global_state_accessor.pxi',
        'includes/libcoreworker.pxi',
        'includes/metric.pxi',
        'includes/object_ref.pxi',
        'includes/ray_config.pxi',
        'includes/serialization.pxi',
        'includes/unique_ids.pxi',

        'experimental/job/example_job/job_config.yaml',

        'dashboard/client/.env.production.local',
        'dashboard/client/.gitignore',
        'dashboard/client/package-lock.json',
        'dashboard/client/package.json',
        'dashboard/client/README.rst',
        'dashboard/client/tsconfig.json',
        'dashboard/client/meson.build',

        '_raylet.pyx'
    ]
)

# https://github.com/mesonbuild/meson/issues/12793
install_subdir(
    'ray/rllib/',
    install_dir: py.get_install_dir() / 'ray/',
    install_tag: 'python-runtime',

    follow_symlinks: true
)

# @Important:
# **/thirdparty_files/**
# **/thirdparty/**
#
# Both these guys are vendored but the first ones could be replaced with externally installed modules.
# When module relying on them imports them, it doesn't import them from thirdparty_files subdirectory
# but adds their content to `sys.path` so they'll be used instead of our variants. This is not only
# undesired behavior (as may lead to really-hard-to-debug conflicts as ones as we've seen before)
# but also really hard-to-implement in meson.
#
# `thirdparty` guys are easier and better - they are baked into this folder as part of the repo and used from
# there and only from there (e.g., `from thirdparty.blah import blah_blah`), so it's fine to keep them.

# === @end: Pure Python ===


# === @begin: JavaScript Bundle ===
subdir('ray/dashboard/client/')
# === @end: JavaScript Bundle ===
