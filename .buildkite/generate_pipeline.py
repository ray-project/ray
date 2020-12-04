import json

BAZEL_TEST_COMMANDS = [(
    "core",
    "bazel test --config=ci $(./scripts/bazel_export_options) --build_tests_only -- //:all -rllib/...",
), (
    "serve",
    "bazel test --config=ci $(./scripts/bazel_export_options) --test_tag_filters=-jenkins_only python/ray/serve/...",
), (
    "dashboard",
    "bazel test --config=ci $(./scripts/bazel_export_options) python/ray/new_dashboard/... && cd dashboard/tests/ui_test && bash run.sh",
), (
    "medium a-j",
    "bazel test --config=ci $(./scripts/bazel_export_options) --test_tag_filters=-jenkins_only,medium_size_python_tests_a_to_j python/ray/tests/...",
), (
    "medium k-z",
    "bazel test --config=ci $(./scripts/bazel_export_options) --test_tag_filters=-jenkins_only,medium_size_python_tests_k_to_z python/ray/tests/...",
), (
    "small & large",
    "bazel test --config=ci $(./scripts/bazel_export_options) --test_tag_filters=-jenkins_only,-medium_size_python_tests_a_to_j,-medium_size_python_tests_k_to_z python/ray/tests/...",
)]

if __name__ == "__main__":
    pipeline_steps = []
    for label, command in BAZEL_TEST_COMMANDS:
        pipeline_steps.append({
            'label': label,
            'commands': ['conda activate && python --version', command],
            'plugins': [{
                'docker#v3.7.0': {
                    'image': '029272617770.dkr.ecr.us-west-2.amazonaws.com/ray_test_env:$BUILDKITE_COMMIT',
                    'debug': True,
                    'user': 'root',
                    'shell': ['/bin/bash', '-e', '-c', '-i'],
                    'shm-size': '2g',
                    'propagate-environment': True,
                    'mount-checkout': False,
                    'workdir': '/workdir'
                }
            }],
            'agents': {
                'queue': 'simon-agent-q'
            },
            "env": {
                "LC_ALL": "C.UTF-8",
                "LANG": "C.UTF-8"
            }
        })
    print(json.dumps(pipeline_steps))
