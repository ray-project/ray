import copy
import logging
import os
import sys

import yaml

# Env variables:

# RAY_REPO          Repo to use for finding the wheel
# RAY_BRANCH        Branch to find the wheel
# RAY_TEST_REPO     Repo to use for test scripts
# RAY_TEST_BRANCH   Branch for test scripts
# FILTER_FILE       File filter
# FILTER_TEST       Test name filter
# RELEASE_TEST_SUITE Release test suite (e.g. manual, nightly)


class ReleaseTest:
    def __init__(self, name: str, smoke_test: bool = False, retry: int = 0):
        self.name = name
        self.smoke_test = smoke_test
        self.retry = retry

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name

    def __contains__(self, item):
        return self.name.__contains__(item)

    def __iter__(self):
        return iter(self.name)

    def __len__(self):
        return len(self.name)


class SmokeTest(ReleaseTest):
    def __init__(self, name: str, retry: int = 0):
        super(SmokeTest, self).__init__(
            name=name, smoke_test=True, retry=retry)


CORE_NIGHTLY_TESTS = {
    "~/ray/release/nightly_tests/nightly_tests.yaml": [
        "shuffle_10gb",
        "shuffle_50gb",
        "shuffle_50gb_large_partition",
        "shuffle_100gb",
        "non_streaming_shuffle_100gb",
        "non_streaming_shuffle_50gb_large_partition",
        "non_streaming_shuffle_50gb",
        "dask_on_ray_10gb_sort",
        "dask_on_ray_100gb_sort",
        SmokeTest("dask_on_ray_large_scale_test_no_spilling"),
        SmokeTest("dask_on_ray_large_scale_test_spilling"),
        "stress_test_placement_group",
        "shuffle_1tb_1000_partition",
        "non_streaming_shuffle_1tb_1000_partition",
        "shuffle_1tb_5000_partitions",
        "non_streaming_shuffle_1tb_5000_partitions",
        "decision_tree_autoscaling",
        "autoscaling_shuffle_1tb_1000_partitions",
        SmokeTest("stress_test_many_tasks"),
        SmokeTest("stress_test_dead_actors"),
        "shuffle_data_loader",
        "dask_on_ray_1tb_sort",
    ],
    "~/ray/benchmarks/benchmark_tests.yaml": [
        "single_node",
        "object_store",
        "many_actors_smoke_test",
        "many_tasks_smoke_test",
        "many_pgs_smoke_test",
    ],
    "~/ray/release/nightly_tests/dataset/dataset_test.yaml": [
        "inference",
        "shuffle_data_loader",
    ],
}

NIGHTLY_TESTS = {
    # "~/ray/release/horovod_tests/horovod_tests.yaml": [
    #     SmokeTest("horovod_test"),
    # ],  # Should we enable this?
    "~/ray/release/golden_notebook_tests/golden_notebook_tests.yaml": [
        "dask_xgboost_test",
        "modin_xgboost_test",
        "torch_tune_serve_test",
    ],
    "~/ray/release/nightly_tests/nightly_tests.yaml": [
        "dask_on_ray_large_scale_test_no_spilling",
        "dask_on_ray_large_scale_test_spilling",
    ],
    "~/ray/release/long_running_tests/long_running_tests.yaml": [
        SmokeTest("actor_deaths"),
        SmokeTest("apex"),
        SmokeTest("impala"),
        SmokeTest("many_actor_tasks"),
        SmokeTest("many_drivers"),
        SmokeTest("many_ppo"),
        SmokeTest("many_tasks"),
        SmokeTest("many_tasks_serialized_ids"),
        SmokeTest("node_failures"),
        SmokeTest("pbt"),
        # SmokeTest("serve"),
        # SmokeTest("serve_failure"),
    ],
    "~/ray/release/microbenchmark/microbenchmark.yaml": [
        "microbenchmark",
    ],
    "~/ray/release/sgd_tests/sgd_tests.yaml": [
        "sgd_gpu",
    ],
    "~/ray/release/tune_tests/scalability_tests/tune_tests.yaml": [
        "bookkeeping_overhead",
        "durable_trainable",
        SmokeTest("long_running_large_checkpoints"),
        SmokeTest("network_overhead"),
        "result_throughput_cluster",
        "result_throughput_single_node",
        "xgboost_sweep",
    ],
    "~/ray/release/xgboost_tests/xgboost_tests.yaml": [
        "train_small",
        "train_moderate",
        "train_gpu",
        "tune_small",
        "tune_4x32",
        "tune_32x4",
        "ft_small_elastic",
        "ft_small_non_elastic",
        "distributed_api_test",
    ],
    "~/ray/release/rllib_tests/rllib_tests.yaml": [
        SmokeTest("learning_tests"),
        "multi_gpu_learning_tests",
        "multi_gpu_with_lstm_learning_tests",
        # We'll have these as per-PR tests soon.
        # "example_scripts_on_gpu_tests",
        SmokeTest("stress_tests"),
    ],
    "~/ray/release/serve_tests/serve_tests.yaml": [
        "single_deployment_1k_noop_replica",
        "multi_deployment_1k_noop_replica",
    ],
    "~/ray/release/runtime_env_tests/runtime_env_tests.yaml": [
        "rte_many_tasks_actors",
        "wheel_urls",
    ],
}

WEEKLY_TESTS = {
    "~/ray/benchmarks/benchmark_tests.yaml": [
        "many_actors",
        "many_tasks",
        "many_pgs",
        "many_nodes",
    ],
    "~/ray/release/nightly_tests/nightly_tests.yaml": [
        "stress_test_many_tasks",
        "stress_test_dead_actors",
    ],
    "~/ray/release/horovod_tests/horovod_tests.yaml": [
        "horovod_test",
    ],
    "~/ray/release/long_running_distributed_tests"
    "/long_running_distributed.yaml": [
        "pytorch_pbt_failure",
    ],
    # Full long running tests (1 day runtime)
    "~/ray/release/long_running_tests/long_running_tests.yaml": [
        "actor_deaths",
        "apex",
        "impala",
        "many_actor_tasks",
        "many_drivers",
        "many_ppo",
        "many_tasks",
        "many_tasks_serialized_ids",
        "node_failures",
        "pbt",
        "serve",
        "serve_failure",
    ],
    "~/ray/release/tune_tests/scalability_tests/tune_tests.yaml": [
        "network_overhead",
        "long_running_large_checkpoints",
    ],
    "~/ray/release/rllib_tests/rllib_tests.yaml": [
        "learning_tests",
        "stress_tests",
    ],
}

MANUAL_TESTS = {
    "~/ray/release/long_running_tests/long_running_tests.yaml": [
        SmokeTest("serve"),
        SmokeTest("serve_failure"),
    ]
}

SUITES = {
    "core-nightly": CORE_NIGHTLY_TESTS,
    "nightly": NIGHTLY_TESTS,
    "weekly": WEEKLY_TESTS,
    "manual": MANUAL_TESTS,
}

DEFAULT_STEP_TEMPLATE = {
    "env": {
        "ANYSCALE_CLOUD_ID": "cld_4F7k8814aZzGG8TNUGPKnc",
        "ANYSCALE_PROJECT": "prj_2xR6uT6t7jJuu1aCwWMsle",
        "RELEASE_AWS_BUCKET": "ray-release-automation-results",
        "RELEASE_AWS_LOCATION": "dev",
        "RELEASE_AWS_DB_NAME": "ray_ci",
        "RELEASE_AWS_DB_TABLE": "release_test_result",
        "AWS_REGION": "us-west-2"
    },
    "agents": {
        "queue": "runner_queue_branch"
    },
    "plugins": [{
        "docker#v3.8.0": {
            "image": "rayproject/ray",
            "propagate-environment": True
        }
    }],
    "commands": []
}


def ask_configuration():
    RAY_BRANCH = os.environ.get("RAY_BRANCH", "master")
    RAY_REPO = os.environ.get("RAY_REPO",
                              "https://github.com/ray-project/ray.git")
    RAY_VERSION = os.environ.get("RAY_VERSION", "")

    RAY_TEST_BRANCH = os.environ.get("RAY_TEST_BRANCH", RAY_BRANCH)
    RAY_TEST_REPO = os.environ.get("RAY_TEST_REPO", RAY_REPO)

    RELEASE_TEST_SUITE = os.environ.get("RELEASE_TEST_SUITE", "nightly")
    FILTER_FILE = os.environ.get("FILTER_FILE", "")
    FILTER_TEST = os.environ.get("FILTER_TEST", "")

    input_ask_step = {
        "input": "Input required: Please specify tests to run",
        "fields": [
            {
                "text": ("Please specify the Ray repository used "
                         "to find the wheel."),
                "hint": ("Repository from which to fetch the latest "
                         "commits to find the Ray wheels. Usually you don't "
                         "need to change this."),
                "default": RAY_REPO,
                "key": "ray_repo"
            },
            {
                "text": ("Please specify the Ray branch used "
                         "to find the wheel."),
                "hint": "For releases, this will be e.g. `releases/1.x.0`",
                "default": RAY_BRANCH,
                "key": "ray_branch"
            },
            {
                "text": ("Please specify the Ray version used "
                         "to find the wheel."),
                "hint": ("Leave empty for latest master. For releases, "
                         "specify the release version."),
                "required": False,
                "default": RAY_VERSION,
                "key": "ray_version"
            },
            {
                "text": ("Please specify the Ray repository used "
                         "to find the tests you would like to run."),
                "hint": ("If you're developing a new release test, this "
                         "will most likely be your GitHub fork."),
                "default": RAY_TEST_REPO,
                "key": "ray_test_repo"
            },
            {
                "text": ("Please specify the Ray branch used "
                         "to find the tests you would like to run."),
                "hint": ("If you're developing a new release test, this "
                         "will most likely be a branch living on your "
                         "GitHub fork."),
                "default": RAY_TEST_BRANCH,
                "key": "ray_test_branch"
            },
            {
                "select": ("Please specify the release test suite containing "
                           "the tests you would like to run."),
                "hint": ("Check in the `build_pipeline.py` if you're "
                         "unsure which suite contains your tests."),
                "required": True,
                "options": sorted(SUITES.keys()),
                "default": RELEASE_TEST_SUITE,
                "key": "release_test_suite"
            },
            {
                "text": ("Please specify a filter for the test files "
                         "that should be included in this build."),
                "hint": ("Only test files (e.g. xgboost_tests.yml) that "
                         "match this string will be included in the test"),
                "default": FILTER_FILE,
                "required": False,
                "key": "filter_file"
            },
            {
                "text": ("Please specify a filter for the test names "
                         "that should be included in this build."),
                "hint": ("Only test names (e.g. tune_4x32) that match "
                         "this string will be included in the test"),
                "default": FILTER_TEST,
                "required": False,
                "key": "filter_test"
            },
        ],
        "key": "input_ask_step",
    }

    run_again_step = {
        "commands": [
            f"export {v}=$(buildkite-agent meta-data get \"{k}\")"
            for k, v in {
                "ray_branch": "RAY_BRANCH",
                "ray_repo": "RAY_REPO",
                "ray_version": "RAY_VERSION",
                "ray_test_branch": "RAY_TEST_BRANCH",
                "ray_test_repo": "RAY_TEST_REPO",
                "release_test_suite": "RELEASE_TEST_SUITE",
                "filter_file": "FILTER_FILE",
                "filter_test": "FILTER_TEST",
            }.items()
        ] + [
            "export AUTOMATIC=1",
            "python3 -m pip install --user pyyaml",
            "git clone -b $${RAY_TEST_BRANCH} $${RAY_TEST_REPO} ~/ray",
            ("python3 ~/ray/release/.buildkite/build_pipeline.py "
             "| buildkite-agent pipeline upload"),
        ],
        "label": ":pipeline: Again",
        "agents": {
            "queue": "runner_queue_branch"
        },
        "depends_on": "input_ask_step",
        "key": "run_again_step",
    }

    return [
        input_ask_step,
        run_again_step,
    ]


def build_pipeline(steps):
    all_steps = []

    RAY_BRANCH = os.environ.get("RAY_BRANCH", "master")
    RAY_REPO = os.environ.get("RAY_REPO",
                              "https://github.com/ray-project/ray.git")
    RAY_VERSION = os.environ.get("RAY_VERSION", "")

    RAY_TEST_BRANCH = os.environ.get("RAY_TEST_BRANCH", RAY_BRANCH)
    RAY_TEST_REPO = os.environ.get("RAY_TEST_REPO", RAY_REPO)

    FILTER_FILE = os.environ.get("FILTER_FILE", "")
    FILTER_TEST = os.environ.get("FILTER_TEST", "")

    logging.info(
        f"Building pipeline \n"
        f"Ray repo/branch to test:\n"
        f" RAY_REPO   = {RAY_REPO}\n"
        f" RAY_BRANCH = {RAY_BRANCH}\n\n"
        f" RAY_VERSION = {RAY_VERSION}\n\n"
        f"Ray repo/branch containing the test configurations and scripts:"
        f" RAY_TEST_REPO   = {RAY_TEST_REPO}\n"
        f" RAY_TEST_BRANCH = {RAY_TEST_BRANCH}\n\n"
        f"Filtering for these tests:\n"
        f" FILTER_FILE = {FILTER_FILE}\n"
        f" FILTER_TEST = {FILTER_TEST}\n\n")

    for test_file, test_names in steps.items():
        if FILTER_FILE and FILTER_FILE not in test_file:
            continue

        test_base = os.path.basename(test_file)
        for test_name in test_names:
            if FILTER_TEST and FILTER_TEST not in test_name:
                continue

            if not isinstance(test_name, ReleaseTest):
                test_name = ReleaseTest(name=test_name)

            logging.info(f"Adding test: {test_base}/{test_name}")

            cmd = str(f"RAY_REPO=\"{RAY_REPO}\" "
                      f"RAY_BRANCH=\"{RAY_BRANCH}\" "
                      f"RAY_VERSION=\"{RAY_VERSION}\" "
                      f"python release/e2e.py "
                      f"--category {RAY_BRANCH} "
                      f"--test-config {test_file} "
                      f"--test-name {test_name}")

            if test_name.smoke_test:
                logging.info("This test will run as a smoke test.")
                cmd += " --smoke-test"

            step_conf = copy.deepcopy(DEFAULT_STEP_TEMPLATE)

            if test_name.retry:
                logging.info(f"This test will be retried up to "
                             f"{test_name.retry} times.")
                step_conf["retry"] = {
                    "automatic": [{
                        "exit_status": "*",
                        "limit": test_name.retry
                    }]
                }

            step_conf["commands"] = [
                "pip install -q -r release/requirements.txt",
                "pip install -U boto3 botocore",
                f"git clone -b {RAY_TEST_BRANCH} {RAY_TEST_REPO} ~/ray",
                cmd,
            ]

            step_conf["label"] = f"{test_name} ({RAY_BRANCH}) - " \
                                 f"{RAY_TEST_BRANCH}/{test_base}"
            all_steps.append(step_conf)

    return all_steps


def alert_pipeline(stats: bool = False):
    step_conf = copy.deepcopy(DEFAULT_STEP_TEMPLATE)

    cmd = "python release/alert.py"
    if stats:
        cmd += " --stats"

    step_conf["commands"] = [
        "pip install -q -r release/requirements.txt",
        "pip install -U boto3 botocore",
        cmd,
    ]
    step_conf["label"] = f"Send periodic alert (stats_only = {stats})"
    return [step_conf]


if __name__ == "__main__":
    alert = os.environ.get("RELEASE_ALERT", "0")

    ask_for_config = not bool(int(os.environ.get("AUTOMATIC", "0")))

    if alert in ["1", "stats"]:
        steps = alert_pipeline(alert == "stats")
    elif ask_for_config:
        steps = ask_configuration()
    else:
        TEST_SUITE = os.environ.get("RELEASE_TEST_SUITE", "nightly")
        PIPELINE_SPEC = SUITES[TEST_SUITE]

        steps = build_pipeline(PIPELINE_SPEC)

    yaml.dump({"steps": steps}, sys.stdout)
