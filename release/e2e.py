"""
This is an end to end release test automation script used to kick off periodic
release tests, running on Anyscale.

The tool leverages app configs and compute templates.

Calling this script will run a single release test.

Example:

python e2e.py --test-config ~/ray/release/xgboost_tests/xgboost_tests.yaml --test-name tune_small

The following steps are then performed:

1. It will look up the test tune_small in the file xgboost_tests.yaml
2. It will fetch the specified app config and compute template and register
   those with anyscale (if they don’t exist yet)
3. It waits until the app config is built
4. It then kicks off the script defined in the run block
5. When the script is finished, it will fetch the latest logs, the full log
   output, and any artifacts specified in the artifacts block.
6. The full logs and artifacts will be stored in a s3 bucket
7. It will also fetch the json file specified in the run block as results.
   This is the file where you should write your metrics to.
8. All results are then stored in a database.
   Specifically it will store the following fields:
   - Timestamp
   - Test name
   - Status (finished, error, timeout, invalid)
   - Last logs (50 lines)
   - results (see above)
   - artifacts (links to s3 files)

Then the script exits. If an error occurs at any time, a fail result is
written to the database.


Writing a new release test
--------------------------
Each release test requires the following:

1. It has to be added in a release test yaml file, describing meta information
   about the test (e.g. name, command to run, timeout)
2. You need an app config yaml
3. You need a compute template yaml
4. You need to define a command to run. This is usually a python script.
   The command should accept (or ignore) a single optional
   `--smoke-test` argument.
   Usually the command should write its result metrics to a json file.
   The json filename is available in the TEST_OUTPUT_JSON env variable.
5. Add your test in release/.buildkite/build_pipeline.py.

The script will have access to these environment variables:

    "RAY_ADDRESS": os.environ.get("RAY_ADDRESS", "auto")
    "TEST_OUTPUT_JSON": results_json_filename
    "IS_SMOKE_TEST": "1" if smoke_test else "0"

For an example, take a look at the XGBoost test suite:

https://github.com/ray-project/ray/blob/master/release/xgboost_tests/xgboost_tests.yaml

These all use the same app configs and similar compute templates. This means
that app configs can be re-used across runs and only have to be built ones.

App configs and compute templates can interpret environment variables.
A notable one is the `RAY_WHEELS` variable which points to the wheels that
should be tested (e.g. latest master wheels). You might want to include
something like this in your `post_build_cmds`:

  - pip3 install -U {{ env["RAY_WHEELS"] | default("ray") }}

If you want to force rebuilds, consider using something like

  - echo {{ env["TIMESTAMP"] }}

so that your app configs changes each time the script is executed. If you
only want to trigger rebuilds once per day, use `DATESTAMP` instead:

  - echo {{ env["DATESTAMP"] }}

Local testing
-------------
For local testing, make sure to authenticate with the ray-ossci AWS user
(e.g. by setting the respective environment variables obtained from go/aws),
or use the `--no-report` command line argument.

Also make sure to set these environment variables:

- ANYSCALE_CLI_TOKEN (should contain your anyscale credential token)
- ANYSCALE_PROJECT (should point to a project ID you have access to)

A test can then be run like this:

python e2e.py --no-report --test-config ~/ray/release/xgboost_tests/xgboost_tests.yaml --test-name tune_small

The `--no-report` option disables storing the results in the DB and
artifacts on S3. If you set this option, you do not need access to the
ray-ossci AWS user.

Using Compilation on Product + App Config Override
--------------------------------------------------
For quick iteration when debugging a release test, go/compile-on-product allows
you to easily modify and recompile Ray, such that the recompilation happens
within an app build step and can benefit from a warm Bazel cache. See
go/compile-on-product for more information.

After kicking off the app build, you can give the app config ID to this script
as an app config override, where the indicated app config will be used instead
of the app config given in the test config. E.g., running

python e2e.py --no-report --test-config ~/ray/benchmarks/benchmark_tests.yaml --test-name=single_node --app-config-id-override=apt_TBngEXXXrhipMXgexVcrpC9i

would run the single_node benchmark test with the apt_TBngEXXXrhipMXgexVcrpC9i
app config instead of the app config given in
~/ray/benchmarks/benchmark_tests.yaml. If the build for the app config is still
in progress, the script will wait until it completes, same as for a locally
defined app config.

Running on Head Node vs Running with Anyscale Connect
-----------------------------------------------------
By default release tests run their drivers on the head node. Support is being
added to run release tests that execute the driver as a subprocess and run
the workload on Anyscale product via Anyscale connect.
Note that when the driver in the test is a subprocess of releaser, releaser
cannot be terminated before the test finishes.
Other known feature gaps when running with Anyscale connect:
- Kicking off a test or checking progress is not supported.
- Downloading / uploading logs and artifacts are unsupported.
- Logs from remote may not have finished streaming, before the driver exits.

Long running tests
------------------
Long running tests can be kicked off with by adding the --kick-off-only
parameters to the e2e script. The status can then be checked with the
--check command.

Long running test sessions will be terminated after `timeout` seconds, after
which the latest result in the TEST_OUTPUT_JSON will be reported. Thus,
long running release tests should update this file periodically.

There are also two config options to configure behavior. The `time_key` is
needed to track the latest update of the TEST_OUTPUT_JSON and should contain
a floating point number (usually `time.time()`). The `max_update_delay` then
specified the maximum time in seconds that can be passed without an update
to the results json. If the output file hasn't been updated in e.g. 60 seconds,
this could indicate that the command is stale/frozen, and thus should fail.

Release test yaml example
-------------------------
- name: example
  owner:
    mail: "kai@anyscale.com"  # Currently not used
    slack: "@tune-team"  # Currentl not used

  cluster:
    app_config: app_config.yaml  # Relative to the release test yaml
    compute_template: tpl_cpu.yaml

  run:
    timeout: 600  # in seconds
    prepare: python wait_cluster.py 4 600  # prepare cmd to run before test
    script: python workloads/train.py  # actual release test command

    # Only needed for long running test
    time_key: last_update  # Key in the results json indicating current time
    max_update_delay: 30  # If state hasn't been updated in 30s, terminate

  # This block is optional
  artifacts:
    # Artifact name: location on head node
    - detailed_output: detailed_output.csv

  # This block is optional. If present, the contents will be
  # deep updated for smoke testing
  smoke_test:
    cluster:
      compute_template: tpl_cpu_smoketest.yaml

"""  # noqa: E501
import argparse
import boto3
import collections
import copy
import datetime
import hashlib
import jinja2
import json
import logging
import multiprocessing
import os
import requests
import shutil
import subprocess
import sys
import tempfile
import time
from queue import Empty
from typing import Any, Dict, Optional, Tuple, List

import yaml

import anyscale
import anyscale.conf
from anyscale.api import instantiate_api_client
from anyscale.controllers.session_controller import SessionController
from anyscale.sdk.anyscale_client.sdk import AnyscaleSDK

logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
formatter = logging.Formatter(fmt="[%(levelname)s %(asctime)s] "
                              "%(filename)s: %(lineno)d  "
                              "%(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def getenv_default(key: str, default: Optional[str] = None):
    """Return environment variable with default value"""
    # If the environment variable is set but "", still return default
    return os.environ.get(key, None) or default


GLOBAL_CONFIG = {
    "ANYSCALE_USER": getenv_default("ANYSCALE_USER",
                                    "release-automation@anyscale.com"),
    "ANYSCALE_HOST": getenv_default("ANYSCALE_HOST",
                                    "https://beta.anyscale.com"),
    "ANYSCALE_CLI_TOKEN": getenv_default("ANYSCALE_CLI_TOKEN"),
    "ANYSCALE_CLOUD_ID": getenv_default(
        "ANYSCALE_CLOUD_ID",
        "cld_4F7k8814aZzGG8TNUGPKnc"),  # cld_4F7k8814aZzGG8TNUGPKnc
    "ANYSCALE_PROJECT": getenv_default("ANYSCALE_PROJECT", ""),
    "RAY_VERSION": getenv_default("RAY_VERSION", "2.0.0.dev0"),
    "RAY_REPO": getenv_default("RAY_REPO",
                               "https://github.com/ray-project/ray.git"),
    "RAY_BRANCH": getenv_default("RAY_BRANCH", "master"),
    "RELEASE_AWS_BUCKET": getenv_default("RELEASE_AWS_BUCKET",
                                         "ray-release-automation-results"),
    "RELEASE_AWS_LOCATION": getenv_default("RELEASE_AWS_LOCATION", "dev"),
    "RELEASE_AWS_DB_NAME": getenv_default("RELEASE_AWS_DB_NAME", "ray_ci"),
    "RELEASE_AWS_DB_TABLE": getenv_default("RELEASE_AWS_DB_TABLE",
                                           "release_test_result"),
    "RELEASE_AWS_DB_SECRET_ARN": getenv_default(
        "RELEASE_AWS_DB_SECRET_ARN",
        "arn:aws:secretsmanager:us-west-2:029272617770:secret:"
        "rds-db-credentials/cluster-7RB7EYTTBK2EUC3MMTONYRBJLE/ray_ci-MQN2hh",
    ),
    "RELEASE_AWS_DB_RESOURCE_ARN": getenv_default(
        "RELEASE_AWS_DB_RESOURCE_ARN",
        "arn:aws:rds:us-west-2:029272617770:cluster:ci-reporting",
    ),
    "DATESTAMP": str(datetime.datetime.now().strftime("%Y%m%d")),
    "TIMESTAMP": str(int(datetime.datetime.now().timestamp())),
    "EXPIRATION_1D": str((datetime.datetime.now() +
                          datetime.timedelta(days=1)).strftime("%Y-%m-%d")),
    "EXPIRATION_2D": str((datetime.datetime.now() +
                          datetime.timedelta(days=2)).strftime("%Y-%m-%d")),
    "EXPIRATION_3D": str((datetime.datetime.now() +
                          datetime.timedelta(days=3)).strftime("%Y-%m-%d")),
}

REPORT_S = 30


def maybe_fetch_api_token():
    if GLOBAL_CONFIG["ANYSCALE_CLI_TOKEN"] is None:
        print("Missing ANYSCALE_CLI_TOKEN, retrieving from AWS secrets store")
        # NOTE(simon) This should automatically retrieve
        # release-automation@anyscale.com's anyscale token
        GLOBAL_CONFIG["ANYSCALE_CLI_TOKEN"] = boto3.client(
            "secretsmanager", region_name="us-west-2"
        ).get_secret_value(
            SecretId="arn:aws:secretsmanager:us-west-2:029272617770:secret:"
            "release-automation/"
            "anyscale-token20210505220406333800000001-BcUuKB")["SecretString"]


class PrepareCommandRuntimeError(RuntimeError):
    pass


class ReleaseTestTimeoutError(RuntimeError):
    pass


class SessionTimeoutError(ReleaseTestTimeoutError):
    pass


class FileSyncTimeoutError(ReleaseTestTimeoutError):
    pass


class CommandTimeoutError(ReleaseTestTimeoutError):
    pass


class PrepareCommandTimeoutError(ReleaseTestTimeoutError):
    pass


class State:
    def __init__(self, state: str, timestamp: float, data: Any):
        self.state = state
        self.timestamp = timestamp
        self.data = data


sys.path.insert(0, anyscale.ANYSCALE_RAY_DIR)


def anyscale_project_url(project_id: str):
    return f"{GLOBAL_CONFIG['ANYSCALE_HOST']}" \
           f"/o/anyscale-internal/projects/{project_id}" \
           f"/?tab=session-list"


def anyscale_session_url(project_id: str, session_id: str):
    return f"{GLOBAL_CONFIG['ANYSCALE_HOST']}" \
           f"/o/anyscale-internal/projects/{project_id}" \
           f"/clusters/{session_id}"


def anyscale_compute_tpl_url(compute_tpl_id: str):
    return f"{GLOBAL_CONFIG['ANYSCALE_HOST']}" \
           f"/o/anyscale-internal/configurations/cluster-computes" \
           f"/{compute_tpl_id}"


def anyscale_app_config_build_url(build_id: str):
    return f"{GLOBAL_CONFIG['ANYSCALE_HOST']}" \
           f"/o/anyscale-internal/configurations/app-config-details" \
           f"/{build_id}"


def wheel_url(ray_version, git_branch, git_commit):
    return f"https://s3-us-west-2.amazonaws.com/ray-wheels/" \
           f"{git_branch}/{git_commit}/" \
           f"ray-{ray_version}-cp37-cp37m-manylinux2014_x86_64.whl"


def wheel_exists(ray_version, git_branch, git_commit):
    url = wheel_url(ray_version, git_branch, git_commit)
    return requests.head(url).status_code == 200


def get_latest_commits(repo: str, branch: str = "master") -> List[str]:
    cur = os.getcwd()
    with tempfile.TemporaryDirectory() as tmpdir:
        os.chdir(tmpdir)

        clone_cmd = [
            "git",
            "clone",
            "--filter=tree:0",
            "--no-checkout",
            # "--single-branch",
            # "--depth=10",
            f"--branch={branch}",
            repo,
            tmpdir,
        ]
        log_cmd = [
            "git",
            "log",
            "-n",
            "10",
            "--pretty=format:%H",
        ]

        subprocess.check_output(clone_cmd)
        commits = subprocess.check_output(log_cmd).decode(
            sys.stdout.encoding).split("\n")
    os.chdir(cur)
    return commits


def find_ray_wheels(repo: str, branch: str, version: str):
    url = None
    commits = get_latest_commits(repo, branch)
    logger.info(f"Latest 10 commits for branch {branch}: {commits}")
    for commit in commits:
        if wheel_exists(version, branch, commit):
            url = wheel_url(version, branch, commit)
            os.environ["RAY_WHEELS"] = url
            logger.info(
                f"Found wheels URL for Ray {version}, branch {branch}: "
                f"{url}")
            break
    return url


def _check_stop(stop_event: multiprocessing.Event, timeout_type: str):
    if stop_event.is_set():
        if timeout_type == "prepare_command":
            raise PrepareCommandTimeoutError(
                "Process timed out in the prepare command stage.")
        if timeout_type == "command":
            raise CommandTimeoutError(
                "Process timed out while running a command.")
        elif timeout_type == "file_sync":
            raise FileSyncTimeoutError(
                "Process timed out while syncing files.")
        elif timeout_type == "session":
            raise SessionTimeoutError(
                "Process timed out while starting a session.")
        else:
            assert False, "Unexpected timeout type."


def _deep_update(d, u):
    for k, v in u.items():
        if isinstance(v, collections.abc.Mapping):
            d[k] = _deep_update(d.get(k, {}), v)
        else:
            d[k] = v
    return d


def _dict_hash(dt: Dict[Any, Any]) -> str:
    json_str = json.dumps(dt, sort_keys=True, ensure_ascii=True)
    sha = hashlib.sha256()
    sha.update(json_str.encode())
    return sha.hexdigest()


def _load_config(local_dir: str, config_file: Optional[str]) -> Optional[Dict]:
    if not config_file:
        return None

    config_path = os.path.join(local_dir, config_file)
    with open(config_path, "rt") as f:
        # Todo: jinja2 render
        content = f.read()

    env = copy.deepcopy(os.environ)
    env.update(GLOBAL_CONFIG)

    content = jinja2.Template(content).render(env=env)
    return yaml.safe_load(content)


def has_errored(result: Dict[Any, Any]) -> bool:
    return result.get("status", "invalid") != "finished"


def report_result(test_suite: str, test_name: str, status: str, logs: str,
                  results: Dict[Any, Any], artifacts: Dict[Any, Any],
                  category: str):
    now = datetime.datetime.utcnow()
    rds_data_client = boto3.client("rds-data", region_name="us-west-2")

    schema = GLOBAL_CONFIG["RELEASE_AWS_DB_TABLE"]

    sql = (
        f"INSERT INTO {schema} "
        f"(created_on, test_suite, test_name, status, last_logs, "
        f"results, artifacts, category) "
        f"VALUES (:created_on, :test_suite, :test_name, :status, :last_logs, "
        f":results, :artifacts, :category)")

    rds_data_client.execute_statement(
        database=GLOBAL_CONFIG["RELEASE_AWS_DB_NAME"],
        parameters=[
            {
                "name": "created_on",
                "typeHint": "TIMESTAMP",
                "value": {
                    "stringValue": now.strftime("%Y-%m-%d %H:%M:%S")
                },
            },
            {
                "name": "test_suite",
                "value": {
                    "stringValue": test_suite
                }
            },
            {
                "name": "test_name",
                "value": {
                    "stringValue": test_name
                }
            },
            {
                "name": "status",
                "value": {
                    "stringValue": status
                }
            },
            {
                "name": "last_logs",
                "value": {
                    "stringValue": logs
                }
            },
            {
                "name": "results",
                "typeHint": "JSON",
                "value": {
                    "stringValue": json.dumps(results)
                },
            },
            {
                "name": "artifacts",
                "typeHint": "JSON",
                "value": {
                    "stringValue": json.dumps(artifacts)
                },
            },
            {
                "name": "category",
                "value": {
                    "stringValue": category
                }
            },
        ],
        secretArn=GLOBAL_CONFIG["RELEASE_AWS_DB_SECRET_ARN"],
        resourceArn=GLOBAL_CONFIG["RELEASE_AWS_DB_RESOURCE_ARN"],
        schema=schema,
        sql=sql,
    )


def log_results_and_artifacts(result: Dict):
    results = result.get("results", {})
    if results:
        msg = "Observed the following results:\n\n"

        for key, val in results.items():
            msg += f"  {key} = {val}\n"
    else:
        msg = "Did not find any results."
    logger.info(msg)

    artifacts = result.get("artifacts", {})
    if artifacts:
        msg = "Saved the following artifacts:\n\n"

        for key, val in artifacts.items():
            msg += f"  {key} = {val}\n"
    else:
        msg = "Did not find any artifacts."
    logger.info(msg)


def _cleanup_session(sdk: AnyscaleSDK, session_id: str):
    if session_id:
        # Just trigger a request. No need to wait until session shutdown.
        sdk.terminate_session(
            session_id=session_id, terminate_session_options={})


def search_running_session(sdk: AnyscaleSDK, project_id: str,
                           session_name: str) -> Optional[str]:
    session_id = None

    logger.info(f"Looking for existing session with name {session_name}")

    result = sdk.search_sessions(
        project_id=project_id,
        sessions_query=dict(name=dict(equals=session_name)))

    if len(result.results) > 0 and result.results[0].state == "Running":
        logger.info("Found existing session.")
        session_id = result.results[0].id
    return session_id


def create_or_find_compute_template(
        sdk: AnyscaleSDK,
        project_id: str,
        compute_tpl: Dict[Any, Any],
        _repeat: bool = True) -> Tuple[Optional[str], Optional[str]]:
    compute_tpl_id = None
    compute_tpl_name = None
    if compute_tpl:
        # As of Anyscale 0.4.1, it is an error to use the same compute template
        # name within the same organization, between different projects.
        compute_tpl_name = f"{project_id}/compute/{_dict_hash(compute_tpl)}"

        logger.info(f"Tests uses compute template "
                    f"with name {compute_tpl_name}. Looking up existing "
                    f"templates.")

        paging_token = None
        while not compute_tpl_id:
            result = sdk.search_compute_templates(
                dict(
                    project_id=project_id,
                    name=dict(equals=compute_tpl_name),
                    include_anonymous=True),
                paging_token=paging_token)
            paging_token = result.metadata.next_paging_token

            for res in result.results:
                if res.name == compute_tpl_name:
                    compute_tpl_id = res.id
                    logger.info(
                        f"Template already exists with ID {compute_tpl_id}")
                    break

            if not paging_token:
                break

        if not compute_tpl_id:
            logger.info(f"Compute template not found. "
                        f"Creating with name {compute_tpl_name}.")
            try:
                result = sdk.create_compute_template(
                    dict(
                        name=compute_tpl_name,
                        project_id=project_id,
                        config=compute_tpl))
                compute_tpl_id = result.result.id
            except Exception as e:
                if _repeat:
                    logger.warning(
                        f"Got exception when trying to create compute "
                        f"template: {e}. Sleeping for 10 seconds and then "
                        f"try again once...")
                    time.sleep(10)
                    return create_or_find_compute_template(
                        sdk=sdk,
                        project_id=project_id,
                        compute_tpl=compute_tpl,
                        _repeat=False)

                raise e

            logger.info(f"Compute template created with ID {compute_tpl_id}")

    return compute_tpl_id, compute_tpl_name


def create_or_find_app_config(
        sdk: AnyscaleSDK,
        project_id: str,
        app_config: Dict[Any, Any],
        _repeat: bool = True) -> Tuple[Optional[str], Optional[str]]:
    app_config_id = None
    app_config_name = None
    if app_config:
        app_config_name = f"{project_id}-{_dict_hash(app_config)}"

        logger.info(f"Test uses an app config with hash {app_config_name}. "
                    f"Looking up existing app configs with this name.")

        paging_token = None
        while not app_config_id:
            result = sdk.list_app_configs(
                project_id=project_id, count=50, paging_token=paging_token)
            paging_token = result.metadata.next_paging_token

            for res in result.results:
                if res.name == app_config_name:
                    app_config_id = res.id
                    logger.info(
                        f"App config already exists with ID {app_config_id}")
                    break

            if not paging_token or app_config_id:
                break

        if not app_config_id:
            logger.info("App config not found. Creating new one.")
            try:
                result = sdk.create_app_config(
                    dict(
                        name=app_config_name,
                        project_id=project_id,
                        config_json=app_config))
                app_config_id = result.result.id
            except Exception as e:
                if _repeat:
                    logger.warning(
                        f"Got exception when trying to create app "
                        f"config: {e}. Sleeping for 10 seconds and then "
                        f"try again once...")
                    time.sleep(10)
                    return create_or_find_app_config(
                        sdk=sdk,
                        project_id=project_id,
                        app_config=app_config,
                        _repeat=False)

                raise e

            logger.info(f"App config created with ID {app_config_id}")

    return app_config_id, app_config_name


def install_app_config_packages(app_config: Dict[Any, Any]):
    os.environ.update(app_config.get("env_vars", {}))
    packages = app_config["python"]["pip_packages"]
    for package in packages:
        subprocess.check_output(["pip", "install", "-U", package], text=True)


def install_matching_ray():
    wheel = os.environ.get("RAY_WHEELS", None)
    if not wheel:
        return
    assert "manylinux2014_x86_64" in wheel, wheel
    if sys.platform == "darwin":
        platform = "macosx_10_15_intel"
    elif sys.platform == "win32":
        platform = "win_amd64"
    else:
        platform = "manylinux2014_x86_64"
    wheel = wheel.replace("manylinux2014_x86_64", platform)
    subprocess.check_output(["pip", "uninstall", "-y", "ray"], text=True)
    subprocess.check_output(["pip", "install", "-U", wheel], text=True)


def wait_for_build_or_raise(sdk: AnyscaleSDK,
                            app_config_id: Optional[str]) -> Optional[str]:
    if not app_config_id:
        return None

    # Fetch build
    build_id = None
    last_status = None
    result = sdk.list_builds(app_config_id)
    for build in sorted(result.results, key=lambda b: b.created_at):
        build_id = build.id
        last_status = build.status

        if build.status == "failed":
            continue

        if build.status == "succeeded":
            logger.info(f"Link to app config build: "
                        f"{anyscale_app_config_build_url(build_id)}")
            return build_id

    if last_status == "failed":
        raise RuntimeError("App config build failed.")

    if not build_id:
        raise RuntimeError("No build found for app config.")

    # Build found but not failed/finished yet
    completed = False
    start_wait = time.time()
    next_report = start_wait + REPORT_S
    logger.info(f"Waiting for build {build_id} to finish...")
    logger.info(f"Track progress here: "
                f"{anyscale_app_config_build_url(build_id)}")
    while not completed:
        now = time.time()
        if now > next_report:
            logger.info(f"... still waiting for build {build_id} to finish "
                        f"({int(now - start_wait)} seconds) ...")
            next_report = next_report + REPORT_S

        result = sdk.get_build(build_id)
        build = result.result

        if build.status == "failed":
            raise RuntimeError(
                f"App config build failed. Please see "
                f"{anyscale_app_config_build_url(build_id)} for details")

        if build.status == "succeeded":
            logger.info("Build succeeded.")
            return build_id

        completed = build.status not in ["in_progress", "pending"]

        if completed:
            raise RuntimeError(
                f"Unknown build status: {build.status}. Please see "
                f"{anyscale_app_config_build_url(build_id)} for details")

        time.sleep(1)

    return build_id


def run_job(cluster_name: str, compute_tpl_name: str, cluster_env_name: str,
            job_name: str, min_workers: str, script: str,
            script_args: List[str], env_vars: Dict[str, str],
            autosuspend: int) -> Tuple[int, str]:
    # Start cluster and job
    address = f"anyscale://{cluster_name}?cluster_compute={compute_tpl_name}" \
              f"&cluster_env={cluster_env_name}&autosuspend={autosuspend}" \
               "&&update=True"
    logger.info(f"Starting job {job_name} with Ray address: {address}")
    env = copy.deepcopy(os.environ)
    env.update(GLOBAL_CONFIG)
    env.update(env_vars)
    env["RAY_ADDRESS"] = address
    env["RAY_JOB_NAME"] = job_name
    env["RAY_RELEASE_MIN_WORKERS"] = str(min_workers)
    proc = subprocess.Popen(
        script.split(" ") + script_args,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True)
    proc.stdout.reconfigure(line_buffering=True)
    logs = ""
    for line in proc.stdout:
        logs += line
        sys.stdout.write(line)
    proc.wait()
    return proc.returncode, logs


def create_and_wait_for_session(
        sdk: AnyscaleSDK,
        stop_event: multiprocessing.Event,
        session_name: str,
        session_options: Dict[Any, Any],
) -> str:
    # Create session
    logger.info(f"Creating session {session_name}")
    result = sdk.create_session(session_options)
    session_id = result.result.id

    # Trigger session start
    logger.info(f"Starting session {session_name} ({session_id})")
    session_url = anyscale_session_url(
        project_id=GLOBAL_CONFIG["ANYSCALE_PROJECT"], session_id=session_id)
    logger.info(f"Link to session: {session_url}")

    result = sdk.start_session(session_id, start_session_options={})
    sop_id = result.result.id
    completed = result.result.completed

    # Wait for session
    logger.info(f"Waiting for session {session_name}...")
    start_wait = time.time()
    next_report = start_wait + REPORT_S
    while not completed:
        _check_stop(stop_event, "session")
        now = time.time()
        if now > next_report:
            logger.info(f"... still waiting for session {session_name} "
                        f"({int(now - start_wait)} seconds) ...")
            next_report = next_report + REPORT_S

        session_operation_response = sdk.get_session_operation(
            sop_id, _request_timeout=30)
        session_operation = session_operation_response.result
        completed = session_operation.completed
        time.sleep(1)

    return session_id


def run_session_command(sdk: AnyscaleSDK,
                        session_id: str,
                        cmd_to_run: str,
                        result_queue: multiprocessing.Queue,
                        env_vars: Dict[str, str],
                        state_str: str = "CMD_RUN") -> Tuple[str, int]:
    full_cmd = " ".join(f"{k}={v}"
                        for k, v in env_vars.items()) + " " + cmd_to_run

    logger.info(f"Running command in session {session_id}: \n" f"{full_cmd}")
    session_url = anyscale_session_url(
        project_id=GLOBAL_CONFIG["ANYSCALE_PROJECT"], session_id=session_id)
    logger.info(f"Link to session: {session_url}")
    result_queue.put(State(state_str, time.time(), None))
    result = sdk.create_session_command(
        dict(session_id=session_id, shell_command=full_cmd))

    scd_id = result.result.id
    return scd_id, result


def wait_for_session_command_to_complete(create_session_command_result,
                                         sdk: AnyscaleSDK,
                                         scd_id: str,
                                         stop_event: multiprocessing.Event,
                                         state_str: str = "CMD_RUN"):
    result = create_session_command_result
    completed = result.result.finished_at is not None
    start_wait = time.time()
    next_report = start_wait + REPORT_S
    while not completed:
        if state_str == "CMD_RUN":
            _check_stop(stop_event, "command")
        elif state_str == "CMD_PREPARE":
            _check_stop(stop_event, "prepare_command")

        now = time.time()
        if now > next_report:
            logger.info(f"... still waiting for command to finish "
                        f"({int(now - start_wait)} seconds) ...")
            next_report = next_report + REPORT_S

        result = sdk.get_session_command(session_command_id=scd_id)
        completed = result.result.finished_at
        time.sleep(1)

    status_code = result.result.status_code
    runtime = time.time() - start_wait

    if status_code != 0:
        if state_str == "CMD_RUN":
            raise RuntimeError(
                f"Command returned non-success status: {status_code}")
        elif state_str == "CMD_PREPARE":
            raise PrepareCommandRuntimeError(
                f"Prepare command returned non-success status: {status_code}")

    return status_code, runtime


def get_command_logs(session_controller: SessionController,
                     scd_id: str,
                     lines: int = 50):
    result = session_controller.api_client.get_execution_logs_api_v2_session_commands_session_command_id_execution_logs_get(  # noqa: E501
        session_command_id=scd_id,
        start_line=-1 * lines,
        end_line=0)

    return result.result.lines


def get_remote_json_content(
        temp_dir: str,
        session_name: str,
        remote_file: Optional[str],
        session_controller: SessionController,
):
    if not remote_file:
        logger.warning("No remote file specified, returning empty dict")
        return {}
    local_target_file = os.path.join(temp_dir, ".tmp.json")
    session_controller.pull(
        session_name=session_name,
        source=remote_file,
        target=local_target_file)
    with open(local_target_file, "rt") as f:
        return json.load(f)


def get_local_json_content(local_file: Optional[str], ):
    if not local_file:
        logger.warning("No local file specified, returning empty dict")
        return {}
    with open(local_file, "rt") as f:
        return json.load(f)


def pull_artifacts_and_store_in_cloud(
        temp_dir: str,
        logs: str,
        session_name: str,
        test_name: str,
        artifacts: Optional[Dict[Any, Any]],
        session_controller: SessionController,
):
    output_log_file = os.path.join(temp_dir, "output.log")
    with open(output_log_file, "wt") as f:
        f.write(logs)

    bucket = GLOBAL_CONFIG["RELEASE_AWS_BUCKET"]
    location = f"{GLOBAL_CONFIG['RELEASE_AWS_LOCATION']}" \
               f"/{session_name}/{test_name}"
    saved_artifacts = {}

    s3_client = boto3.client("s3")
    s3_client.upload_file(output_log_file, bucket, f"{location}/output.log")
    saved_artifacts["output.log"] = f"s3://{bucket}/{location}/output.log"

    # Download artifacts
    if artifacts:
        for name, remote_file in artifacts.items():
            logger.info(f"Downloading artifact `{name}` from "
                        f"{remote_file}")
            local_target_file = os.path.join(temp_dir, name)
            session_controller.pull(
                session_name=session_name,
                source=remote_file,
                target=local_target_file)

            # Upload artifacts to s3
            s3_client.upload_file(local_target_file, bucket,
                                  f"{location}/{name}")
            saved_artifacts[name] = f"s3://{bucket}/{location}/{name}"

    return saved_artifacts


def find_session_by_test_name(
        sdk: AnyscaleSDK,
        session_controller: SessionController,
        temp_dir: str,
        state_json: str,
        project_id: str,
        test_name: str,
) -> Optional[Tuple[str, str, Dict[Any, Any]]]:
    paging_token = None

    while True:  # Will break if paging_token is None after first search
        result = sdk.search_sessions(
            project_id=project_id,
            sessions_query=dict(
                name=dict(contains=test_name),
                state_filter=["Running"],
                paging=dict(count=20, paging_token=paging_token)))

        for session in result.results:
            logger.info(f"Found sessions {session.name}")
            if not session.name.startswith(test_name):
                continue

            try:
                session_state = get_remote_json_content(
                    temp_dir=temp_dir,
                    session_name=session.name,
                    remote_file=state_json,
                    session_controller=session_controller)
            except Exception as exc:
                raise RuntimeError(f"Could not get remote json content "
                                   f"for session {session.name}") from exc

            if session_state.get("test_name") == test_name:
                return session.id, session.name, session_state

        session_token = result.metadata.next_paging_token

        if not session_token:
            return None


def get_latest_running_command_id(sdk: AnyscaleSDK, session_id: str
                                  ) -> Tuple[Optional[str], Optional[bool]]:
    scd_id = None
    paging_token = None

    success = None

    while not scd_id:
        result = sdk.list_session_commands(
            session_id=session_id, paging_token=paging_token)

        paging_token = result.metadata.next_paging_token

        for cmd in result.results:
            if not scd_id:
                scd_id = cmd.id

            completed = cmd.finished_at is not None

            if completed:
                if success is None:
                    success = True

                success = success and cmd.status_code == 0

            if not completed:
                return cmd.id, None

    return scd_id, success or False


def run_test_config(
        local_dir: str,
        project_id: str,
        test_name: str,
        test_config: Dict[Any, Any],
        commit_url: str,
        session_name: str = None,
        smoke_test: bool = False,
        no_terminate: bool = False,
        kick_off_only: bool = False,
        check_progress: bool = False,
        upload_artifacts: bool = True,
        app_config_id_override: Optional[str] = None,
) -> Dict[Any, Any]:
    """

    Returns:
        Dict with the following entries:
            status (str): One of [finished, error, timeout]
            command_link (str): Link to command (Anyscale web UI)
            last_logs (str): Last logs (excerpt) to send to owner
            artifacts (dict): Dict of artifacts
                Key: Name
                Value: S3 URL
    """
    # Todo (mid-term): Support other cluster definitions
    #  (not only cluster configs)
    cluster_config_rel_path = test_config["cluster"].get(
        "cluster_config", None)
    cluster_config = _load_config(local_dir, cluster_config_rel_path)

    app_config_rel_path = test_config["cluster"].get("app_config", None)
    app_config = _load_config(local_dir, app_config_rel_path)

    compute_tpl_rel_path = test_config["cluster"].get("compute_template", None)
    compute_tpl = _load_config(local_dir, compute_tpl_rel_path)

    stop_event = multiprocessing.Event()
    result_queue = multiprocessing.Queue()

    if not session_name:
        session_name = f"{test_name}_{int(time.time())}"

    temp_dir = tempfile.mkdtemp()

    # Result and state files
    results_json = test_config["run"].get("results", None)
    if results_json is None:
        results_json = "/tmp/release_test_out.json"

    state_json = test_config["run"].get("state", None)
    if state_json is None:
        state_json = "/tmp/release_test_state.json"

    env_vars = {
        "RAY_ADDRESS": os.environ.get("RAY_ADDRESS", "auto"),
        "TEST_OUTPUT_JSON": results_json,
        "TEST_STATE_JSON": state_json,
        "IS_SMOKE_TEST": "1" if smoke_test else "0",
    }

    with open(os.path.join(local_dir, ".anyscale.yaml"), "wt") as f:
        f.write(f"project_id: {project_id}")
    os.chdir(local_dir)

    # Setup interface
    # Unfortunately, there currently seems to be no great way to
    # transfer files with the Anyscale SDK.
    # So we use the session controller instead.
    sdk = AnyscaleSDK(auth_token=GLOBAL_CONFIG["ANYSCALE_CLI_TOKEN"])

    session_controller = SessionController(
        api_client=instantiate_api_client(
            cli_token=GLOBAL_CONFIG["ANYSCALE_CLI_TOKEN"],
            host=GLOBAL_CONFIG["ANYSCALE_HOST"],
        ),
        anyscale_api_client=sdk.api_client,
    )

    timeout = test_config["run"].get("timeout", 1800)
    if "RELEASE_OVERRIDE_TIMEOUT" in os.environ:
        previous_timeout = timeout
        timeout = int(os.environ.get("RELEASE_OVERRIDE_TIMEOUT", str(timeout)))
        logger.warning(f"Release test timeout override: {timeout} "
                       f"(would have been {previous_timeout})")

    # If a test is long running, timeout does not mean it failed
    is_long_running = test_config["run"].get("long_running", False)

    build_id_override = None
    if test_config["run"].get("use_connect"):
        autosuspend_mins = test_config["run"].get("autosuspend_mins", 5)
        assert not kick_off_only, \
            "Unsupported for running with Anyscale connect."
        if app_config_id_override is not None:
            logger.info(
                "Using connect and an app config override, waiting until "
                "build finishes so we can fetch the app config in order to "
                "install its pip packages locally.")
            build_id_override = wait_for_build_or_raise(
                sdk, app_config_id_override)
            response = sdk.get_cluster_environment_build(build_id_override)
            app_config = response.result.config_json
        install_app_config_packages(app_config)
        install_matching_ray()
    elif "autosuspend_mins" in test_config["run"]:
        raise ValueError(
            "'autosuspend_mins' is only supported if 'use_connect' is True.")

    # Add information to results dict
    def _update_results(results: Dict):
        if "last_update" in results:
            results["last_update_diff"] = time.time() - results["last_update"]
        if smoke_test:
            results["smoke_test"] = True

    def _process_finished_command(session_controller: SessionController,
                                  scd_id: str,
                                  results: Optional[Dict] = None,
                                  runtime: int = None,
                                  commit_url: str = None,
                                  session_url: str = None):
        logger.info("Command finished successfully.")
        if results_json:
            results = results or get_remote_json_content(
                temp_dir=temp_dir,
                session_name=session_name,
                remote_file=results_json,
                session_controller=session_controller,
            )
        else:
            results = {"passed": 1}

        _update_results(results)

        if scd_id:
            logs = get_command_logs(session_controller, scd_id,
                                    test_config.get("log_lines", 50))
        else:
            logs = "No command found to fetch logs for"

        if upload_artifacts:
            saved_artifacts = pull_artifacts_and_store_in_cloud(
                temp_dir=temp_dir,
                logs=logs,  # Also save logs in cloud
                session_name=session_name,
                test_name=test_name,
                artifacts=test_config.get("artifacts", {}),
                session_controller=session_controller,
            )

            logger.info("Fetched results and stored on the cloud. Returning.")
        else:
            saved_artifacts = {}
            logger.info("Usually I would have fetched the results and "
                        "artifacts and stored them on S3.")

        # Add these metadata here to avoid changing SQL schema.
        results["_runtime"] = runtime
        results["_session_url"] = session_url
        results["_commit_url"] = commit_url
        results["_stable"] = test_config.get("stable", True)
        result_queue.put(
            State(
                "END",
                time.time(),
                {
                    "status": "finished",
                    "last_logs": logs,
                    "results": results,
                    "artifacts": saved_artifacts,
                },
            ))

    # When running the test script in client mode, the finish command is a
    # completed local process.
    def _process_finished_client_command(returncode: int, logs: str):
        if upload_artifacts:
            saved_artifacts = pull_artifacts_and_store_in_cloud(
                temp_dir=temp_dir,
                logs=logs,  # Also save logs in cloud
                session_name=session_name,
                test_name=test_name,
                artifacts=None,
                session_controller=None,
            )
            logger.info("Stored results on the cloud. Returning.")
        else:
            saved_artifacts = {}
            logger.info("Usually I would have fetched the results and "
                        "artifacts and stored them on S3.")

        if results_json:
            results = get_local_json_content(local_file=results_json, )
        else:
            results = {
                "passed": int(returncode == 0),
            }

        results["returncode"] = returncode

        _update_results(results)

        result_queue.put(
            State(
                "END",
                time.time(),
                {
                    "status": "finished",
                    "last_logs": logs,
                    "results": results,
                    "artifacts": saved_artifacts,
                },
            ))

    def _run(logger):
        # These values will be set as the test runs.
        session_url = None
        runtime = None
        anyscale.conf.CLI_TOKEN = GLOBAL_CONFIG["ANYSCALE_CLI_TOKEN"]

        session_id = None
        scd_id = None
        try:
            # First, look for running sessions
            session_id = search_running_session(sdk, project_id, session_name)
            compute_tpl_name = None
            app_config_id = app_config_id_override
            app_config_name = None
            build_id = build_id_override
            if not session_id:
                logger.info("No session found.")
                # Start session
                session_options = dict(
                    name=session_name, project_id=project_id)

                if cluster_config is not None:
                    logging.info("Starting session with cluster config")
                    cluster_config_str = json.dumps(cluster_config)
                    session_options["cluster_config"] = cluster_config_str
                    session_options["cloud_id"] = (
                        GLOBAL_CONFIG["ANYSCALE_CLOUD_ID"], )
                    session_options["uses_app_config"] = False
                else:
                    logging.info("Starting session with app/compute config")

                    # Find/create compute template
                    compute_tpl_id, compute_tpl_name = \
                        create_or_find_compute_template(
                            sdk, project_id, compute_tpl)

                    logger.info(f"Link to compute template: "
                                f"{anyscale_compute_tpl_url(compute_tpl_id)}")

                    # Find/create app config
                    if app_config_id is None:
                        (
                            app_config_id,
                            app_config_name,
                        ) = create_or_find_app_config(sdk, project_id,
                                                      app_config)
                    else:
                        logger.info(
                            f"Using override app config {app_config_id}")
                        app_config_name = sdk.get_app_config(
                            app_config_id).result.name
                    if build_id is None:
                        # We might have already retrieved the build ID when
                        # installing app config packages locally if using
                        # connect, so only get the build ID if it's not set.
                        build_id = wait_for_build_or_raise(sdk, app_config_id)

                    session_options["compute_template_id"] = compute_tpl_id
                    session_options["build_id"] = build_id
                    session_options["uses_app_config"] = True

                if not test_config["run"].get("use_connect"):
                    session_id = create_and_wait_for_session(
                        sdk=sdk,
                        stop_event=stop_event,
                        session_name=session_name,
                        session_options=session_options,
                    )

            if test_config["run"].get("use_connect"):
                assert compute_tpl_name, "Compute template must exist."
                assert app_config_name, "Cluster environment must exist."
                script_args = test_config["run"].get("args", [])
                if smoke_test:
                    script_args += ["--smoke-test"]
                min_workers = 0
                for node_type in compute_tpl["worker_node_types"]:
                    min_workers += node_type["min_workers"]
                # Build completed, use job timeout
                result_queue.put(State("CMD_RUN", time.time(), None))
                returncode, logs = run_job(
                    cluster_name=test_name,
                    compute_tpl_name=compute_tpl_name,
                    cluster_env_name=app_config_name,
                    job_name=session_name,
                    min_workers=min_workers,
                    script=test_config["run"]["script"],
                    script_args=script_args,
                    env_vars=env_vars,
                    autosuspend=autosuspend_mins)
                _process_finished_client_command(returncode, logs)
                return

            # Write test state json
            test_state_file = os.path.join(local_dir, "test_state.json")
            with open(test_state_file, "wt") as f:
                json.dump({
                    "start_time": time.time(),
                    "test_name": test_name
                }, f)

            # Rsync up
            logger.info("Syncing files to session...")
            session_controller.push(
                session_name=session_name,
                source=None,
                target=None,
                config=None,
                all_nodes=False,
            )

            logger.info("Syncing test state to session...")
            session_controller.push(
                session_name=session_name,
                source=test_state_file,
                target=state_json,
                config=None,
                all_nodes=False,
            )

            session_url = anyscale_session_url(
                project_id=GLOBAL_CONFIG["ANYSCALE_PROJECT"],
                session_id=session_id)
            _check_stop(stop_event, "file_sync")

            # Optionally run preparation command
            prepare_command = test_config["run"].get("prepare")
            if prepare_command:
                logger.info(f"Running preparation command: {prepare_command}")
                scd_id, result = run_session_command(
                    sdk=sdk,
                    session_id=session_id,
                    cmd_to_run=prepare_command,
                    result_queue=result_queue,
                    env_vars=env_vars,
                    state_str="CMD_PREPARE")
                _, _ = wait_for_session_command_to_complete(
                    result,
                    sdk=sdk,
                    scd_id=scd_id,
                    stop_event=stop_event,
                    state_str="CMD_PREPARE")

            # Run release test command
            cmd_to_run = test_config["run"]["script"] + " "

            args = test_config["run"].get("args", [])
            if args:
                cmd_to_run += " ".join(args) + " "

            if smoke_test:
                cmd_to_run += " --smoke-test"

            scd_id, result = run_session_command(
                sdk=sdk,
                session_id=session_id,
                cmd_to_run=cmd_to_run,
                result_queue=result_queue,
                env_vars=env_vars,
                state_str="CMD_RUN")

            if not kick_off_only:
                _, runtime = wait_for_session_command_to_complete(
                    result,
                    sdk=sdk,
                    scd_id=scd_id,
                    stop_event=stop_event,
                    state_str="CMD_RUN")
                _process_finished_command(
                    session_controller=session_controller,
                    scd_id=scd_id,
                    runtime=runtime,
                    session_url=session_url,
                    commit_url=commit_url)
            else:
                result_queue.put(
                    State("END", time.time(), {
                        "status": "kickoff",
                        "last_logs": ""
                    }))

        except (ReleaseTestTimeoutError, Exception) as e:
            logger.error(e, exc_info=True)

            logs = str(e)
            if scd_id is not None:
                try:
                    logs = logs + "; Command logs:" + get_command_logs(
                        session_controller, scd_id,
                        test_config.get("log_lines", 50))
                except Exception as e2:
                    logger.error(e2, exc_info=True)

            # Long running tests are "finished" successfully when
            # timed out
            if isinstance(e, ReleaseTestTimeoutError) and is_long_running:
                _process_finished_command(
                    session_controller=session_controller, scd_id=scd_id)
            else:
                timeout_type = ""
                runtime = None
                if isinstance(e, CommandTimeoutError):
                    timeout_type = "timeout"
                    runtime = 0
                elif (isinstance(e, PrepareCommandTimeoutError)
                      or isinstance(e, FileSyncTimeoutError)
                      or isinstance(e, SessionTimeoutError)
                      or isinstance(e, PrepareCommandRuntimeError)):
                    timeout_type = "infra_timeout"
                    runtime = None
                elif isinstance(e, RuntimeError):
                    timeout_type = "runtime_error"
                    runtime = 0
                else:
                    timeout_type = "unknown timeout"
                    runtime = None

                # Add these metadata here to avoid changing SQL schema.
                results = {}
                results["_runtime"] = runtime
                results["_session_url"] = session_url
                results["_commit_url"] = commit_url
                results["_stable"] = test_config.get("stable", True)
                result_queue.put(
                    State(
                        "END", time.time(), {
                            "status": timeout_type,
                            "last_logs": logs,
                            "results": results
                        }))
        finally:
            if no_terminate:
                logger.warning(
                    "`no_terminate` is set to True, so the session will "
                    "*not* be terminated!")
            else:
                _cleanup_session(sdk, session_id)

    def _check_progress(logger):
        anyscale.conf.CLI_TOKEN = GLOBAL_CONFIG["ANYSCALE_CLI_TOKEN"]

        should_terminate = False
        session_id = None
        scd_id = None
        try:
            existing_session = find_session_by_test_name(
                sdk=sdk,
                session_controller=session_controller,
                temp_dir=temp_dir,
                state_json=state_json,
                project_id=project_id,
                test_name=test_name)

            if existing_session is None:
                logger.info(f"Found no existing session for {test_name}")
                result_queue.put(
                    State("END", time.time(), {
                        "status": "nosession",
                        "last_logs": ""
                    }))
                return

            session_id, session_name, session_state = existing_session

            logger.info(f"Found existing session for {test_name}: "
                        f"{session_name}")

            scd_id, success = get_latest_running_command_id(
                sdk=sdk, session_id=session_id)

            latest_result = get_remote_json_content(
                temp_dir=temp_dir,
                session_name=session_name,
                remote_file=results_json,
                session_controller=session_controller,
            )

            # Fetch result json and check if it has been updated recently
            result_time_key = test_config["run"].get("time_key", None)
            maximum_update_delay = test_config["run"].get(
                "max_update_delay", None)

            if result_time_key and maximum_update_delay:
                last_update = latest_result.get(result_time_key, None)

                if not last_update:
                    result_queue.put(
                        State(
                            "END", time.time(), {
                                "status": "error",
                                "last_logs": f"Test did not store "
                                f"{result_time_key} in the "
                                f"results json."
                            }))
                    return

                delay = time.time() - last_update
                logger.info(f"Last update was at {last_update:.2f}. "
                            f"This was {delay:.2f} seconds ago "
                            f"(maximum allowed: {maximum_update_delay})")

                if delay > maximum_update_delay:
                    raise RuntimeError(
                        f"Test did not update the results json within "
                        f"the last {maximum_update_delay} seconds.")

            if time.time() - session_state["start_time"] > timeout:
                # Long running test reached timeout
                logger.info(
                    f"Test command reached timeout after {timeout} seconds")
                _process_finished_command(
                    session_controller=session_controller,
                    scd_id=scd_id,
                    results=latest_result)
                should_terminate = True

            elif success:
                logger.info("All commands finished.")
                _process_finished_command(
                    session_controller=session_controller,
                    scd_id=scd_id,
                    results=latest_result)
                should_terminate = True

            else:
                rest_time = timeout - time.time() + session_state["start_time"]
                logger.info(f"Test command should continue running "
                            f"for {rest_time} seconds")
                result_queue.put(
                    State("END", time.time(), {
                        "status": "kickoff",
                        "last_logs": "Test is still running"
                    }))

        except Exception as e:
            logger.error(e, exc_info=True)

            logs = str(e)
            if scd_id is not None:
                try:
                    logs = get_command_logs(session_controller, scd_id,
                                            test_config.get("log_lines", 50))
                    logs += f"\n{str(e)}"
                except Exception as e2:
                    logger.error(e2, exc_info=True)

            result_queue.put(
                State("END", time.time(), {
                    "status": "error",
                    "last_logs": logs
                }))
            should_terminate = True
        finally:
            if should_terminate:
                logger.warning("Terminating session")
                _cleanup_session(sdk, session_id)

    if not check_progress:
        process = multiprocessing.Process(target=_run, args=(logger, ))
    else:
        process = multiprocessing.Process(
            target=_check_progress, args=(logger, ))

    build_timeout = test_config["run"].get("build_timeout", 1800)

    project_url = anyscale_project_url(
        project_id=GLOBAL_CONFIG["ANYSCALE_PROJECT"])
    logger.info(f"Link to project: {project_url}")

    msg = f"This will now run test {test_name}."
    if smoke_test:
        msg += " This is a smoke test."
    if is_long_running:
        msg += " This is a long running test."
    logger.info(msg)

    logger.info(f"Starting process with timeout {timeout} "
                f"(build timeout {build_timeout})")
    process.start()

    # The timeout time will be updated after the build finished
    # Build = App config + compute template build and session start
    timeout_time = time.time() + build_timeout

    result = {}
    while process.is_alive():
        try:
            state: State = result_queue.get(timeout=1)
        except (Empty, TimeoutError):
            if time.time() > timeout_time:
                stop_event.set()
                logger.warning("Process timed out.")

                if not is_long_running:
                    logger.warning("Terminating process in 10 seconds.")
                    time.sleep(10)
                    logger.warning("Terminating process now.")
                    process.terminate()
                else:
                    logger.info("Process is long running. Give 2 minutes to "
                                "fetch result and terminate.")
                    start_terminate = time.time()
                    while time.time(
                    ) < start_terminate + 120 and process.is_alive():
                        time.sleep(1)
                    if process.is_alive():
                        logger.warning("Terminating forcefully now.")
                        process.terminate()
                    else:
                        logger.info("Long running results collected.")
                break
            continue

        if not isinstance(state, State):
            raise RuntimeError(f"Expected `State` object, got {result}")

        if state.state == "CMD_PREPARE":
            # Reset timeout after build finished
            timeout_time = state.timestamp + timeout

        if state.state == "CMD_RUN":
            # Reset timeout after prepare command or build finished
            timeout_time = state.timestamp + timeout

        elif state.state == "END":
            result = state.data
            break

    while not result_queue.empty():
        state = result_queue.get_nowait()
        result = state.data

    logger.info("Final check if everything worked.")
    try:
        result.setdefault("status", "error (status not found)")
    except (TimeoutError, Empty):
        result = {"status": "timeout", "last_logs": "Test timed out."}

    logger.info(f"Final results: {result}")

    log_results_and_artifacts(result)

    shutil.rmtree(temp_dir)

    return result


def run_test(test_config_file: str,
             test_name: str,
             project_id: str,
             commit_url: str,
             category: str = "unspecified",
             smoke_test: bool = False,
             no_terminate: bool = False,
             kick_off_only: bool = False,
             check_progress=False,
             report=True,
             session_name=None,
             app_config_id_override=None):
    with open(test_config_file, "rt") as f:
        test_configs = yaml.load(f, Loader=yaml.FullLoader)

    test_config_dict = {}
    for test_config in test_configs:
        name = test_config.pop("name")
        test_config_dict[name] = test_config

    if test_name not in test_config_dict:
        raise ValueError(
            f"Test with name `{test_name}` not found in test config file "
            f"at `{test_config_file}`.")

    test_config = test_config_dict[test_name]

    if smoke_test and "smoke_test" in test_config:
        smoke_test_config = test_config.pop("smoke_test")
        test_config = _deep_update(test_config, smoke_test_config)

    local_dir = os.path.dirname(test_config_file)
    if "local_dir" in test_config:
        # local_dir is relative to test_config_file
        local_dir = os.path.join(local_dir, test_config["local_dir"])

    if test_config["run"].get("use_connect"):
        assert not kick_off_only, \
            "--kick-off-only is unsupported when running with " \
            "Anyscale connect."
        assert not check_progress, \
            "--check is unsupported when running with Anyscale connect."
        if test_config.get("artifacts", {}):
            logger.error(
                "Saving artifacts are not yet supported when running with "
                "Anyscale connect.")

    result = run_test_config(
        local_dir,
        project_id,
        test_name,
        test_config,
        commit_url,
        session_name=session_name,
        smoke_test=smoke_test,
        no_terminate=no_terminate,
        kick_off_only=kick_off_only,
        check_progress=check_progress,
        upload_artifacts=report,
        app_config_id_override=app_config_id_override)

    status = result.get("status", "invalid")

    if kick_off_only:
        if status != "kickoff":
            raise RuntimeError("Error kicking off test.")

        logger.info("Kicked off test. It's now up to the `--check` "
                    "part of the script to track its process.")
        return
    else:
        # `--check` or no kick off only

        if status == "nosession":
            logger.info(f"No running session found for test {test_name}, so "
                        f"assuming everything is fine.")
            return

        if status == "kickoff":
            logger.info(f"Test {test_name} is still running.")
            return

        last_logs = result.get("last_logs", "No logs.")

        test_suite = os.path.basename(test_config_file).replace(".yaml", "")

        report_kwargs = dict(
            test_suite=test_suite,
            test_name=test_name,
            status=status,
            logs=last_logs,
            results=result.get("results", {}),
            artifacts=result.get("artifacts", {}),
            category=category,
        )

        if report:
            report_result(**report_kwargs)
        else:
            logger.info(f"Usually I would now report the following results:\n"
                        f"{report_kwargs}")

        if has_errored(result):
            raise RuntimeError(last_logs)

    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--test-config", type=str, required=True, help="Test config file")
    parser.add_argument("--test-name", type=str, help="Test name in config")
    parser.add_argument(
        "--ray-wheels", required=False, type=str, help="URL to ray wheels")
    parser.add_argument(
        "--no-terminate",
        action="store_true",
        default=False,
        help="Don't terminate session after failure")
    parser.add_argument(
        "--no-report",
        action="store_true",
        default=False,
        help="Do not report any results or upload to S3")
    parser.add_argument(
        "--kick-off-only",
        action="store_true",
        default=False,
        help="Kick off only (don't wait for command to finish)")
    parser.add_argument(
        "--check",
        action="store_true",
        default=False,
        help="Check (long running) status")
    parser.add_argument(
        "--category",
        type=str,
        default="unspecified",
        help="Category name, e.g. `release-1.3.0` (will be saved in database)")
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    parser.add_argument(
        "--session-name",
        required=False,
        type=str,
        help="Name of the session to run this test.")
    parser.add_argument(
        "--app-config-id-override",
        required=False,
        type=str,
        help=("An app config ID, which will override the test config app "
              "config."))
    args, _ = parser.parse_known_args()

    if not GLOBAL_CONFIG["ANYSCALE_PROJECT"]:
        raise RuntimeError(
            "You have to set the ANYSCALE_PROJECT environment variable!")

    maybe_fetch_api_token()

    if args.ray_wheels:
        os.environ["RAY_WHEELS"] = str(args.ray_wheels)
        url = str(args.ray_wheels)
    elif not args.check:
        url = find_ray_wheels(
            GLOBAL_CONFIG["RAY_REPO"],
            GLOBAL_CONFIG["RAY_BRANCH"],
            GLOBAL_CONFIG["RAY_VERSION"],
        )
        if not url:
            raise RuntimeError(f"Could not find wheels for "
                               f"Ray {GLOBAL_CONFIG['RAY_VERSION']}, "
                               f"branch {GLOBAL_CONFIG['RAY_BRANCH']}")

    test_config_file = os.path.abspath(os.path.expanduser(args.test_config))

    run_test(
        test_config_file=test_config_file,
        test_name=args.test_name,
        project_id=GLOBAL_CONFIG["ANYSCALE_PROJECT"],
        commit_url=url,
        category=args.category,
        smoke_test=args.smoke_test,
        no_terminate=args.no_terminate or args.kick_off_only,
        kick_off_only=args.kick_off_only,
        check_progress=args.check,
        report=not args.no_report,
        session_name=args.session_name,
        app_config_id_override=args.app_config_id_override,
    )
