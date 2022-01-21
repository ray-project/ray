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

Exit codes
----------
The script exits with code 0 on success, i.e. if the test has been run
end to end without failures and the subsequent results checks have passed.
In all other cases, an exit code > 0 is returned.

Exit code 1 is the general failure exit code returned by Python when we
encounter an error that isn't caught by the rest of the script.

Generally, we try to catch errors as they occur, and return a specific exit
code that can be used in automation tools to e.g. retry a test when nodes
didn't come up in time.

These exit codes are defined in the ``ExitCode`` enum below.

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

  - pip3 uninstall ray -y || true
  - pip3 install -U {{ env["RAY_WHEELS"] | default("ray") }}

If you want to force rebuilds, consider using something like

  - echo {{ env["TIMESTAMP"] }}

so that your app configs changes each time the script is executed. If you
only want to trigger rebuilds once per day, use `DATESTAMP` instead:

  - echo {{ env["DATESTAMP"] }}

Local testing
-------------
Make sure to set these environment variables:

- ANYSCALE_CLI_TOKEN (should contain your anyscale credential token)
- ANYSCALE_PROJECT (should point to a project ID you have access to)

A test can then be run like this:

python e2e.py --test-config ~/ray/release/xgboost_tests/xgboost_tests.yaml --test-name tune_small

Using Compilation on Product + App Config Override
--------------------------------------------------
For quick iteration when debugging a release test, go/compile-on-product allows
you to easily modify and recompile Ray, such that the recompilation happens
within an app build step and can benefit from a warm Bazel cache. See
go/compile-on-product for more information.

After kicking off the app build, you can give the app config ID to this script
as an app config override, where the indicated app config will be used instead
of the app config given in the test config. E.g., running

python e2e.py --test-config ~/ray/benchmarks/benchmark_tests.yaml --test-name=single_node --app-config-id-override=apt_TBngEXXXrhipMXgexVcrpC9i

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
import enum
import random
import string
import shlex

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
import re
import tempfile
import time
from queue import Empty
from typing import Any, Dict, Optional, Tuple, List

import yaml

import anyscale
import anyscale.conf
from anyscale.authenticate import get_auth_api_client
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


def _format_link(link: str):
    # Use ANSI escape code to allow link to be clickable
    # https://buildkite.com/docs/pipelines/links-and-images
    # -in-log-output
    return "\033]1339;url='" + link + "'\a\n"


def getenv_default(key: str, default: Optional[str] = None):
    """Return environment variable with default value"""
    # If the environment variable is set but "", still return default
    return os.environ.get(key, None) or default


GLOBAL_CONFIG = {
    "ANYSCALE_USER": getenv_default("ANYSCALE_USER",
                                    "release-automation@anyscale.com"),
    "ANYSCALE_HOST": getenv_default("ANYSCALE_HOST",
                                    "https://console.anyscale.com"),
    "ANYSCALE_CLI_TOKEN": getenv_default("ANYSCALE_CLI_TOKEN"),
    "ANYSCALE_CLOUD_ID": getenv_default(
        "ANYSCALE_CLOUD_ID",
        "cld_4F7k8814aZzGG8TNUGPKnc"),  # anyscale_default_cloud
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
    "RELEASE_RESULTS_DIR": getenv_default("RELEASE_RESULTS_DIR",
                                          "/tmp/ray_release_test_artifacts"),
    "DATESTAMP": str(datetime.datetime.now().strftime("%Y%m%d")),
    "TIMESTAMP": str(int(datetime.datetime.now().timestamp())),
    "EXPIRATION_1D": str((datetime.datetime.now() +
                          datetime.timedelta(days=1)).strftime("%Y-%m-%d")),
    "EXPIRATION_2D": str((datetime.datetime.now() +
                          datetime.timedelta(days=2)).strftime("%Y-%m-%d")),
    "EXPIRATION_3D": str((datetime.datetime.now() +
                          datetime.timedelta(days=3)).strftime("%Y-%m-%d")),
    "REPORT_RESULT": getenv_default("REPORT_RESULT", ""),
}

REPORT_S = 30
RETRY_MULTIPLIER = 2
VALID_TEAMS = ["ml", "core", "serve"]


class ExitCode(enum.Enum):
    # If you change these, also change the `retry` section
    # in `build_pipeline.py` and the `reason()` function in `run_e2e.sh`
    UNSPECIFIED = 2
    UNKNOWN = 3
    RUNTIME_ERROR = 4
    COMMAND_ERROR = 5
    COMMAND_TIMEOUT = 6
    PREPARE_TIMEOUT = 7
    FILESYNC_TIMEOUT = 8
    SESSION_TIMEOUT = 9
    PREPARE_ERROR = 10
    APPCONFIG_BUILD_ERROR = 11
    INFRA_ERROR = 12


def exponential_backoff_retry(f, retry_exceptions, initial_retry_delay_s,
                              max_retries):
    retry_cnt = 0
    retry_delay_s = initial_retry_delay_s
    while True:
        try:
            return f()
        except retry_exceptions as e:
            retry_cnt += 1
            if retry_cnt > max_retries:
                raise
            logger.info(f"Retry function call failed due to {e} "
                        f"in {retry_delay_s} seconds...")
            time.sleep(retry_delay_s)
            retry_delay_s *= RETRY_MULTIPLIER


def maybe_fetch_api_token():
    if GLOBAL_CONFIG["ANYSCALE_CLI_TOKEN"] is None:
        logger.info(
            "Missing ANYSCALE_CLI_TOKEN, retrieving from AWS secrets store")
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


class ReleaseTestRuntimeError(RuntimeError):
    pass


class ReleaseTestInfraError(ReleaseTestRuntimeError):
    pass


class ReleaseTestTimeoutError(ReleaseTestRuntimeError):
    pass


class SessionTimeoutError(ReleaseTestTimeoutError):
    pass


class FileSyncTimeoutError(ReleaseTestTimeoutError):
    pass


class CommandTimeoutError(ReleaseTestTimeoutError):
    pass


class PrepareCommandTimeoutError(ReleaseTestTimeoutError):
    pass


# e.g., App config failure.
class AppConfigBuildFailure(RuntimeError):
    pass


class State:
    def __init__(self, state: str, timestamp: float, data: Any):
        self.state = state
        self.timestamp = timestamp
        self.data = data


class CommandRunnerHack:
    def __init__(self):
        self.subprocess_pool: Dict[int, subprocess.Popen] = dict()
        self.start_time: Dict[int, float] = dict()
        self.counter = 0

    def run_command(self, session_name, cmd_to_run, env_vars) -> int:
        self.counter += 1
        command_id = self.counter
        env = os.environ.copy()
        env["RAY_ADDRESS"] = f"anyscale://{session_name}"
        env["ANYSCALE_CLI_TOKEN"] = GLOBAL_CONFIG["ANYSCALE_CLI_TOKEN"]
        env["ANYSCALE_HOST"] = GLOBAL_CONFIG["ANYSCALE_HOST"]
        full_cmd = " ".join(f"{k}={v}"
                            for k, v in env_vars.items()) + " " + cmd_to_run
        logger.info(
            f"Executing {cmd_to_run} with {env_vars} via ray job submit")
        proc = subprocess.Popen(
            " ".join(["ray", "job", "submit",
                      shlex.quote(full_cmd)]),
            shell=True,
            stdout=sys.stdout,
            stderr=sys.stderr,
            env=env)
        self.subprocess_pool[command_id] = proc
        self.start_time[command_id] = time.time()
        return command_id

    def wait_command(self, command_id: int):
        retcode = self.subprocess_pool[command_id].wait()
        duration = time.time() - self.start_time[command_id]
        return retcode, duration


global_command_runner = CommandRunnerHack()


class S3SyncSessionController(SessionController):
    def __init__(self, sdk, result_queue):
        self.sdk = sdk
        self.result_queue = result_queue
        self.s3_client = boto3.client("s3")
        self.bucket = GLOBAL_CONFIG["RELEASE_AWS_BUCKET"]
        super().__init__()

    def _generate_tmp_s3_path(self):
        fn = "".join(random.choice(string.ascii_lowercase) for i in range(10))
        location = f"tmp/{fn}"
        return location

    def pull(self, session_name, source, target):
        remote_upload_to = self._generate_tmp_s3_path()
        # remote source -> s3
        cid = global_command_runner.run_command(
            session_name, (f"pip install -q awscli && aws s3 cp {source} "
                           f"s3://{self.bucket}/{remote_upload_to} "
                           "--acl bucket-owner-full-control"), {})
        global_command_runner.wait_command(cid)

        # s3 -> local target
        self.s3_client.download_file(
            Bucket=self.bucket,
            Key=remote_upload_to,
            Filename=target,
        )

    def _push_local_dir(self, session_name):
        remote_upload_to = self._generate_tmp_s3_path()
        # pack local dir
        _, local_path = tempfile.mkstemp()
        shutil.make_archive(local_path, "gztar", os.getcwd())
        # local source -> s3
        self.s3_client.upload_file(
            Filename=local_path + ".tar.gz",
            Bucket=self.bucket,
            Key=remote_upload_to,
        )
        # s3 -> remote target
        cid = global_command_runner.run_command(
            session_name, ("pip install -q awscli && "
                           f"aws s3 cp s3://{self.bucket}/{remote_upload_to} "
                           f"archive.tar.gz && "
                           "tar xf archive.tar.gz"), {})
        global_command_runner.wait_command(cid)

    def push(self, session_name, source, target):
        if source is None and target is None:
            self._push_local_dir(session_name)
            return

        assert isinstance(source, str)
        assert isinstance(target, str)

        remote_upload_to = self._generate_tmp_s3_path()
        # local source -> s3
        self.s3_client.upload_file(
            Filename=source,
            Bucket=self.bucket,
            Key=remote_upload_to,
        )
        # s3 -> remote target
        cid = global_command_runner.run_command(
            session_name, "pip install -q awscli && "
            f"aws s3 cp s3://{self.bucket}/{remote_upload_to} {target}", {})
        global_command_runner.wait_command(cid)


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


def commit_or_url(commit_or_url: str) -> str:
    if commit_or_url.startswith("http"):
        url = None
        # Directly return the S3 url
        if "s3" in commit_or_url and "amazonaws.com" in commit_or_url:
            url = commit_or_url
        # Resolve the redirects for buildkite artifacts
        # This is needed because otherwise pip won't recognize the file name.
        elif "buildkite.com" in commit_or_url and "artifacts" in commit_or_url:
            url = requests.head(commit_or_url, allow_redirects=True).url
        if url is not None:
            # Extract commit from url so that we can do the
            # commit sanity check later.
            p = re.compile("/([a-f0-9]{40})/")
            m = p.search(url)
            if m is not None:
                os.environ["RAY_COMMIT"] = m.group(1)
            return url

    # Else, assume commit
    os.environ["RAY_COMMIT"] = commit_or_url
    return wheel_url(GLOBAL_CONFIG["RAY_VERSION"], GLOBAL_CONFIG["RAY_BRANCH"],
                     commit_or_url)


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
            os.environ["RAY_COMMIT"] = commit
            logger.info(
                f"Found wheels URL for Ray {version}, branch {branch}: "
                f"{url}")
            break
    return url


def populate_wheels_sanity_check(commit: Optional[str] = None):
    if not commit:
        cmd = ("python -c 'import ray; print("
               "\"No commit sanity check available, but this is the "
               "Ray wheel commit:\", ray.__commit__)'")
    else:
        cmd = (f"python -c 'import ray; "
               f"assert ray.__commit__ == \"{commit}\", ray.__commit__'")
    os.environ["RAY_WHEELS_SANITY_CHECK"] = cmd


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


def maybe_get_alert_for_result(result_dict: Dict[str, Any]) -> Optional[str]:
    # If we get a result dict, check if any alerts should be raised
    from alert import SUITE_TO_FN, default_handle_result

    logger.info("Checking if results are valid...")

    # Copy dict because we modify kwargs here
    handle_result_kwargs = result_dict.copy()
    handle_result_kwargs["created_on"] = None

    test_suite = handle_result_kwargs.get("test_suite", None)

    handle_fn = SUITE_TO_FN.get(test_suite, None)
    if not handle_fn:
        logger.warning(f"No handle for suite {test_suite}")
        alert = default_handle_result(**handle_result_kwargs)
    else:
        alert = handle_fn(**handle_result_kwargs)

    return alert


def report_result(*, test_suite: str, test_name: str, status: str,
                  last_logs: str, results: Dict[Any, Any],
                  artifacts: Dict[Any, Any], category: str, team: str):
    #   session_url: str, commit_url: str,
    #   runtime: float, stable: bool, frequency: str, return_code: int):
    """Report the test result to database."""
    now = datetime.datetime.utcnow()
    rds_data_client = boto3.client("rds-data", region_name="us-west-2")

    schema = GLOBAL_CONFIG["RELEASE_AWS_DB_TABLE"]

    parameters = [{
        "name": "created_on",
        "typeHint": "TIMESTAMP",
        "value": {
            "stringValue": now.strftime("%Y-%m-%d %H:%M:%S")
        },
    }, {
        "name": "test_suite",
        "value": {
            "stringValue": test_suite
        }
    }, {
        "name": "test_name",
        "value": {
            "stringValue": test_name
        }
    }, {
        "name": "status",
        "value": {
            "stringValue": status
        }
    }, {
        "name": "last_logs",
        "value": {
            "stringValue": last_logs
        }
    }, {
        "name": "results",
        "typeHint": "JSON",
        "value": {
            "stringValue": json.dumps(results)
        },
    }, {
        "name": "artifacts",
        "typeHint": "JSON",
        "value": {
            "stringValue": json.dumps(artifacts)
        },
    }, {
        "name": "category",
        "value": {
            "stringValue": category
        }
    }, {
        "name": "team",
        "value": {
            "stringValue": team
        }
    }]
    columns = [param["name"] for param in parameters]
    values = [f":{param['name']}" for param in parameters]
    column_str = ", ".join(columns).strip(", ")
    value_str = ", ".join(values).strip(", ")

    sql = (f"INSERT INTO {schema} " f"({column_str}) " f"VALUES ({value_str})")

    logger.info(f"Query: {sql}")

    # Default boto3 call timeout is 45 seconds.
    retry_delay_s = 64
    MAX_RDS_RETRY = 3
    exponential_backoff_retry(
        lambda: rds_data_client.execute_statement(
            database=GLOBAL_CONFIG["RELEASE_AWS_DB_NAME"],
            parameters=parameters,
            secretArn=GLOBAL_CONFIG["RELEASE_AWS_DB_SECRET_ARN"],
            resourceArn=GLOBAL_CONFIG["RELEASE_AWS_DB_RESOURCE_ARN"],
            schema=schema,
            sql=sql),
        retry_exceptions=rds_data_client.exceptions.StatementTimeoutException,
        initial_retry_delay_s=retry_delay_s,
        max_retries=MAX_RDS_RETRY)
    logger.info("Result has been persisted to the database")


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


def find_cloud_by_name(sdk: AnyscaleSDK, cloud_name: str,
                       _repeat: bool = True) -> Optional[str]:
    cloud_id = None
    logger.info(f"Looking up cloud with name `{cloud_name}`. ")

    paging_token = None
    while not cloud_id:
        result = sdk.search_clouds(
            clouds_query=dict(
                paging=dict(count=50, paging_token=paging_token)))

        paging_token = result.metadata.next_paging_token

        for res in result.results:
            if res.name == cloud_name:
                cloud_id = res.id
                logger.info(
                    f"Found cloud with name `{cloud_name}` as `{cloud_id}`")
                break

        if not paging_token or cloud_id or not len(result.results):
            break

    return cloud_id


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


def run_bash_script(local_dir: str, bash_script: str):
    previous_dir = os.getcwd()

    bash_script_local_dir = os.path.dirname(bash_script)
    file_name = os.path.basename(bash_script)

    full_local_dir = os.path.join(local_dir, bash_script_local_dir)
    os.chdir(full_local_dir)

    subprocess.run("./" + file_name, shell=True, check=True)

    os.chdir(previous_dir)


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
            logger.info(
                f"Link to app config build: "
                f"{_format_link(anyscale_app_config_build_url(build_id))}")
            return build_id

    if last_status == "failed":
        raise AppConfigBuildFailure("App config build failed.")

    if not build_id:
        raise AppConfigBuildFailure("No build found for app config.")

    # Build found but not failed/finished yet
    completed = False
    start_wait = time.time()
    next_report = start_wait + REPORT_S
    logger.info(f"Waiting for build {build_id} to finish...")
    logger.info(f"Track progress here: "
                f"{_format_link(anyscale_app_config_build_url(build_id))}")
    while not completed:
        now = time.time()
        if now > next_report:
            logger.info(f"... still waiting for build {build_id} to finish "
                        f"({int(now - start_wait)} seconds) ...")
            next_report = next_report + REPORT_S

        result = sdk.get_build(build_id)
        build = result.result

        if build.status == "failed":
            raise AppConfigBuildFailure(
                f"App config build failed. Please see "
                f"{anyscale_app_config_build_url(build_id)} for details")

        if build.status == "succeeded":
            logger.info("Build succeeded.")
            return build_id

        completed = build.status not in ["in_progress", "pending"]

        if completed:
            raise AppConfigBuildFailure(
                f"Unknown build status: {build.status}. Please see "
                f"{anyscale_app_config_build_url(build_id)} for details")

        time.sleep(1)

    return build_id


def run_job(cluster_name: str, compute_tpl_name: str, cluster_env_name: str,
            job_name: str, min_workers: str, script: str,
            script_args: List[str], env_vars: Dict[str, str],
            autosuspend: int) -> Tuple[int, str]:
    # Start cluster and job
    address = f"anyscale://{cluster_name}?autosuspend={autosuspend}"
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
        project_id: str,
) -> str:
    # Create session
    logger.info(f"Creating session {session_name}")
    result = sdk.create_session(session_options)
    session_id = result.result.id

    # Trigger session start
    logger.info(f"Starting session {session_name} ({session_id})")
    session_url = anyscale_session_url(
        project_id=GLOBAL_CONFIG["ANYSCALE_PROJECT"], session_id=session_id)
    logger.info(f"Link to session: {_format_link(session_url)}")

    result = sdk.start_session(session_id, start_session_options={})
    sop_id = result.result.id
    completed = result.result.completed

    # Wait for session
    logger.info(f"Waiting for session {session_name}...")
    start_wait = time.time()
    next_report = start_wait + REPORT_S
    while not completed:
        # Sleep 1 sec before next check.
        time.sleep(1)

        session_operation_response = sdk.get_session_operation(
            sop_id, _request_timeout=30)
        session_operation = session_operation_response.result
        completed = session_operation.completed

        try:
            _check_stop(stop_event, "session")
        except SessionTimeoutError as e:
            # Always queue session termination.
            # We can't do this later as we won't return anything here
            # and the session ID will not be set in the control loop
            _cleanup_session(sdk=sdk, session_id=session_id)
            raise e

        now = time.time()
        if now > next_report:
            logger.info(f"... still waiting for session {session_name} "
                        f"({int(now - start_wait)} seconds) ...")
            next_report = next_report + REPORT_S

    result = sdk.get_session(session_id)
    if not result.result.state != "Active":
        raise ReleaseTestInfraError(
            f"Cluster did not come up - most likely the nodes are currently "
            f"not available. Please check the cluster startup logs: "
            f"{anyscale_session_url(project_id, session_id)}")

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
    logger.info(f"Link to session: {_format_link(session_url)}")
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
        # Sleep 1 sec before next check.
        time.sleep(1)

        result = exponential_backoff_retry(
            lambda: sdk.get_session_command(session_command_id=scd_id),
            retry_exceptions=Exception,
            initial_retry_delay_s=10,
            max_retries=3)
        completed = result.result.finished_at

        if state_str == "CMD_RUN":
            _check_stop(stop_event, "command")
        elif state_str == "CMD_PREPARE":
            _check_stop(stop_event, "prepare_command")

        now = time.time()
        if now > next_report:
            logger.info(f"... still waiting for command to finish "
                        f"({int(now - start_wait)} seconds) ...")
            next_report = next_report + REPORT_S

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
    result = exponential_backoff_retry(
        lambda: session_controller.api_client.get_execution_logs_api_v2_session_commands_session_command_id_execution_logs_get(  # noqa: E501
            session_command_id=scd_id,
            start_line=-1 * lines,
            end_line=0),
        retry_exceptions=Exception,
        initial_retry_delay_s=10,
        max_retries=3)

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
        keep_results_dir: bool = False,
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

    get_auth_api_client(
        cli_token=GLOBAL_CONFIG["ANYSCALE_CLI_TOKEN"],
        host=GLOBAL_CONFIG["ANYSCALE_HOST"])
    session_controller = S3SyncSessionController(sdk, result_queue)

    cloud_id = test_config["cluster"].get("cloud_id", None)
    cloud_name = test_config["cluster"].get("cloud_name", None)
    if cloud_id and cloud_name:
        raise RuntimeError(
            f"You can't supply both a `cloud_name` ({cloud_name}) and a "
            f"`cloud_id` ({cloud_id}) in the test cluster configuration. "
            f"Please provide only one.")
    elif cloud_name and not cloud_id:
        cloud_id = find_cloud_by_name(sdk, cloud_name)
        if not cloud_id:
            raise RuntimeError(
                f"Couldn't find cloud with name `{cloud_name}`.")
    else:
        cloud_id = cloud_id or GLOBAL_CONFIG["ANYSCALE_CLOUD_ID"]

    # Overwrite global config so that `_load_config` sets the correct cloud
    GLOBAL_CONFIG["ANYSCALE_CLOUD_ID"] = cloud_id

    cluster_config_rel_path = test_config["cluster"].get(
        "cluster_config", None)
    cluster_config = _load_config(local_dir, cluster_config_rel_path)

    app_config_rel_path = test_config["cluster"].get("app_config", None)
    app_config = _load_config(local_dir, app_config_rel_path)
    # A lot of staging tests share the same app config yaml, except the flags.
    # `app_env_vars` in test config will help this one.
    # Here we extend the env_vars to use the one specified in the test config.
    if test_config.get("app_env_vars") is not None:
        if app_config["env_vars"] is None:
            app_config["env_vars"] = test_config["app_env_vars"]
        else:
            app_config["env_vars"].update(test_config["app_env_vars"])
        logger.info(f"Using app config:\n{app_config}")

    compute_tpl_rel_path = test_config["cluster"].get("compute_template", None)
    compute_tpl = _load_config(local_dir, compute_tpl_rel_path)

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
            try:
                logs = get_command_logs(session_controller, scd_id,
                                        test_config.get("log_lines", 50))
            except Exception as e:
                raise ReleaseTestInfraError(
                    f"Could not fetch command logs: {e}. This is an "
                    f"infrastructure error on the Anyscale side.")
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
        test_uses_ray_connect = test_config["run"].get("use_connect")

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
                    session_options["cloud_id"] = cloud_id
                    session_options["uses_app_config"] = False
                else:
                    logging.info("Starting session with app/compute config")

                    # Find/create compute template
                    compute_tpl_id, compute_tpl_name = \
                        create_or_find_compute_template(
                            sdk, project_id, compute_tpl)

                    url = _format_link(
                        anyscale_compute_tpl_url(compute_tpl_id))

                    logger.info(f"Link to compute template: {url}")

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

                # Start session
                session_id = create_and_wait_for_session(
                    sdk=sdk,
                    stop_event=stop_event,
                    session_name=session_name,
                    session_options=session_options,
                    project_id=project_id,
                )

            prepare_command = test_config["run"].get("prepare")

            # Write test state json
            test_state_file = os.path.join(local_dir, "test_state.json")
            with open(test_state_file, "wt") as f:
                json.dump({
                    "start_time": time.time(),
                    "test_name": test_name
                }, f)

            on_k8s = test_config["cluster"].get("compute_on_k8s")
            if prepare_command or not test_uses_ray_connect:
                if test_uses_ray_connect:
                    logger.info("Found a prepare command, so pushing it "
                                "to the session.")
                # Rsync up
                logger.info("Syncing files to session...")
                session_controller.push(
                    session_name=session_name,
                    source=None,
                    target=None,
                )

                logger.info("Syncing test state to session...")
                session_controller.push(
                    session_name=session_name,
                    source=test_state_file,
                    target=state_json,
                )

                session_url = anyscale_session_url(
                    project_id=GLOBAL_CONFIG["ANYSCALE_PROJECT"],
                    session_id=session_id)
                _check_stop(stop_event, "file_sync")

                # Optionally run preparation command
                if prepare_command:
                    logger.info(
                        f"Running preparation command: {prepare_command}")
                    if on_k8s:
                        cid = global_command_runner.run_command(
                            session_name, prepare_command, env_vars)
                        status_code, _ = global_command_runner.wait_command(
                            cid)
                        if status_code != 0:
                            raise PrepareCommandRuntimeError()
                    else:
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

            if test_uses_ray_connect:
                script_args = test_config["run"].get("args", [])
                if smoke_test:
                    script_args += ["--smoke-test"]
                min_workers = 0
                for node_type in compute_tpl["worker_node_types"]:
                    min_workers += node_type["min_workers"]
                # Build completed, use job timeout
                result_queue.put(State("CMD_RUN", time.time(), None))
                returncode, logs = run_job(
                    cluster_name=session_name,
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

            # Run release test command
            cmd_to_run = test_config["run"]["script"] + " "

            args = test_config["run"].get("args", [])
            if args:
                cmd_to_run += " ".join(args) + " "

            if smoke_test:
                cmd_to_run += " --smoke-test"

            if on_k8s:
                cmd_id = global_command_runner.run_command(
                    session_name, cmd_to_run, env_vars=env_vars)
            else:
                scd_id, result = run_session_command(
                    sdk=sdk,
                    session_id=session_id,
                    cmd_to_run=cmd_to_run,
                    result_queue=result_queue,
                    env_vars=env_vars,
                    state_str="CMD_RUN")

            if not kick_off_only:
                if on_k8s:
                    retcode, runtime = global_command_runner.wait_command(
                        cmd_id)
                    if retcode != 0:
                        raise RuntimeError("Command errored")
                    _process_finished_command(
                        session_controller=session_controller,
                        scd_id="",
                        runtime=runtime,
                        session_url=session_url,
                        commit_url=commit_url)
                else:
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
                runtime = None
                if isinstance(e, CommandTimeoutError):
                    error_type = "timeout"
                    runtime = 0
                    exit_code = ExitCode.COMMAND_TIMEOUT
                elif isinstance(e, PrepareCommandTimeoutError):
                    error_type = "infra_timeout"
                    runtime = None
                    exit_code = ExitCode.PREPARE_TIMEOUT
                elif isinstance(e, FileSyncTimeoutError):
                    error_type = "infra_timeout"
                    runtime = None
                    exit_code = ExitCode.FILESYNC_TIMEOUT
                elif isinstance(e, SessionTimeoutError):
                    error_type = "infra_timeout"
                    runtime = None
                    exit_code = ExitCode.SESSION_TIMEOUT
                elif isinstance(e, PrepareCommandRuntimeError):
                    error_type = "infra_timeout"
                    runtime = None
                    exit_code = ExitCode.PREPARE_ERROR
                elif isinstance(e, AppConfigBuildFailure):
                    error_type = "infra_timeout"
                    runtime = None
                    exit_code = ExitCode.APPCONFIG_BUILD_ERROR
                elif isinstance(e, ReleaseTestInfraError):
                    error_type = "infra_error"
                    exit_code = ExitCode.INFRA_ERROR
                elif isinstance(e, RuntimeError):
                    error_type = "runtime_error"
                    runtime = 0
                    exit_code = ExitCode.RUNTIME_ERROR
                else:
                    error_type = "unknown timeout"
                    runtime = None
                    exit_code = ExitCode.UNKNOWN

                # Add these metadata here to avoid changing SQL schema.
                results = {}
                results["_runtime"] = runtime
                results["_session_url"] = session_url
                results["_commit_url"] = commit_url
                results["_stable"] = test_config.get("stable", True)
                result_queue.put(
                    State(
                        "END", time.time(), {
                            "status": error_type,
                            "last_logs": logs,
                            "results": results,
                            "exit_code": exit_code.value
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
    prepare_timeout = test_config["run"].get("prepare_timeout", timeout)

    project_url = anyscale_project_url(
        project_id=GLOBAL_CONFIG["ANYSCALE_PROJECT"])
    logger.info(f"Link to project: {_format_link(project_url)}")

    msg = f"This will now run test {test_name}."
    if smoke_test:
        msg += " This is a smoke test."
    if is_long_running:
        msg += " This is a long running test."
    logger.info(msg)

    logger.info(f"Starting process with timeout {timeout} "
                f"(prepare timeout {prepare_timeout}, "
                f"build timeout {build_timeout})")
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
            timeout_time = state.timestamp + prepare_timeout

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

    if not keep_results_dir:
        logger.info(f"Removing results dir {temp_dir}")
        shutil.rmtree(temp_dir)
    else:
        # Write results.json
        with open(os.path.join(temp_dir, "results.json"), "wt") as fp:
            json.dump(result, fp)

        out_dir = os.path.expanduser(GLOBAL_CONFIG["RELEASE_RESULTS_DIR"])

        logger.info(f"Moving results dir {temp_dir} to persistent location "
                    f"{out_dir}")

        try:
            shutil.rmtree(out_dir, ignore_errors=True)
            shutil.copytree(temp_dir, out_dir)
            logger.info(f"Dir contents: {os.listdir(out_dir)}")
        except Exception as e:
            logger.error(
                f"Ran into error when copying results dir to persistent "
                f"location: {str(e)}")

    return result


def run_test(test_config_file: str,
             test_name: str,
             project_id: str,
             commit_url: str,
             category: str = "unspecified",
             smoke_test: bool = False,
             no_terminate: bool = False,
             kick_off_only: bool = False,
             check_progress: bool = False,
             report: bool = True,
             keep_results_dir: bool = False,
             session_name: Optional[str] = None,
             app_config_id_override=None) -> Dict[str, Any]:
    with open(test_config_file, "rt") as f:
        test_configs = yaml.safe_load(f)

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

    # Perform necessary driver side setup.
    driver_setup_script = test_config.get("driver_setup", None)
    if driver_setup_script:
        run_bash_script(local_dir, driver_setup_script)
    logger.info(test_config)
    team = test_config.get("team", "unspecified").strip(" ").lower()
    # When running local test, this validates the team name.
    # If the team name is not specified, they will be recorded as "unspecified"
    if not report and team not in VALID_TEAMS:
        logger.warning(
            f"Incorrect team name {team} has given."
            "Please specify team under the name field in the test config. "
            "For example, within nightly_tests.yaml,\n"
            "\tname: test_xxx\n"
            f"\tteam: {'|'.join(VALID_TEAMS)}\n"
            "\tcluster:...")

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
        keep_results_dir=keep_results_dir,
        app_config_id_override=app_config_id_override)

    status = result.get("status", "invalid")

    if kick_off_only:
        if status != "kickoff":
            raise RuntimeError("Error kicking off test.")

        logger.info("Kicked off test. It's now up to the `--check` "
                    "part of the script to track its process.")
        return {}
    else:
        # `--check` or no kick off only

        if status == "nosession":
            logger.info(f"No running session found for test {test_name}, so "
                        f"assuming everything is fine.")
            return {}

        if status == "kickoff":
            logger.info(f"Test {test_name} is still running.")
            return {}

        last_logs = result.get("last_logs", "No logs.")

        test_suite = os.path.basename(test_config_file).replace(".yaml", "")

        report_kwargs = dict(
            test_suite=test_suite,
            test_name=test_name,
            status=status,
            last_logs=last_logs,
            results=result.get("results", {}),
            artifacts=result.get("artifacts", {}),
            category=category,
            team=team)

        if not has_errored(result):
            # Check if result are met if test succeeded
            alert = maybe_get_alert_for_result(report_kwargs)

            if alert:
                # If we get an alert, the test failed.
                logger.error(f"Alert has been raised for "
                             f"{test_suite}/{test_name} "
                             f"({category}): {alert}")
                result["status"] = "error (alert raised)"
                report_kwargs["status"] = "error (alert raised)"

                # For printing/reporting to the database
                report_kwargs["last_logs"] = alert
                last_logs = alert
            else:
                logger.info(f"No alert raised for test "
                            f"{test_suite}/{test_name} "
                            f"({category}) - the test successfully passed!")

        if report:
            try:
                report_result(**report_kwargs)
            except Exception as e:
                # On database error the test should still pass
                # Todo: flag somewhere else?
                logger.exception(f"Error persisting results to database: {e}")
        else:
            logger.info(f"Usually I would now report the following results:\n"
                        f"{report_kwargs}")

        if has_errored(result):
            # If the script terminates due to an uncaught error, it
            # will return exit code 1, so we use 2 per default to
            # catch these cases.
            exit_code = result.get("exit_code", ExitCode.UNSPECIFIED.value)
            logger.error(last_logs)
            logger.info(f"Exiting with exit code {exit_code}")
            sys.exit(exit_code)

        return report_kwargs


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
        "--report",
        action="store_true",
        default=False,
        help="Whether to report results and upload to S3")
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
        "--keep-results-dir",
        action="store_true",
        default=False,
        help="Keep results in directory (named RELEASE_RESULTS_DIR), e.g. "
        "for Buildkite artifact upload.")
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

    ray_wheels = args.ray_wheels or os.environ.get("RAY_WHEELS", "")

    maybe_fetch_api_token()
    if ray_wheels:
        logger.info(f"Using Ray wheels provided from URL/commit: "
                    f"{ray_wheels}")
        url = commit_or_url(str(ray_wheels))
        logger.info(f"Resolved url link is: {url}")
        # Overwrite with actual URL
        os.environ["RAY_WHEELS"] = url
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

    # RAY_COMMIT is set by commit_or_url and find_ray_wheels
    populate_wheels_sanity_check(os.environ.get("RAY_COMMIT", ""))

    test_config_file = os.path.abspath(os.path.expanduser(args.test_config))

    # Override it from the global variable.
    report = GLOBAL_CONFIG["REPORT_RESULT"]
    if report.lower() == "1" or report.lower() == "true":
        report = True
    else:
        report = args.report

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
        report=report,
        session_name=args.session_name,
        keep_results_dir=args.keep_results_dir,
        app_config_id_override=args.app_config_id_override,
    )
