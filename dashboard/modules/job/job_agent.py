import asyncio
import json
import logging
import os.path
import itertools
import subprocess
import sys
import secrets
import uuid
import traceback
from abc import abstractmethod
from typing import Union

import attr
from attr.validators import instance_of
import ray.new_dashboard.utils as dashboard_utils
from ray.new_dashboard.utils import create_task
from ray.new_dashboard.modules.job import job_consts
from ray.new_dashboard.modules.job.job_description import JobDescription
from ray.core.generated import job_agent_pb2
from ray.core.generated import job_agent_pb2_grpc
from ray.core.generated import agent_manager_pb2

logger = logging.getLogger(__name__)


@attr.s(kw_only=True, slots=True)
class JobInfo(JobDescription):
    # TODO(fyrestone): We should use job id instead of unique id.
    unique_id = attr.ib(type=str, validator=instance_of(str))
    # The temp directory.
    temp_dir = attr.ib(type=str, validator=instance_of(str))
    # The log directory.
    log_dir = attr.ib(type=str, validator=instance_of(str))
    # The driver process instance.
    driver = attr.ib(
        type=Union[None, asyncio.subprocess.Process], default=None)

    def __attrs_post_init__(self):
        # Support json values for env.
        self.env = {
            k: v if isinstance(v, str) else json.dumps(v)
            for k, v in self.env.items()
        }


class JobProcessor:
    """Wraps the job info and provides common utils to download packages,
    start drivers, etc.

    Args:
        job_info (JobInfo): The job info.
    """
    _cmd_index_gen = itertools.count(1)

    def __init__(self, job_info):
        assert isinstance(job_info, JobInfo)
        self._job_info = job_info

    async def _download_package(self, http_session, url, filename):
        unique_id = self._job_info.unique_id
        cmd_index = next(self._cmd_index_gen)
        logger.info("[%s] Start download[%s] %s to %s", unique_id, cmd_index,
                    url, filename)
        async with http_session.get(url, ssl=False) as response:
            with open(filename, "wb") as f:
                while True:
                    chunk = await response.content.read(
                        job_consts.DOWNLOAD_BUFFER_SIZE)
                    if not chunk:
                        break
                    f.write(chunk)
            logger.info("[%s] Finished download[%s] %s to %s", unique_id,
                        cmd_index, url, filename)

    async def _unpack_package(self, filename, path):
        code = f"import shutil; " \
               f"shutil.unpack_archive({repr(filename)}, {repr(path)})"
        unzip_cmd = [self._get_current_python(), "-c", code]
        await self._check_output_cmd(unzip_cmd)

    async def _check_output_cmd(self, cmd):
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)
        unique_id = self._job_info.unique_id
        cmd_index = next(self._cmd_index_gen)
        proc.cmd_index = cmd_index
        logger.info("[%s] Run cmd[%s] %s", unique_id, cmd_index, repr(cmd))
        stdout, stderr = await proc.communicate()
        stdout = stdout.decode("utf-8")
        logger.info("[%s] Output of cmd[%s]: %s", unique_id, cmd_index, stdout)
        if proc.returncode != 0:
            stderr = stderr.decode("utf-8")
            logger.error("[%s] Error of cmd[%s]: %s", unique_id, cmd_index,
                         stderr)
            raise subprocess.CalledProcessError(
                proc.returncode, cmd, output=stdout, stderr=stderr)
        return stdout

    async def _start_driver(self, cmd, stdout, stderr, env):
        unique_id = self._job_info.unique_id
        job_package_dir = job_consts.JOB_UNPACK_DIR.format(
            temp_dir=self._job_info.temp_dir, unique_id=unique_id)
        cmd_str = subprocess.list2cmdline(cmd)
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=stdout,
            stderr=stderr,
            env={
                **os.environ,
                **env,
            },
            cwd=job_package_dir,
        )
        logger.info("[%s] Start driver cmd %s with pid %s", unique_id,
                    repr(cmd_str), proc.pid)
        return proc

    @staticmethod
    def _get_current_python():
        return sys.executable

    @staticmethod
    def _new_log_files(log_dir, filename):
        if log_dir is None:
            return None, None
        stdout = open(
            os.path.join(log_dir, filename + ".out"), "a", buffering=1)
        stderr = open(
            os.path.join(log_dir, filename + ".err"), "a", buffering=1)
        return stdout, stderr

    @abstractmethod
    async def run(self):
        pass


class DownloadPackage(JobProcessor):
    """ Download the job package.

    Args:
        job_info (JobInfo): The job info.
        http_session (aiohttp.ClientSession): The client session.
    """

    def __init__(self, job_info, http_session):
        super().__init__(job_info)
        self._http_session = http_session

    async def run(self):
        temp_dir = self._job_info.temp_dir
        unique_id = self._job_info.unique_id
        filename = job_consts.DOWNLOAD_PACKAGE_FILE.format(
            temp_dir=temp_dir, unique_id=unique_id)
        unpack_dir = job_consts.JOB_UNPACK_DIR.format(
            temp_dir=temp_dir, unique_id=unique_id)
        url = self._job_info.runtime_env.working_dir
        await self._download_package(self._http_session, url, filename)
        await self._unpack_package(filename, unpack_dir)


class StartPythonDriver(JobProcessor):
    """ Start the driver for Python job.

    Args:
        job_info (JobInfo): The job info.
        redis_address (tuple): The (ip, port) of redis.
        redis_password (str): The password of redis.
    """

    _template = """import sys
sys.path.append({import_path})
import ray
from ray._private.utils import hex_to_binary
ray.init(ignore_reinit_error=True,
         address={redis_address},
         _redis_password={redis_password},
         job_config=ray.job_config.JobConfig({job_config_args}),
)
import {driver_entry}
{driver_entry}.main({driver_args})
# If the driver exits normally, we invoke Ray.shutdown() again
# here, in case the user code forgot to invoke it.
ray.shutdown()
"""

    def __init__(self, job_info, redis_address, redis_password):
        super().__init__(job_info)
        self._redis_address = redis_address
        self._redis_password = redis_password

    def _gen_driver_code(self):
        temp_dir = self._job_info.temp_dir
        unique_id = self._job_info.unique_id
        job_package_dir = job_consts.JOB_UNPACK_DIR.format(
            temp_dir=temp_dir, unique_id=unique_id)
        driver_entry_file = job_consts.JOB_DRIVER_ENTRY_FILE.format(
            temp_dir=temp_dir, unique_id=unique_id, uuid=uuid.uuid4())
        ip, port = self._redis_address

        # Per job config
        job_config_items = {
            "worker_env": self._job_info.env,
            "code_search_path": [job_package_dir],
        }

        job_config_args = ", ".join(f"{key}={repr(value)}"
                                    for key, value in job_config_items.items()
                                    if value is not None)
        driver_args = ", ".join([repr(x) for x in self._job_info.driver_args])
        driver_code = self._template.format(
            job_config_args=job_config_args,
            import_path=repr(job_package_dir),
            redis_address=repr(ip + ":" + str(port)),
            redis_password=repr(self._redis_password),
            driver_entry=self._job_info.driver_entry,
            driver_args=driver_args)
        with open(driver_entry_file, "w") as fp:
            fp.write(driver_code)
        return driver_entry_file

    async def run(self):
        python = self._get_current_python()
        driver_file = self._gen_driver_code()
        driver_cmd = [python, "-u", driver_file]
        stdout_file, stderr_file = self._new_log_files(
            self._job_info.log_dir, f"driver-{self._job_info.unique_id}")
        return await self._start_driver(driver_cmd, stdout_file, stderr_file,
                                        self._job_info.env)


class JobAgent(dashboard_utils.DashboardAgentModule,
               job_agent_pb2_grpc.JobAgentServiceServicer):
    """ The JobAgentService defined in job_agent.proto for initializing /
    cleaning job environments.
    """

    async def InitializeJobEnv(self, request, context):
        # TODO(fyrestone): Handle duplicated InitializeJobEnv requests
        # when initializing job environment.
        # TODO(fyrestone): Support reinitialize job environment.

        # TODO(fyrestone): Use job id instead of unique id.
        unique_id = secrets.token_hex(6)

        # Parse the job description from the request.
        try:
            job_description_data = json.loads(request.job_description)
            job_info = JobInfo(
                unique_id=unique_id,
                temp_dir=self._dashboard_agent.temp_dir,
                log_dir=self._dashboard_agent.log_dir,
                **job_description_data)
        except json.JSONDecodeError as ex:
            error_message = str(ex)
            error_message += f", job_payload:\n{request.job_description}"
            logger.error("[%s] Initialize job environment failed, %s.",
                         unique_id, error_message)
            return job_agent_pb2.InitializeJobEnvReply(
                status=agent_manager_pb2.AGENT_RPC_STATUS_FAILED,
                error_message=error_message)
        except Exception as ex:
            logger.exception(ex)
            return job_agent_pb2.InitializeJobEnvReply(
                status=agent_manager_pb2.AGENT_RPC_STATUS_FAILED,
                error_message=traceback.format_exc())

        async def _initialize_job_env():
            os.makedirs(
                job_consts.JOB_DIR.format(
                    temp_dir=job_info.temp_dir, unique_id=unique_id),
                exist_ok=True)
            # Download the job package.
            await DownloadPackage(job_info,
                                  self._dashboard_agent.http_session).run()
            # Start the driver.
            logger.info("[%s] Starting driver.", unique_id)
            language = job_info.language
            if language == job_consts.PYTHON:
                driver = await StartPythonDriver(
                    job_info, self._dashboard_agent.redis_address,
                    self._dashboard_agent.redis_password).run()
            else:
                raise Exception(f"Unsupported language type: {language}")
            job_info.driver = driver

        initialize_task = create_task(_initialize_job_env())

        try:
            await initialize_task
        except asyncio.CancelledError:
            logger.error("[%s] Initialize job environment has been cancelled.",
                         unique_id)
            return job_agent_pb2.InitializeJobEnvReply(
                status=agent_manager_pb2.AGENT_RPC_STATUS_FAILED,
                error_message="InitializeJobEnv has been cancelled, "
                "did you call CleanJobEnv?")
        except Exception as ex:
            logger.exception(ex)
            return job_agent_pb2.InitializeJobEnvReply(
                status=agent_manager_pb2.AGENT_RPC_STATUS_FAILED,
                error_message=traceback.format_exc())

        driver_pid = 0
        if job_info.driver:
            driver_pid = job_info.driver.pid

        logger.info(
            "[%s] Job environment initialized, "
            "the driver (pid=%s) started.", unique_id, driver_pid)
        return job_agent_pb2.InitializeJobEnvReply(
            status=agent_manager_pb2.AGENT_RPC_STATUS_OK,
            driver_pid=driver_pid)

    async def run(self, server):
        job_agent_pb2_grpc.add_JobAgentServiceServicer_to_server(self, server)
