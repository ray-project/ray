import asyncio
import json
import logging
import os.path
import itertools
import subprocess
import sys
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
from ray._private.utils import hex_to_binary, binary_to_hex

logger = logging.getLogger(__name__)


@attr.s(kw_only=True, slots=True)
class JobInfo(JobDescription):
    # The job id hex string.
    job_id = attr.ib(type=str, validator=instance_of(str))
    # The temp directory.
    temp_dir = attr.ib(type=str, validator=instance_of(str))
    # The log directory.
    log_dir = attr.ib(type=str, validator=instance_of(str))
    # The driver process instance.
    driver = attr.ib(
        type=Union[None, asyncio.subprocess.Process], default=None)

    def __attrs_post_init__(self):
        job_package_dir = job_consts.JOB_UNPACK_DIR.format(
            temp_dir=self.temp_dir, job_id=self.job_id)
        # Support json values for env.
        self.env = {
            k: v if isinstance(v, str) else json.dumps(v)
            for k, v in self.env.items()
        }
        self.env["RAY_JOB_DIR"] = job_package_dir


class JobProcessor:
    _cmd_index_gen = itertools.count(1)

    def __init__(self, job_info):
        assert isinstance(job_info, JobInfo)
        self._job_info = job_info

    async def _download_package(self, http_session, url, filename):
        job_id = self._job_info.job_id
        cmd_index = next(self._cmd_index_gen)
        logger.info("[%s] Start download[%s] %s to %s", job_id, cmd_index, url,
                    filename)
        async with http_session.get(url, ssl=False) as response:
            with open(filename, "wb") as f:
                while True:
                    chunk = await response.content.read(
                        job_consts.DOWNLOAD_BUFFER_SIZE)
                    if not chunk:
                        break
                    f.write(chunk)
            logger.info("[%s] Finished download[%s] %s to %s", job_id,
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
        job_id = self._job_info.job_id
        cmd_index = next(self._cmd_index_gen)
        proc.cmd_index = cmd_index
        logger.info("[%s] Run cmd[%s] %s", job_id, cmd_index, repr(cmd))
        stdout, stderr = await proc.communicate()
        stdout = stdout.decode("utf-8")
        logger.info("[%s] Output of cmd[%s]: %s", job_id, cmd_index, stdout)
        if proc.returncode != 0:
            stderr = stderr.decode("utf-8")
            logger.error("[%s] Output of cmd[%s]: %s", job_id, cmd_index,
                         stderr)
            raise subprocess.CalledProcessError(
                proc.returncode, cmd, output=stdout, stderr=stderr)
        return stdout

    async def _start_driver(self, cmd, stdout, stderr, env):
        job_id = self._job_info.job_id
        job_package_dir = job_consts.JOB_UNPACK_DIR.format(
            temp_dir=self._job_info.temp_dir, job_id=job_id)
        cmd_str = subprocess.list2cmdline(cmd)
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=stdout,
            stderr=stderr,
            env={
                **os.environ,
                **env,
                "RAY_JOB_DIR": job_package_dir,
            },
            cwd=job_package_dir,
        )
        logger.info("[%s] Start driver cmd %s with pid %s", job_id,
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
    def __init__(self, job_info, http_session):
        super().__init__(job_info)
        self._http_session = http_session

    async def run(self):
        temp_dir = self._job_info.temp_dir
        job_id = self._job_info.job_id
        filename = job_consts.DOWNLOAD_PACKAGE_FILE.format(
            temp_dir=temp_dir, job_id=job_id)
        unpack_dir = job_consts.JOB_UNPACK_DIR.format(
            temp_dir=temp_dir, job_id=job_id)
        await self._download_package(self._http_session, self._job_info.url,
                                     filename)
        await self._unpack_package(filename, unpack_dir)


class StartPythonDriver(JobProcessor):
    _template = """import sys
sys.path.append({import_path})
import ray
from ray._private.utils import hex_to_binary
ray.init(ignore_reinit_error=True,
         address={redis_address},
         _redis_password={redis_password},
         job_id=ray.JobID({job_id}),
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
        job_id = self._job_info.job_id
        job_package_dir = job_consts.JOB_UNPACK_DIR.format(
            temp_dir=temp_dir, job_id=job_id)
        driver_entry_file = job_consts.JOB_DRIVER_ENTRY_FILE.format(
            temp_dir=temp_dir, job_id=job_id, uuid=uuid.uuid4())
        ip, port = self._redis_address

        # Per job config
        job_config_items = {
            "worker_env": self._job_info.env,
            "code_search_path": [job_package_dir],
        }

        job_config_args = ", ".join(f"{key}={repr(value)}"
                                    for key, value in job_config_items.items()
                                    if value is not None)
        driver_args = ", ".join([repr(x) for x in self._job_info.driverArgs])
        driver_code = self._template.format(
            job_id=repr(hex_to_binary(job_id)),
            job_config_args=job_config_args,
            import_path=repr(job_package_dir),
            redis_address=repr(ip + ":" + str(port)),
            redis_password=repr(self._redis_password),
            driver_entry=self._job_info.driverEntry,
            driver_args=driver_args)
        with open(driver_entry_file, "w") as fp:
            fp.write(driver_code)
        return driver_entry_file

    async def run(self):
        python = self._get_current_python()
        driver_file = self._gen_driver_code()
        driver_cmd = [python, "-u", driver_file]
        stdout_file, stderr_file = self._new_log_files(
            self._job_info.log_dir, f"driver-{self._job_info.job_id}")
        return await self._start_driver(driver_cmd, stdout_file, stderr_file,
                                        self._job_info.env)


class JobAgent(dashboard_utils.DashboardAgentModule,
               job_agent_pb2_grpc.JobAgentServiceServicer):
    def __init__(self, dashboard_agent):
        super().__init__(dashboard_agent)
        self._job_table = {}

    async def InitializeJobEnv(self, request, context):
        job_id = binary_to_hex(request.job_data.job_id)
        if job_id in self._job_table:
            logger.info("[%s] Job environment is ready, skip initialization.",
                        job_id)
            return job_agent_pb2.InitializeJobEnvReply(
                status=agent_manager_pb2.AGENT_RPC_STATUS_OK)

        try:
            job_description_data = json.loads(request.job_data.job_payload)
            job_info = JobInfo(
                job_id=job_id,
                temp_dir=self._dashboard_agent.temp_dir,
                log_dir=self._dashboard_agent.log_dir,
                **job_description_data)
        except json.JSONDecodeError as ex:
            error_message = str(ex)
            error_message += f", job_payload:\n{request.job_data.job_payload}"
            logger.error("[%s] Initialize job environment failed, %s.", job_id,
                         error_message)
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
                    temp_dir=job_info.temp_dir, job_id=job_id),
                exist_ok=True)
            await DownloadPackage(job_info,
                                  self._dashboard_agent.http_session).run()
            if request.start_driver:
                logger.info("[%s] Starting driver.", job_id)
                language = job_info.language
                if language == job_consts.PYTHON:
                    driver = await StartPythonDriver(
                        job_info, self._dashboard_agent.redis_address,
                        self._dashboard_agent.redis_password).run()
                else:
                    raise Exception(f"Unsupported language type: {language}")
                job_info.driver = driver
            else:
                logger.info("[%s] Not start driver.", job_id)

        initialize_task = create_task(_initialize_job_env())
        self._job_table[job_id] = job_info

        try:
            await initialize_task
        except asyncio.CancelledError:
            logger.error("[%s] Initialize job environment has been cancelled.",
                         job_id)
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
        if request.start_driver:
            if job_info.driver:
                driver_pid = job_info.driver.pid

        logger.info("[%s] Initialize job environment success.", job_id)
        return job_agent_pb2.InitializeJobEnvReply(
            status=agent_manager_pb2.AGENT_RPC_STATUS_OK,
            driver_pid=driver_pid)

    async def run(self, server):
        job_agent_pb2_grpc.add_JobAgentServiceServicer_to_server(self, server)
