import asyncio
import json
import logging
import os.path
import shutil
import subprocess
import sys
import uuid
import pathlib
import traceback
from abc import abstractmethod
from urllib.parse import urlparse

import async_timeout
import ray.new_dashboard.utils as dashboard_utils
from ray.new_dashboard.utils import create_task
from ray.new_dashboard.modules.job import job_consts, md5sum
from ray.core.generated import job_agent_pb2
from ray.core.generated import job_agent_pb2_grpc
from ray.core.generated import agent_manager_pb2
from ray._private.services import RAY_HOME, get_ray_jars_dir
from ray.utils import hex_to_binary, binary_to_hex

logger = logging.getLogger(__name__)

COMMON_JVM_OPTIONS = [
    "-Xloggc:${RAY_LOG_DIR}/jvm_gc_%p.log",
    "-XX:+HeapDumpOnOutOfMemoryError",
    "-XX:HeapDumpPath=${RAY_TEMP_DIR}",
    "-XX:ErrorFile=${RAY_LOG_DIR}/hs_err_pid%p.log",
    "-XX:+CrashOnOutOfMemoryError",
    "-ea",
    "-server",
    "-XX:+UseCMSInitiatingOccupancyOnly",
    "-XX:CMSInitiatingOccupancyFraction=68",
    "-XX:+UseConcMarkSweepGC",
    "-XX:+UseParNewGC",
    "-XX:CMSFullGCsBeforeCompaction=5",
    "-XX:+UseCMSCompactAtFullCollection",
    # Reference counting requires System.gc()
    # "-XX:+DisableExplicitGC",
    "-verbose:gc",
    "-XX:+PrintGCDetails",
    "-XX:+PrintGCDateStamps",
    "-XX:-OmitStackTraceInFastThrow",
]


class JobFatalError(Exception):
    pass


class JobInfo:
    def __init__(self, job_id, job_info, temp_dir, log_dir):
        self._job_id = job_id
        self._job_info = job_info
        self._temp_dir = temp_dir
        self._log_dir = log_dir
        self._driver = None
        self._initialize_task = None

    def temp_dir(self):
        return self._temp_dir

    def log_dir(self):
        return self._log_dir

    def language(self):
        return self._job_info["language"]

    def supported_languages(self):
        return self._job_info.get("supportedLanguages", ["PYTHON", "JAVA"])

    def url(self):
        return self._job_info["url"]

    def job_id(self):
        return self._job_id

    def driver_entry(self):
        return self._job_info["driverEntry"]

    def driver_args(self):
        return self._job_info["driverArgs"]

    def initialize_env_timeout_seconds(self):
        timeout = self._job_info.get("initializeEnvTimeoutSeconds",
                                     job_consts.INITIALIZE_ENV_TIMEOUT_SECONDS)
        timeout = min(timeout, job_consts.INITIALIZE_ENV_TIMEOUT_SECONDS_LIMIT)
        timeout = max(timeout, 1)
        return timeout

    def is_pip_check(self):
        return self._job_info.get("pipCheck", True)

    def java_dependency_list(self):
        dependencies = self._job_info.get("dependencies", {}).get("java", [])
        if not dependencies:
            return []
        return [dashboard_utils.Bunch(d) for d in dependencies]

    def python_requirements_file(self):
        requirements = self._job_info.get("dependencies", {}).get("python", [])
        if not requirements:
            return None
        filename = job_consts.PYTHON_REQUIREMENTS_FILE.format(
            temp_dir=self.temp_dir(), job_id=self.job_id())
        with open(filename, "w") as fp:
            fp.writelines(r.strip() + os.linesep for r in requirements)
        return filename

    def set_driver(self, driver):
        self._driver = driver

    def driver(self):
        return self._driver

    def set_initialize_task(self, task):
        self._initialize_task = task

    def initialize_task(self):
        return self._initialize_task

    def env(self):
        env_dict = {}
        # Support json values for env.
        for k, v in self._job_info.get("env", {}).items():
            if isinstance(v, str):
                env_dict[k] = v
            else:
                env_dict[k] = json.dumps(v)
        return env_dict

    def num_java_workers_per_process(self):
        return self._job_info.get("numJavaWorkersPerProcess")

    def jvm_options(self):
        options = self._job_info.get("jvmOptions", [])
        assert isinstance(options, list)
        return options

    def mark_environ_ready(self):
        mark_file = job_consts.JOB_MARK_ENVIRON_READY_FILE.format(
            temp_dir=self._temp_dir, job_id=self._job_id)
        pathlib.Path(mark_file).touch()

    def is_environ_ready(self):
        mark_file = job_consts.JOB_MARK_ENVIRON_READY_FILE.format(
            temp_dir=self._temp_dir, job_id=self._job_id)
        return pathlib.Path(mark_file).exists()


class JobProcessor:
    def __init__(self, job_info):
        assert isinstance(job_info, JobInfo)
        self._job_info = job_info
        self._running_proc = []

    async def clean(self):
        running_proc = [p for p in self._running_proc if p.returncode is None]
        if running_proc:
            logger.info("[%s] Clean running proc of %s: %s",
                        self._job_info.job_id(),
                        type(self).__name__,
                        ", ".join(f"cmd[{p.cmd_index}]" for p in running_proc))
        for proc in running_proc:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            await proc.wait()
            assert proc.returncode is not None

    async def _download_package(self, http_session, url, filename):
        job_id = self._job_info.job_id()
        cmd_index = self._get_next_cmd_index()
        logger.info("[%s] Start download[%s] %s to %s", job_id, cmd_index, url,
                    filename)
        async with http_session.get(url) as response:
            with open(filename, "wb") as f:
                while True:
                    chunk = await response.content.read(
                        job_consts.DOWNLOAD_RESOURCE_BUFFER_SIZE)
                    if not chunk:
                        break
                    f.write(chunk)
            logger.info("[%s] Finished download[%s] %s to %s", job_id,
                        cmd_index, url, filename)

    async def _unzip_package(self, filename, path):
        code = f"import shutil; " \
               f"shutil.unpack_archive({repr(filename)}, {repr(path)})"
        unzip_cmd = [self._get_current_python(), "-c", code]
        await self._check_call_cmd(unzip_cmd)

    @staticmethod
    def _get_next_cmd_index(start_index=[1]):
        cmd_index = start_index[0]
        start_index[0] += 1
        return cmd_index

    async def _check_call_cmd(self, cmd):
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)
        self._running_proc.append(proc)
        job_id = self._job_info.job_id()
        cmd_index = self._get_next_cmd_index()
        proc.cmd_index = cmd_index
        logger.info("[%s] Run cmd[%s] %s", job_id, cmd_index, repr(cmd))
        stdout, stderr = await proc.communicate()
        if stdout:
            logger.info("[%s] Output of cmd[%s]: %s", job_id, cmd_index,
                        stdout.decode("utf-8"))
        if proc.returncode != 0:
            if stderr:
                stderr = stderr.decode("utf-8")
                logger.error("[%s] Output of cmd[%s]: %s", job_id, cmd_index,
                             stderr)
                raise Exception(
                    f"Run command {repr(cmd)} exit with {proc.returncode}:\n"
                    f"{stderr}")
            raise Exception(
                f"Run command {repr(cmd)} exit with {proc.returncode}.")

    async def _check_output_cmd(self, cmd):
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)
        self._running_proc.append(proc)
        job_id = self._job_info.job_id()
        cmd_index = self._get_next_cmd_index()
        proc.cmd_index = cmd_index
        logger.info("[%s] Run cmd[%s] %s", job_id, cmd_index, repr(cmd))
        stdout, stderr = await proc.communicate()
        if stdout:
            stdout = stdout.decode("utf-8")
            logger.info("[%s] Output of cmd[%s]: %s", job_id, cmd_index,
                        stdout)
        if stderr:
            stderr = stderr.decode("utf-8")
            if proc.returncode:
                logger.error("[%s] Output of cmd[%s]: %s", job_id, cmd_index,
                             stderr)
                raise subprocess.CalledProcessError(
                    proc.returncode, cmd, output=stdout, stderr=stderr)
        return stdout

    async def _start_driver(self, cmd, stdout, stderr, env):
        job_id = self._job_info.job_id()
        job_package_dir = job_consts.DOWNLOAD_PACKAGE_UNZIP_DIR.format(
            temp_dir=self._job_info.temp_dir(), job_id=job_id)
        cmd_str = subprocess.list2cmdline(cmd)
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=stdout,
            stderr=stderr,
            env={
                **os.environ,
                **env,
                "CMDLINE": cmd_str,
                "RAY_JOB_DIR": job_package_dir,
            },
            cwd=job_package_dir,
        )
        proc.cmdline = cmd_str
        logger.info("[%s] Start driver cmd %s with pid %s", job_id,
                    repr(cmd_str), proc.pid)
        return proc

    @staticmethod
    def _get_current_python():
        return sys.executable

    @staticmethod
    def _get_virtualenv_python(virtualenv_path):
        return os.path.join(virtualenv_path, "bin/python")

    @staticmethod
    def _is_in_virtualenv():
        return (hasattr(sys, "real_prefix")
                or (hasattr(sys, "base_prefix")
                    and sys.base_prefix != sys.prefix))

    @staticmethod
    def _new_log_files(log_dir, filename):
        if log_dir is None:
            return None, None
        stdout = open(
            os.path.join(log_dir, filename + ".out"), "a", buffering=1)
        stderr = open(
            os.path.join(log_dir, filename + ".err"), "a", buffering=1)
        return stdout, stderr

    def _force_hardlink(self, src, dst):
        logger.info("[%s] Link %s to %s", self._job_info.job_id(), src, dst)
        try:
            os.remove(dst)
        except Exception:
            pass
        os.link(src, dst)

    @abstractmethod
    async def run(self):
        pass


class DownloadPackage(JobProcessor):
    def __init__(self, job_info, http_session):
        super().__init__(job_info)
        self._http_session = http_session

    async def run(self):
        url = self._job_info.url()
        temp_dir = self._job_info.temp_dir()
        job_id = self._job_info.job_id()
        filename = job_consts.DOWNLOAD_PACKAGE.format(
            temp_dir=temp_dir, job_id=job_id)
        unzip_dir = job_consts.DOWNLOAD_PACKAGE_UNZIP_DIR.format(
            temp_dir=temp_dir, job_id=job_id)
        await self._download_package(self._http_session, url, filename)
        await self._unzip_package(filename, unzip_dir)


class PreparePythonEnviron(JobProcessor):
    async def _create_virtualenv(self, path, app_data):
        shutil.rmtree(path, ignore_errors=True)
        python = self._get_current_python()
        if self._is_in_virtualenv():
            try:
                import clonevirtualenv
            except ImportError:
                raise Exception(
                    "We can't create a virtualenv from virtualenv, "
                    "please `pip install virtualenv-clone` then try again.")
            python_dir = os.path.abspath(
                os.path.join(os.path.dirname(python), ".."))
            clonevirtualenv = clonevirtualenv.__file__
            create_venv_cmd = [python, clonevirtualenv, python_dir, path]
        else:
            create_venv_cmd = [
                python, "-m", "virtualenv", "--app-data", app_data,
                "--reset-app-data", "--system-site-packages", "--no-download",
                path
            ]
        await self._check_call_cmd(create_venv_cmd)

    async def _install_python_requirements(self, path, requirements_file):
        python = self._get_virtualenv_python(path)
        pypi = []
        if job_consts.PYTHON_PACKAGE_INDEX:
            pypi = ["-i", job_consts.PYTHON_PACKAGE_INDEX]
        pip_cache_dir = job_consts.PYTHON_PIP_CACHE.format(
            temp_dir=self._job_info.temp_dir())
        pip_download_cmd = [
            python, "-m", "pip", "download", "--destination-directory",
            pip_cache_dir, "-r", requirements_file
        ] + pypi
        pip_install_cmd = [
            python, "-m", "pip", "install", "--no-index", "--find-links",
            pip_cache_dir, "-r", requirements_file
        ]
        await self._check_call_cmd(pip_download_cmd)
        await self._check_call_cmd(pip_install_cmd)

    async def _ray_mark_internal(self, path):
        python = self._get_virtualenv_python(path)
        output = await self._check_output_cmd([
            python, "-c", "import ray; print(ray.__version__, ray.__path__[0])"
        ])
        ray_version, ray_path = [s.strip() for s in output.split()]
        pathlib.Path(os.path.join(ray_path, ".internal_ray")).touch()
        return ray_version, ray_path

    async def _check_ray_is_internal(self, path, ray_version, ray_path):
        python = self._get_virtualenv_python(path)
        output = await self._check_output_cmd([
            python, "-c", "import ray; print(ray.__version__, ray.__path__[0])"
        ])
        actual_ray_version, actual_ray_path = [
            s.strip() for s in output.split()
        ]
        is_exists = pathlib.Path(
            os.path.join(actual_ray_path, ".internal_ray")).exists()
        if not is_exists:
            raise JobFatalError("Change ray version is not allowed: \n"
                                f"  current version: {actual_ray_version}, "
                                f"current path: {actual_ray_path}\n"
                                f"  expect version: {ray_version}, "
                                f"expect path: {ray_path}")

    async def _pip_check(self, path):
        job_id = self._job_info.job_id()
        if not self._job_info.is_pip_check():
            logger.info("[%s] Skip pip check on %s", job_id, path)
            return
        python = self._get_virtualenv_python(path)
        output = await self._check_output_cmd([python, "-m", "pip", "check"])
        output = output.strip()
        if "no broken" not in output.lower():
            raise JobFatalError(f"pip check on {path} failed:\n{output}")
        else:
            logger.info("[%s] pip check on %s success.", job_id, path)

    async def run(self):
        temp_dir = self._job_info.temp_dir()
        job_id = self._job_info.job_id()
        requirements_file = self._job_info.python_requirements_file()
        virtualenv_path = job_consts.PYTHON_VIRTUAL_ENV_DIR.format(
            temp_dir=temp_dir, job_id=job_id)
        # Create the virtualenv cache dir per job to avoid cache data broken.
        virtualenv_cache = job_consts.PYTHON_VIRTUAL_ENV_CACHE_DIR.format(
            temp_dir=temp_dir, job_id=job_id)
        await self._create_virtualenv(virtualenv_path, virtualenv_cache)
        if requirements_file:
            ray_version, ray_path = await self._ray_mark_internal(
                virtualenv_path)
            await self._install_python_requirements(virtualenv_path,
                                                    requirements_file)
            await self._check_ray_is_internal(virtualenv_path, ray_version,
                                              ray_path)
            await self._pip_check(virtualenv_path)


class StartPythonDriver(JobProcessor):
    _template = """import sys
sys.path.append({import_path})
import ray
from ray.utils import hex_to_binary
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

    def _gen_driver_code(self, python_executable):
        temp_dir = self._job_info.temp_dir()
        job_id = self._job_info.job_id()
        job_package_dir = job_consts.DOWNLOAD_PACKAGE_UNZIP_DIR.format(
            temp_dir=temp_dir, job_id=job_id)
        driver_entry_file = job_consts.JOB_DRIVER_ENTRY_FILE.format(
            temp_dir=temp_dir, job_id=job_id, uuid=uuid.uuid4())
        ip, port = self._redis_address

        # Per job config
        job_config_items = {
            "worker_env": self._job_info.env(),
            "worker_cwd": job_package_dir,
            "num_java_workers_per_process": self._job_info.
            num_java_workers_per_process(),
            "jvm_options": COMMON_JVM_OPTIONS + self._job_info.jvm_options(),
            "code_search_path": [job_package_dir],
            "python_worker_executable": python_executable,
        }

        # User may set the config in ray.conf, in this case,
        # the value will be None.
        job_config_items = {
            k: v
            for k, v in job_config_items.items() if v is not None
        }

        job_config_args = ", ".join(
            f"{key}={repr(value)}" for key, value in job_config_items.items())

        if isinstance(self._job_info.driver_args(), str):
            driver_args = repr(self._job_info.driver_args())
        else:
            assert isinstance(self._job_info.driver_args(), list),\
                "driver_args should be str or list instead of " \
                f"{type(self._job_info.driver_args())}"
            driver_args = ", ".join(
                [repr(x) for x in self._job_info.driver_args()])
        driver_code = self._template.format(
            job_id=repr(hex_to_binary(job_id)),
            job_config_args=job_config_args,
            import_path=repr(job_package_dir),
            redis_address=repr(ip + ":" + str(port)),
            redis_password=repr(self._redis_password),
            driver_entry=self._job_info.driver_entry(),
            driver_args=driver_args)
        with open(driver_entry_file, "w") as fp:
            fp.write(driver_code)
        return driver_entry_file

    async def run(self):
        temp_dir = self._job_info.temp_dir()
        log_dir = self._job_info.log_dir()
        job_id = self._job_info.job_id()
        virtualenv_path = job_consts.PYTHON_VIRTUAL_ENV_DIR.format(
            temp_dir=temp_dir, job_id=job_id)
        python = self._get_virtualenv_python(virtualenv_path)
        driver_file = self._gen_driver_code(python)
        driver_cmd = [python, "-u", driver_file]
        stdout_file, stderr_file = self._new_log_files(log_dir,
                                                       f"driver-{job_id}")
        job_package_dir = job_consts.DOWNLOAD_PACKAGE_UNZIP_DIR.format(
            temp_dir=temp_dir, job_id=job_id)
        env = dict(self._job_info.env(), RAY_JOB_DIR=job_package_dir)
        return await self._start_driver(driver_cmd, stdout_file, stderr_file,
                                        env)


class PrepareJavaEnviron(JobProcessor):
    def __init__(self, job_info, http_session):
        super().__init__(job_info)
        self._http_session = http_session

    async def run(self):
        temp_dir = self._job_info.temp_dir()
        job_id = self._job_info.job_id()
        job_package_dir = job_consts.DOWNLOAD_PACKAGE_UNZIP_DIR.format(
            temp_dir=temp_dir, job_id=job_id)
        os.makedirs(job_package_dir, exist_ok=True)
        java_shared_library_dir = job_consts.JAVA_SHARED_LIBRARY_DIR.format(
            temp_dir=temp_dir)
        os.makedirs(java_shared_library_dir, exist_ok=True)
        python = self._get_current_python()
        dependencies = self._job_info.java_dependency_list()
        for d in dependencies:
            url_path = urlparse(d.url).path
            basename_from_url = os.path.basename(url_path)
            basename_with_md5 = basename_from_url + "." + d.md5.lower()
            # The full path of file in java shared library dir.
            cached_filename = os.path.join(java_shared_library_dir,
                                           basename_with_md5)
            # The full path of file in job unzip dir.
            download_filename = os.path.join(job_package_dir,
                                             basename_from_url)
            # Download jar and cache to shared dir.
            if not os.path.exists(cached_filename):
                logger.info("[%s] Cache miss: %s", job_id, cached_filename)
                await self._download_package(self._http_session, d.url,
                                             download_filename)
                if d.md5:
                    cmd = [python, md5sum.__file__, download_filename]
                    output = await self._check_output_cmd(cmd)
                    calc_md5 = output.split()[0]
                    if calc_md5.lower() != d.md5.lower():
                        raise Exception(f"Downloaded file {download_filename} "
                                        f"is corrupted: "
                                        f"{calc_md5} != {d.md5}(expected)")
                    self._force_hardlink(download_filename, cached_filename)
                else:
                    logger.info(
                        "[%s] MD5 of %s is empty, skip verifying and caching.",
                        job_id, d.url)
            else:
                logger.info("[%s] Cache hit: %s.", job_id, cached_filename)
                self._force_hardlink(cached_filename, download_filename)


class StartJavaDriver(JobProcessor):
    def __init__(self, job_info, redis_address, redis_password, node_ip,
                 log_dir):
        super().__init__(job_info)
        self._redis_address = redis_address
        self._redis_password = redis_password
        self._node_ip = node_ip
        self._log_dir = log_dir

    def _build_java_worker_command(self):
        """This method assembles the command used to start a Java worker.

        Returns:
            The command string for starting Java worker.
        """
        temp_dir = self._job_info.temp_dir()
        job_id = self._job_info.job_id()
        job_package_dir = job_consts.DOWNLOAD_PACKAGE_UNZIP_DIR.format(
            temp_dir=temp_dir, job_id=job_id)

        pairs = []
        ip, port = self._redis_address
        redis_address = ip + ":" + str(port)
        if redis_address is not None:
            pairs.append(("ray.address", redis_address))

        if self._redis_password is not None:
            pairs.append(("ray.redis.password", self._redis_password))
        else:
            pairs.append(("ray.redis.password", ""))

        if self._node_ip is not None:
            pairs.append(("ray.node-ip", self._node_ip))

        pairs.append(("ray.home", RAY_HOME))
        pairs.append(("ray.job.id", self._job_info.job_id()))
        pairs.append(("ray.run-mode", "CLUSTER"))
        pairs.append(("ray.logging.dir", self._log_dir))

        # Per job config
        env = dict(self._job_info.env(), RAY_JOB_DIR=job_package_dir)
        for key, value in env.items():
            pairs.append(("ray.job.worker-env." + key, value))
        pairs.append(("ray.job.worker-cwd", job_package_dir))
        if self._job_info.num_java_workers_per_process() is not None:
            pairs.append(("ray.job.num-java-workers-per-process",
                          self._job_info.num_java_workers_per_process()))
        for i, jvm_option in enumerate(COMMON_JVM_OPTIONS +
                                       self._job_info.jvm_options()):
            pairs.append(("ray.job.jvm-options." + str(i), jvm_option))

        pairs.append(("ray.job.code-search-path", job_package_dir))
        command = ["java"] + COMMON_JVM_OPTIONS + [
            "-D{}={}".format(*pair) for pair in pairs
        ]

        # Add ray jars path to java classpath
        ray_jars = ":".join([
            os.path.join(get_ray_jars_dir(), "*"),
            os.path.join(job_package_dir, "*"),
        ])
        options = self._job_info.jvm_options()
        cp_index = -1
        for i in range(len(options)):
            option = options[i]
            if option == "-cp" or option == "-classpath":
                cp_index = i + 1
                break
        if cp_index != -1:
            options[cp_index] = options[cp_index] + os.pathsep + ray_jars
        else:
            options = ["-cp", ray_jars] + options
        # Put `jvm_options` in the last, so it can overwrite the
        # above options.
        command += options

        command += ["io.ray.runtime.runner.worker.DefaultDriver"]
        command += [self._job_info.driver_entry()]
        command += self._job_info.driver_args()

        return command

    async def run(self):
        driver_cmd = self._build_java_worker_command()
        log_dir = self._job_info.log_dir()
        job_id = self._job_info.job_id()
        stdout_file, stderr_file = self._new_log_files(log_dir,
                                                       f"driver-{job_id}")
        return await self._start_driver(driver_cmd, stdout_file, stderr_file,
                                        self._job_info.env())


class JobAgent(dashboard_utils.DashboardAgentModule,
               job_agent_pb2_grpc.JobAgentServiceServicer):
    def __init__(self, dashboard_agent):
        super().__init__(dashboard_agent)
        self._job_table = {}

    async def _prepare_job_environ(self, job_info):
        job_id = job_info.job_id()
        last_ex = None
        os.makedirs(
            job_consts.JOB_DIR.format(
                temp_dir=job_info.temp_dir(), job_id=job_id),
            exist_ok=True)
        http_session = self._dashboard_agent.http_session
        for i in range(job_consts.JOB_RETRY_TIMES):
            processors = []
            concurrent_tasks = []

            async def _clean_all_concurrent_tasks():
                logger.info("[%s] Clean all concurrent tasks.", job_id)
                for task in concurrent_tasks:
                    task.cancel()
                await asyncio.gather(*concurrent_tasks, return_exceptions=True)
                for p in processors:
                    await p.clean()

            try:
                with async_timeout.timeout(
                        job_info.initialize_env_timeout_seconds()):
                    language_to_processor = {
                        "PYTHON": PreparePythonEnviron(job_info),
                        "JAVA": PrepareJavaEnviron(job_info, http_session),
                    }
                    processors.append(DownloadPackage(job_info, http_session))
                    for language in set([job_info.language()] +
                                        job_info.supported_languages()):
                        processors.append(language_to_processor[language])
                    concurrent_tasks = [
                        create_task(p.run()) for p in processors
                    ]
                    await asyncio.gather(*concurrent_tasks)
                break
            except (JobFatalError, asyncio.CancelledError) as ex:
                await _clean_all_concurrent_tasks()
                raise ex
            except Exception as ex:
                logger.exception(ex)
                last_ex = ex
                await _clean_all_concurrent_tasks()
                logger.info("[%s] Retry to prepare job environment [%s/%s].",
                            job_id, i + 1, job_consts.JOB_RETRY_TIMES)
                await asyncio.sleep(job_consts.JOB_RETRY_INTERVAL_SECONDS)
        else:
            logger.error("[%s] Failed to prepare job environment.", job_id)
            raise last_ex
        logger.info("[%s] Finished to prepare job environment.", job_id)

    async def InitializeJobEnv(self, request, context):
        job_id = binary_to_hex(request.job_data.job_id)
        if job_id in self._job_table:
            logger.info("[%s] Job environment is ready, skip initialization.",
                        job_id)
            return job_agent_pb2.InitializeJobEnvReply(
                status=agent_manager_pb2.AGENT_RPC_STATUS_OK)

        try:
            job_info = JobInfo(job_id, json.loads(
                request.job_data.job_payload), self._dashboard_agent.temp_dir,
                               self._dashboard_agent.log_dir)
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
            if job_info.is_environ_ready():
                logger.info("[%s] Job environment is ready.", job_id)
            else:
                await self._prepare_job_environ(job_info)
                job_info.mark_environ_ready()
            if request.start_driver:
                logger.info("[%s] Starting driver.", job_id)
                language = job_info.language()
                if language == "PYTHON":
                    driver = await StartPythonDriver(
                        job_info, self._dashboard_agent.redis_address,
                        self._dashboard_agent.redis_password).run()
                elif language == "JAVA":
                    driver = await StartJavaDriver(
                        job_info, self._dashboard_agent.redis_address,
                        self._dashboard_agent.redis_password,
                        self._dashboard_agent.ip,
                        self._dashboard_agent.log_dir).run()
                else:
                    raise Exception(f"Unsupported language type: {language}")
                job_info.set_driver(driver)
            else:
                logger.info("[%s] Not start driver.", job_id)

        initialize_task = create_task(_initialize_job_env())
        job_info.set_initialize_task(initialize_task)
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
        driver_cmdline = ""
        if request.start_driver:
            driver = job_info.driver()
            if driver:
                driver_pid = driver.pid
                driver_cmdline = driver.cmdline

        logger.info("[%s] Initialize job environment success.", job_id)
        return job_agent_pb2.InitializeJobEnvReply(
            status=agent_manager_pb2.AGENT_RPC_STATUS_OK,
            driver_pid=driver_pid,
            driver_cmdline=driver_cmdline)

    async def CleanJobEnv(self, request, context):
        job_id = binary_to_hex(request.job_id)
        job_info = self._job_table.pop(job_id, None)
        if job_info:
            assert isinstance(job_info, JobInfo)
            logger.info("[%s] Clean job environment found job info.", job_id)

            # Cancel job task.
            cancel_job_task = [
                job_info.initialize_task(),
            ]
            for cancel_task in cancel_job_task:
                try:
                    if cancel_task and not cancel_task.done():
                        logger.info("[%s] Cancelling task %s.", job_id,
                                    cancel_task)
                        cancel_task.cancel()
                        try:
                            await cancel_task
                        except asyncio.CancelledError:
                            pass
                        logger.info("[%s] Task %s has been cancelled.", job_id,
                                    cancel_task)
                except Exception as ex:
                    logger.exception(ex)

            # Kill driver.
            driver = job_info.driver()
            job_info.set_driver(None)
            if driver:
                logger.info("[%s] Killing driver %s.", job_id, driver.pid)
                try:
                    driver.kill()
                except ProcessLookupError:
                    pass
                await driver.wait()
                assert driver.returncode is not None
                logger.info("[%s] Driver %s has been killed.", job_id,
                            driver.pid)
        else:
            logger.info("[%s] Clean job environment not found job info.",
                        job_id)

        job_dir = job_consts.JOB_DIR.format(
            temp_dir=self._dashboard_agent.temp_dir, job_id=job_id)
        logger.info("[%s] Removing job directory %s.", job_id, job_dir)
        shutil.rmtree(job_dir, ignore_errors=True)
        logger.info("[%s] Clean job environment success.", job_id)
        return job_agent_pb2.CleanJobEnvReply(
            status=agent_manager_pb2.AGENT_RPC_STATUS_OK)

    async def run(self, server):
        job_agent_pb2_grpc.add_JobAgentServiceServicer_to_server(self, server)
