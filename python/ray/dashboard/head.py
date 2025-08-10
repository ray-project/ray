import asyncio
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, Set, Tuple

import ray
import ray.dashboard.consts as dashboard_consts
import ray.dashboard.utils as dashboard_utils
import ray.experimental.internal_kv as internal_kv
from ray._private import ray_constants
from ray._private.async_utils import enable_monitor_loop_lag
from ray._private.ray_constants import env_integer
from ray._common.usage.usage_lib import TagKey, record_extra_usage_tag
from ray._raylet import GcsClient
from ray.dashboard.consts import (
    AVAILABLE_COMPONENT_NAMES_FOR_METRICS,
    DASHBOARD_METRIC_PORT,
)
from ray.dashboard.dashboard_metrics import DashboardPrometheusMetrics
from ray.dashboard.utils import (
    DashboardHeadModule,
    DashboardHeadModuleConfig,
    async_loop_forever,
)
from ray._common.network_utils import build_address

import psutil

try:
    import prometheus_client
except ImportError:
    prometheus_client = None

if TYPE_CHECKING:
    from ray.dashboard.subprocesses.handle import SubprocessModuleHandle

logger = logging.getLogger(__name__)

# NOTE: Executor in this head is intentionally constrained to just 1 thread by
#       default to limit its concurrency, therefore reducing potential for
#       GIL contention
RAY_DASHBOARD_DASHBOARD_HEAD_TPE_MAX_WORKERS = env_integer(
    "RAY_DASHBOARD_DASHBOARD_HEAD_TPE_MAX_WORKERS", 1
)


class DashboardHead:
    def __init__(
        self,
        http_host: str,
        http_port: int,
        http_port_retries: int,
        gcs_address: str,
        cluster_id_hex: str,
        node_ip_address: str,
        log_dir: str,
        logging_level: int,
        logging_format: str,
        logging_filename: str,
        logging_rotate_bytes: int,
        logging_rotate_backup_count: int,
        temp_dir: str,
        session_dir: str,
        minimal: bool,
        serve_frontend: bool,
        modules_to_load: Optional[Set[str]] = None,
    ):
        """
        Args:
            http_host: The host address for the Http server.
            http_port: The port for the Http server.
            http_port_retries: The maximum retry to bind ports for the Http server.
            gcs_address: The GCS address in the {address}:{port} format.
            log_dir: The log directory. E.g., /tmp/session_latest/logs.
            logging_level: The logging level (e.g. logging.INFO, logging.DEBUG)
            logging_format: The format string for log messages
            logging_filename: The name of the log file
            logging_rotate_bytes: Max size in bytes before rotating log file
            logging_rotate_backup_count: Number of backup files to keep when rotating
            temp_dir: The temp directory. E.g., /tmp.
            session_dir: The session directory. E.g., tmp/session_latest.
            minimal: Whether or not it will load the minimal modules.
            serve_frontend: If configured, frontend HTML is
                served from the dashboard.
            modules_to_load: A set of module name in string to load.
                By default (None), it loads all available modules.
                Note that available modules could be changed depending on
                minimal flags.
        """
        self.minimal = minimal
        self.serve_frontend = serve_frontend
        # If it is the minimal mode, we shouldn't serve frontend.
        if self.minimal:
            self.serve_frontend = False
        # Public attributes are accessible for all head modules.
        # Walkaround for issue: https://github.com/ray-project/ray/issues/7084
        self.http_host = "127.0.0.1" if http_host == "localhost" else http_host
        self.http_port = http_port
        self.http_port_retries = http_port_retries
        self._modules_to_load = modules_to_load
        self._modules_loaded = False
        self.metrics = None

        self._executor = ThreadPoolExecutor(
            max_workers=RAY_DASHBOARD_DASHBOARD_HEAD_TPE_MAX_WORKERS,
            thread_name_prefix="dashboard_head_executor",
        )

        assert gcs_address is not None
        self.gcs_address = gcs_address
        self.cluster_id_hex = cluster_id_hex
        self.log_dir = log_dir
        self.logging_level = logging_level
        self.logging_format = logging_format
        self.logging_filename = logging_filename
        self.logging_rotate_bytes = logging_rotate_bytes
        self.logging_rotate_backup_count = logging_rotate_backup_count
        self.temp_dir = temp_dir
        self.session_dir = session_dir
        self.session_name = Path(session_dir).name
        self.gcs_error_subscriber = None
        self.gcs_log_subscriber = None
        self.ip = node_ip_address
        self.pid = os.getpid()
        self.dashboard_proc = psutil.Process()

        # If the dashboard is started as non-minimal version, http server should
        # be configured to expose APIs.
        self.http_server = None

    async def _configure_http_server(
        self,
        dashboard_head_modules: List[DashboardHeadModule],
        subprocess_module_handles: List["SubprocessModuleHandle"],
    ):
        from ray.dashboard.http_server_head import HttpServerDashboardHead

        self.http_server = HttpServerDashboardHead(
            self.ip,
            self.http_host,
            self.http_port,
            self.http_port_retries,
            self.gcs_address,
            self.session_name,
            self.metrics,
        )
        await self.http_server.run(dashboard_head_modules, subprocess_module_handles)

    @property
    def http_session(self):
        if not self._modules_loaded and not self.http_server:
            # When the dashboard is still starting up, this property gets
            # called as part of the method_route_table_factory magic. In
            # this case, the property is not actually used but the magic
            # method calls every property to look for a route to add to
            # the global route table. It should be okay for http_server
            # to still be None at this point.
            return None
        assert self.http_server, "Accessing unsupported API in a minimal ray."
        return self.http_server.http_session

    @async_loop_forever(dashboard_consts.GCS_CHECK_ALIVE_INTERVAL_SECONDS)
    async def _gcs_check_alive(self):
        try:
            # If gcs is permanently dead, gcs client will exit the process
            # (see gcs_rpc_client.h)
            await self.gcs_client.async_check_alive(node_ids=[], timeout=None)
        except Exception:
            logger.warning("Failed to check gcs aliveness, will retry", exc_info=True)

    def _load_modules(
        self, modules_to_load: Optional[Set[str]] = None
    ) -> Tuple[List[DashboardHeadModule], List["SubprocessModuleHandle"]]:
        """
        If minimal, only load DashboardHeadModule.
        If non-minimal, load both kinds of modules: DashboardHeadModule, SubprocessModule.

        If modules_to_load is not None, only load the modules in the set.
        """
        dashboard_head_modules = self._load_dashboard_head_modules(modules_to_load)
        subprocess_module_handles = self._load_subprocess_module_handles(
            modules_to_load
        )

        all_names = {type(m).__name__ for m in dashboard_head_modules} | {
            h.module_cls.__name__ for h in subprocess_module_handles
        }
        assert len(all_names) == len(dashboard_head_modules) + len(
            subprocess_module_handles
        ), "Duplicate module names. A module name can't be a DashboardHeadModule and a SubprocessModule at the same time."

        # Verify modules are loaded as expected.
        if modules_to_load is not None and all_names != modules_to_load:
            assert False, (
                f"Actual loaded modules {all_names}, doesn't match the requested modules "
                f"to load, {modules_to_load}."
            )

        self._modules_loaded = True
        return dashboard_head_modules, subprocess_module_handles

    def _load_dashboard_head_modules(
        self, modules_to_load: Optional[Set[str]] = None
    ) -> List[DashboardHeadModule]:
        """Load `DashboardHeadModule`s.

        Args:
            modules: A list of module names to load. By default (None),
                it loads all modules.
        """
        modules = []
        head_cls_list = dashboard_utils.get_all_modules(DashboardHeadModule)

        config = DashboardHeadModuleConfig(
            minimal=self.minimal,
            cluster_id_hex=self.cluster_id_hex,
            session_name=self.session_name,
            gcs_address=self.gcs_address,
            log_dir=self.log_dir,
            temp_dir=self.temp_dir,
            session_dir=self.session_dir,
            ip=self.ip,
            http_host=self.http_host,
            http_port=self.http_port,
        )

        # Select modules to load.
        if modules_to_load is not None:
            head_cls_list = [
                cls for cls in head_cls_list if cls.__name__ in modules_to_load
            ]

        logger.info(f"DashboardHeadModules to load: {modules_to_load}.")

        for cls in head_cls_list:
            logger.info(f"Loading {DashboardHeadModule.__name__}: {cls}.")
            c = cls(config)
            modules.append(c)

        logger.info(f"Loaded {len(modules)} dashboard head modules: {modules}.")
        return modules

    def _load_subprocess_module_handles(
        self, modules_to_load: Optional[Set[str]] = None
    ) -> List["SubprocessModuleHandle"]:
        """
        If minimal, return an empty list.
        If non-minimal, load `SubprocessModule`s by creating Handles to them.

        Args:
            modules: A list of module names to load. By default (None),
                it loads all modules.
        """
        if self.minimal:
            logger.info("Subprocess modules not loaded in minimal mode.")
            return []

        from ray.dashboard.subprocesses.handle import SubprocessModuleHandle
        from ray.dashboard.subprocesses.module import (
            SubprocessModule,
            SubprocessModuleConfig,
        )

        handles = []
        subprocess_cls_list = dashboard_utils.get_all_modules(SubprocessModule)

        loop = ray._common.utils.get_or_create_event_loop()
        config = SubprocessModuleConfig(
            cluster_id_hex=self.cluster_id_hex,
            gcs_address=self.gcs_address,
            session_name=self.session_name,
            temp_dir=self.temp_dir,
            session_dir=self.session_dir,
            logging_level=self.logging_level,
            logging_format=self.logging_format,
            log_dir=self.log_dir,
            logging_filename=self.logging_filename,
            logging_rotate_bytes=self.logging_rotate_bytes,
            logging_rotate_backup_count=self.logging_rotate_backup_count,
            socket_dir=str(Path(self.session_dir) / "sockets"),
        )

        # Select modules to load.
        if modules_to_load is not None:
            subprocess_cls_list = [
                cls for cls in subprocess_cls_list if cls.__name__ in modules_to_load
            ]

        for cls in subprocess_cls_list:
            logger.info(f"Loading {SubprocessModule.__name__}: {cls}.")
            handle = SubprocessModuleHandle(loop, cls, config)
            handles.append(handle)

        logger.info(f"Loaded {len(handles)} subprocess modules: {handles}.")
        return handles

    async def _setup_metrics(self, gcs_client):
        metrics = DashboardPrometheusMetrics()

        # Setup prometheus metrics export server
        assert internal_kv._internal_kv_initialized()
        assert gcs_client is not None
        address = build_address(self.ip, DASHBOARD_METRIC_PORT)
        await gcs_client.async_internal_kv_put(
            "DashboardMetricsAddress".encode(), address.encode(), True, namespace=None
        )
        if prometheus_client:
            try:
                logger.info(
                    "Starting dashboard metrics server on port {}".format(
                        DASHBOARD_METRIC_PORT
                    )
                )
                kwargs = {"addr": "127.0.0.1"} if self.ip == "127.0.0.1" else {}
                prometheus_client.start_http_server(
                    port=DASHBOARD_METRIC_PORT,
                    registry=metrics.registry,
                    **kwargs,
                )
            except Exception:
                logger.exception(
                    "An exception occurred while starting the metrics server."
                )
        elif not prometheus_client:
            logger.warning(
                "`prometheus_client` not found, so metrics will not be exported."
            )

        return metrics

    @dashboard_utils.async_loop_forever(dashboard_consts.METRICS_RECORD_INTERVAL_S)
    async def _record_dashboard_metrics(
        self, subprocess_module_handles: List["SubprocessModuleHandle"]
    ):
        labels = {
            "ip": self.ip,
            "pid": self.pid,
            "Version": ray.__version__,
            "Component": "dashboard",
            "SessionName": self.session_name,
        }
        assert "dashboard" in AVAILABLE_COMPONENT_NAMES_FOR_METRICS
        self._record_cpu_mem_metrics_for_proc(self.dashboard_proc)
        for subprocess_module_handle in subprocess_module_handles:
            assert subprocess_module_handle.process is not None
            proc = psutil.Process(subprocess_module_handle.process.pid)
            self._record_cpu_mem_metrics_for_proc(
                proc, subprocess_module_handle.module_cls.__name__
            )

        loop = ray._common.utils.get_or_create_event_loop()

        self.metrics.metrics_event_loop_tasks.labels(**labels).set(
            len(asyncio.all_tasks(loop))
        )

        # Report the max lag since the last export, if any.
        if self._event_loop_lag_s_max is not None:
            self.metrics.metrics_event_loop_lag.labels(**labels).set(
                float(self._event_loop_lag_s_max)
            )
            self._event_loop_lag_s_max = None

    def _record_cpu_mem_metrics_for_proc(
        self, proc: psutil.Process, module_name: str = ""
    ):
        labels = {
            "ip": self.ip,
            "pid": proc.pid,
            "Version": ray.__version__,
            "Component": "dashboard" if not module_name else "dashboard_" + module_name,
            "SessionName": self.session_name,
        }
        proc_attrs = proc.as_dict(attrs=["cpu_percent", "memory_full_info"])
        self.metrics.metrics_dashboard_cpu.labels(**labels).set(
            float(proc_attrs.get("cpu_percent", 0.0))
        )
        # memory_full_info is None on Mac due to the permission issue
        # (https://github.com/giampaolo/psutil/issues/883)
        if proc_attrs.get("memory_full_info") is not None:
            self.metrics.metrics_dashboard_mem_uss.labels(**labels).set(
                float(proc_attrs.get("memory_full_info").uss) / 1.0e6
            )
            self.metrics.metrics_dashboard_mem_rss.labels(**labels).set(
                float(proc_attrs.get("memory_full_info").rss) / 1.0e6
            )

    async def run(self):
        gcs_address = self.gcs_address

        # Dashboard will handle connection failure automatically
        self.gcs_client = GcsClient(address=gcs_address, cluster_id=self.cluster_id_hex)
        internal_kv._initialize_internal_kv(self.gcs_client)

        dashboard_head_modules, subprocess_module_handles = self._load_modules(
            self._modules_to_load
        )
        # Parallel start all subprocess modules.
        for handle in subprocess_module_handles:
            handle.start_module()
        # Wait for all subprocess modules to be ready.
        for handle in subprocess_module_handles:
            handle.wait_for_module_ready()

        if not self.minimal:
            self.metrics = await self._setup_metrics(self.gcs_client)
            self._event_loop_lag_s_max: Optional[float] = None

            def on_new_lag(lag_s):
                # Record the lag. It's exported in `record_dashboard_metrics`
                self._event_loop_lag_s_max = max(self._event_loop_lag_s_max or 0, lag_s)

            enable_monitor_loop_lag(on_new_lag)

            self.record_dashboard_metrics_task = asyncio.create_task(
                self._record_dashboard_metrics(subprocess_module_handles)
            )

        try:
            assert internal_kv._internal_kv_initialized()
            # Note: We always record the usage, but it is not reported
            # if the usage stats is disabled.
            record_extra_usage_tag(TagKey.DASHBOARD_USED, "False")
        except Exception as e:
            logger.warning(
                "Failed to record the dashboard usage. "
                "This error message is harmless and can be ignored. "
                f"Error: {e}"
            )

        http_host, http_port = self.http_host, self.http_port
        if self.serve_frontend:
            logger.info("Initialize the http server.")
            await self._configure_http_server(
                dashboard_head_modules, subprocess_module_handles
            )
            http_host, http_port = self.http_server.get_address()
            logger.info(
                f"http server initialized at {build_address(http_host, http_port)}"
            )
        else:
            logger.info("http server disabled.")

        # We need to expose dashboard's node's ip for other worker nodes
        # if it's listening to all interfaces.
        dashboard_http_host = (
            self.ip
            if self.http_host != ray_constants.DEFAULT_DASHBOARD_IP
            else http_host
        )
        # This synchronous code inside an async context is not great.
        # It is however acceptable, because this only gets run once
        # during initialization and therefore cannot block the event loop.
        # This could be done better in the future, including
        # removing the polling on the Ray side, by communicating the
        # server address to Ray via stdin / stdout or a pipe.
        self.gcs_client.internal_kv_put(
            ray_constants.DASHBOARD_ADDRESS.encode(),
            build_address(dashboard_http_host, http_port).encode(),
            True,
            namespace=ray_constants.KV_NAMESPACE_DASHBOARD,
        )

        concurrent_tasks = [
            self._gcs_check_alive(),
        ]
        for m in dashboard_head_modules:
            concurrent_tasks.append(m.run())
        await asyncio.gather(*concurrent_tasks)

        if self.http_server:
            await self.http_server.cleanup()
