"""Unit tests for GPU providers."""

import os
import time
import unittest
from unittest.mock import Mock, call, patch

from ray.dashboard.modules.reporter.gpu_accounting import WindowAccountant
from ray.dashboard.modules.reporter.gpu_providers import (
    MB,
    RAY_SKIP_PROCESS_UTIL_API,
    RAY_SKIP_PROCESS_UTIL_API_DEVICE_NAMES,
    AmdGpuProvider,
    GpuMetricProvider,
    GpuProvider,
    GpuProviderType,
    GpuUtilizationInfo,
    NvidiaGpuProvider,
    ProcessGPUInfo,
)


class TestProcessGPUInfo(unittest.TestCase):
    """Test ProcessGPUInfo TypedDict."""

    def test_creation(self):
        """Test ProcessGPUInfo creation."""
        process_info = ProcessGPUInfo(
            pid=1234, gpu_memory_usage=256, gpu_utilization=None
        )

        self.assertEqual(process_info["pid"], 1234)
        self.assertEqual(process_info["gpu_memory_usage"], 256)
        self.assertIsNone(process_info["gpu_utilization"])


class TestGpuUtilizationInfo(unittest.TestCase):
    """Test GpuUtilizationInfo TypedDict."""

    def test_creation_with_processes(self):
        """Test GpuUtilizationInfo with process information."""
        process1 = ProcessGPUInfo(pid=1234, gpu_memory_usage=256, gpu_utilization=None)
        process2 = ProcessGPUInfo(pid=5678, gpu_memory_usage=512, gpu_utilization=None)

        gpu_info = GpuUtilizationInfo(
            index=0,
            name="NVIDIA GeForce RTX 3080",
            uuid="GPU-12345678-1234-1234-1234-123456789abc",
            utilization_gpu=75,
            memory_used=8192,
            memory_total=10240,
            processes_pids={1234: process1, 5678: process2},
        )

        self.assertEqual(gpu_info["index"], 0)
        self.assertEqual(gpu_info["name"], "NVIDIA GeForce RTX 3080")
        self.assertEqual(gpu_info["uuid"], "GPU-12345678-1234-1234-1234-123456789abc")
        self.assertEqual(gpu_info["utilization_gpu"], 75)
        self.assertEqual(gpu_info["memory_used"], 8192)
        self.assertEqual(gpu_info["memory_total"], 10240)
        self.assertEqual(len(gpu_info["processes_pids"]), 2)
        self.assertIn(1234, gpu_info["processes_pids"])
        self.assertIn(5678, gpu_info["processes_pids"])
        self.assertEqual(gpu_info["processes_pids"][1234]["pid"], 1234)
        self.assertEqual(gpu_info["processes_pids"][1234]["gpu_memory_usage"], 256)
        self.assertEqual(gpu_info["processes_pids"][5678]["pid"], 5678)
        self.assertEqual(gpu_info["processes_pids"][5678]["gpu_memory_usage"], 512)

    def test_creation_without_processes(self):
        """Test GpuUtilizationInfo without process information."""
        gpu_info = GpuUtilizationInfo(
            index=1,
            name="AMD Radeon RX 6800 XT",
            uuid="GPU-87654321-4321-4321-4321-ba9876543210",
            utilization_gpu=None,
            memory_used=4096,
            memory_total=16384,
            processes_pids=None,
        )

        self.assertEqual(gpu_info["index"], 1)
        self.assertEqual(gpu_info["name"], "AMD Radeon RX 6800 XT")
        self.assertEqual(gpu_info["uuid"], "GPU-87654321-4321-4321-4321-ba9876543210")
        self.assertIsNone(gpu_info["utilization_gpu"])  # Should be None, not -1
        self.assertEqual(gpu_info["memory_used"], 4096)
        self.assertEqual(gpu_info["memory_total"], 16384)
        self.assertIsNone(gpu_info["processes_pids"])  # Should be None, not []


class TestGpuProvider(unittest.TestCase):
    """Test abstract GpuProvider class."""

    def test_decode_bytes(self):
        """Test _decode method with bytes input."""
        result = GpuProvider._decode(b"test string")
        self.assertEqual(result, "test string")

    def test_decode_string(self):
        """Test _decode method with string input."""
        result = GpuProvider._decode("test string")
        self.assertEqual(result, "test string")

    def test_abstract_methods_not_implemented(self):
        """Test that abstract methods raise NotImplementedError."""

        class IncompleteProvider(GpuProvider):
            pass

        with self.assertRaises(TypeError):
            IncompleteProvider()


class TestNvidiaGpuProvider(unittest.TestCase):
    """Test NvidiaGpuProvider class."""

    def setUp(self):
        """Set up test fixtures."""
        self.provider = NvidiaGpuProvider()

    def _configure_nvidia_collection(self, mock_pynvml, gpu_names):
        class MockNVMLError(Exception):
            pass

        handles = [f"gpu-{index}" for index in range(len(gpu_names))]
        gpu_indices = {handle: index for index, handle in enumerate(handles)}

        mock_pynvml.NVMLError = MockNVMLError
        mock_pynvml.NVML_TEMPERATURE_GPU = 0
        mock_pynvml.nvmlDeviceGetCount.return_value = len(gpu_names)
        mock_pynvml.nvmlDeviceGetHandleByIndex.side_effect = handles.__getitem__
        mock_pynvml.nvmlDeviceGetName.side_effect = lambda handle: gpu_names[
            gpu_indices[handle]
        ].encode()
        mock_pynvml.nvmlDeviceGetUUID.side_effect = (
            lambda handle: f"GPU-{gpu_indices[handle]}".encode()
        )
        mock_pynvml.nvmlDeviceGetMigMode.return_value = (False, False)
        mock_pynvml.nvmlDeviceGetMemoryInfo.side_effect = lambda handle: Mock(
            used=(gpu_indices[handle] + 1) * 256 * MB,
            total=16 * 1024 * MB,
        )
        mock_pynvml.nvmlDeviceGetUtilizationRates.return_value = Mock(gpu=75)
        mock_pynvml.nvmlDeviceGetComputeRunningProcesses.side_effect = lambda handle: [
            Mock(
                pid=1000 + gpu_indices[handle],
                usedGpuMemory=(gpu_indices[handle] + 1) * 128 * MB,
            )
        ]
        mock_pynvml.nvmlDeviceGetGraphicsRunningProcesses.return_value = []
        mock_pynvml.nvmlDeviceGetProcessesUtilizationInfo.side_effect = (
            lambda handle, _: [
                Mock(
                    pid=1000 + gpu_indices[handle],
                    smUtil=40 + gpu_indices[handle],
                )
            ]
        )
        mock_pynvml.nvmlDeviceGetPowerUsage.return_value = 100000
        mock_pynvml.nvmlDeviceGetTemperature.return_value = 50
        # GPM off unless a test turns it on with _configure_gpm.
        mock_pynvml.nvmlGpmQueryDeviceSupport.return_value = Mock(isSupportedDevice=0)

        self.provider._pynvml = mock_pynvml
        self.provider._initialized = True
        return handles

    def _configure_gpm(self, mock_pynvml, readings):
        """Script GPM: each nvmlGpmMetricsGet call pops {metric_id: (value, ret)}."""
        mock_pynvml.nvmlGpmQueryDeviceSupport.return_value = Mock(isSupportedDevice=1)
        mock_pynvml.nvmlGpmSampleAlloc.side_effect = lambda: object()
        mock_pynvml.NVML_GPM_METRICS_GET_VERSION = 1
        mock_pynvml.NVML_GPM_METRIC_GRAPHICS_UTIL = 1
        mock_pynvml.NVML_GPM_METRIC_SM_UTIL = 2
        mock_pynvml.c_nvmlGpmMetricsGet_t.side_effect = lambda: Mock(
            metrics=[Mock(metricId=0, value=0.0, nvmlReturn=0) for _ in range(4)]
        )
        queue = list(readings)

        def metrics_get(request):
            reading = queue.pop(0)
            for i in range(request.numMetrics):
                value, ret = reading[request.metrics[i].metricId]
                request.metrics[i].value = value
                request.metrics[i].nvmlReturn = ret
            return request

        mock_pynvml.nvmlGpmMetricsGet.side_effect = metrics_get

    def _gpm_clock(self, uuid="GPU-0"):
        """Give the device's GPM state a fake monotonic clock."""
        from ray.dashboard.modules.reporter.gpu_providers import _GpmState

        clock = {"now": 1000.0}
        self.provider._gpm_state[uuid] = _GpmState(
            accountant=WindowAccountant(clock=lambda: clock["now"])
        )
        return clock

    def test_get_provider_name(self):
        """Test provider name."""
        self.assertEqual(self.provider.get_provider_name(), GpuProviderType.NVIDIA)

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_is_available_success(self, mock_pynvml):
        """Test is_available when NVIDIA GPU is available."""
        mock_pynvml.nvmlInit.return_value = None
        mock_pynvml.nvmlShutdown.return_value = None

        # Mock sys.modules to make the import work
        import sys

        original_modules = sys.modules.copy()
        sys.modules["ray._private.thirdparty.pynvml"] = mock_pynvml

        try:
            self.assertTrue(self.provider.is_available())
            mock_pynvml.nvmlInit.assert_called_once()
            mock_pynvml.nvmlShutdown.assert_called_once()
        finally:
            # Restore original modules
            sys.modules.clear()
            sys.modules.update(original_modules)

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_is_available_failure(self, mock_pynvml):
        """Test is_available when NVIDIA GPU is not available."""
        mock_pynvml.nvmlInit.side_effect = Exception("NVIDIA driver not found")

        # Mock sys.modules to make the import work but nvmlInit fail
        import sys

        original_modules = sys.modules.copy()
        sys.modules["ray._private.thirdparty.pynvml"] = mock_pynvml

        try:
            self.assertFalse(self.provider.is_available())
        finally:
            # Restore original modules
            sys.modules.clear()
            sys.modules.update(original_modules)

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_initialize_success(self, mock_pynvml):
        """Test successful initialization."""
        # Ensure provider starts fresh
        self.provider._initialized = False

        mock_pynvml.nvmlInit.return_value = None

        # Mock sys.modules to make the import work
        import sys

        original_modules = sys.modules.copy()
        sys.modules["ray._private.thirdparty.pynvml"] = mock_pynvml

        try:
            self.assertTrue(self.provider._initialize())
            self.assertTrue(self.provider._initialized)
            mock_pynvml.nvmlInit.assert_called_once()
        finally:
            # Restore original modules
            sys.modules.clear()
            sys.modules.update(original_modules)

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_initialize_failure(self, mock_pynvml):
        """Test failed initialization."""
        # Ensure provider starts fresh
        self.provider._initialized = False

        # Make nvmlInit fail
        mock_pynvml.nvmlInit.side_effect = Exception("Initialization failed")

        # Mock sys.modules to make the import work but nvmlInit fail
        import sys

        original_modules = sys.modules.copy()
        sys.modules["ray._private.thirdparty.pynvml"] = mock_pynvml

        try:
            self.assertFalse(self.provider._initialize())
            self.assertFalse(self.provider._initialized)
        finally:
            # Restore original modules
            sys.modules.clear()
            sys.modules.update(original_modules)

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_initialize_already_initialized(self, mock_pynvml):
        """Test initialization when already initialized."""
        self.provider._initialized = True

        self.assertTrue(self.provider._initialize())
        mock_pynvml.nvmlInit.assert_not_called()

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_shutdown(self, mock_pynvml):
        """Test shutdown."""
        self.provider._initialized = True
        self.provider._pynvml = mock_pynvml

        self.provider._shutdown()

        self.assertFalse(self.provider._initialized)
        mock_pynvml.nvmlShutdown.assert_called_once()

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_shutdown_not_initialized(self, mock_pynvml):
        """Test shutdown when not initialized."""
        self.provider._shutdown()
        mock_pynvml.nvmlShutdown.assert_not_called()

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_process_utilization_skipped_only_for_ppu_device(self, mock_pynvml):
        """Test that PPU devices are skipped without degrading NVIDIA GPUs."""
        handles = self._configure_nvidia_collection(
            mock_pynvml, ["MetaX ppu Accelerator", "NVIDIA H100"]
        )

        with self.assertLogs(
            "ray.dashboard.modules.reporter.gpu_providers", level="INFO"
        ) as logs:
            result = self.provider.get_gpu_utilization()

        self.assertEqual(self.provider._skip_process_util_gpu_indices, {0})
        self.assertEqual(
            mock_pynvml.nvmlDeviceGetProcessesUtilizationInfo.call_args_list,
            [call(handles[1], 0)],
        )
        self.assertEqual(result[0]["processes_pids"][1000]["gpu_memory_usage"], 128)
        self.assertIsNone(result[0]["processes_pids"][1000]["gpu_utilization"])
        self.assertEqual(result[0]["memory_used"], 256)
        self.assertEqual(result[0]["power_mw"], 100000)
        self.assertEqual(result[0]["temperature_c"], 50)
        self.assertEqual(result[1]["processes_pids"][1001]["gpu_utilization"], 41)
        self.assertTrue(
            any("MetaX ppu Accelerator" in message for message in logs.output)
        )

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_process_utilization_skipped_for_nonzero_ppu_device(self, mock_pynvml):
        """Test that a nonzero PPU index is skipped independently."""
        handles = self._configure_nvidia_collection(
            mock_pynvml, ["NVIDIA H100", "MetaX PPU Accelerator"]
        )

        result = self.provider.get_gpu_utilization()

        self.assertEqual(self.provider._skip_process_util_gpu_indices, {1})
        self.assertEqual(
            mock_pynvml.nvmlDeviceGetProcessesUtilizationInfo.call_args_list,
            [call(handles[0], 0)],
        )
        self.assertEqual(result[0]["processes_pids"][1000]["gpu_utilization"], 40)
        self.assertIsNone(result[1]["processes_pids"][1001]["gpu_utilization"])

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_ppu_detection_matches_ppu_device_name_patterns(self, mock_pynvml):
        """Test PPU model suffixes without matching embedded PPU strings."""
        handles = self._configure_nvidia_collection(
            mock_pynvml,
            [
                "XPPU Accelerator",
                "PPU2 Accelerator",
                "PPU_2 Accelerator",
                "PPU-ZW810",
                "PPU810",
                "PPUZW910",
            ],
        )

        self.provider.get_gpu_utilization()

        self.assertEqual(self.provider._skip_process_util_gpu_indices, {1, 2, 3, 4, 5})
        self.assertEqual(
            mock_pynvml.nvmlDeviceGetProcessesUtilizationInfo.call_args_list,
            [call(handles[0], 0)],
        )

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_process_utilization_skipped_for_configured_device_name(self, mock_pynvml):
        """Test that operators can skip an unsafe device by its exact name."""
        with patch.dict(
            os.environ,
            {RAY_SKIP_PROCESS_UTIL_API_DEVICE_NAMES: "vendor accelerator,Other"},
        ):
            self.provider = NvidiaGpuProvider()
        handles = self._configure_nvidia_collection(
            mock_pynvml, ["Vendor Accelerator", "NVIDIA H100"]
        )

        result = self.provider.get_gpu_utilization()

        self.assertEqual(self.provider._skip_process_util_gpu_indices, {0})
        self.assertEqual(
            mock_pynvml.nvmlDeviceGetProcessesUtilizationInfo.call_args_list,
            [call(handles[1], 0)],
        )
        self.assertIsNone(result[0]["processes_pids"][1000]["gpu_utilization"])

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_ppu_detection_failure_falls_back_and_retries(self, mock_pynvml):
        """Test that detection failure skips one cycle and is retried."""
        handles = self._configure_nvidia_collection(
            mock_pynvml, ["NVIDIA H100", "NVIDIA H200"]
        )
        name_call_counts = {handle: 0 for handle in handles}

        def get_name(handle):
            name_call_counts[handle] += 1
            if handle == handles[1] and name_call_counts[handle] == 1:
                raise mock_pynvml.NVMLError("temporary name query failure")
            return f"NVIDIA GPU {handle}".encode()

        mock_pynvml.nvmlDeviceGetName.side_effect = get_name

        with patch.object(self.provider, "_shutdown"):
            first_result = self.provider.get_gpu_utilization()

            self.assertTrue(self.provider._force_fallback_this_cycle)
            self.assertFalse(self.provider._ppu_detection_done)
            mock_pynvml.nvmlDeviceGetProcessesUtilizationInfo.assert_not_called()
            self.assertEqual(
                first_result[0]["processes_pids"][1000]["gpu_memory_usage"], 128
            )
            self.assertIsNone(
                first_result[0]["processes_pids"][1000]["gpu_utilization"]
            )

            second_result = self.provider.get_gpu_utilization()

        self.assertFalse(self.provider._force_fallback_this_cycle)
        self.assertTrue(self.provider._ppu_detection_done)
        self.assertEqual(
            mock_pynvml.nvmlDeviceGetProcessesUtilizationInfo.call_args_list,
            [call(handles[0], 0), call(handles[1], 0)],
        )
        self.assertEqual(
            second_result[0]["processes_pids"][1000]["gpu_utilization"], 40
        )

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_ppu_detection_failure_falls_back_on_every_cycle(self, mock_pynvml):
        """Test that repeated detection failures never call the unsafe API."""
        handles = self._configure_nvidia_collection(
            mock_pynvml, ["NVIDIA H100", "NVIDIA H200"]
        )

        def get_name(handle):
            if handle == handles[1]:
                raise mock_pynvml.NVMLError("persistent name query failure")
            return b"NVIDIA H100"

        mock_pynvml.nvmlDeviceGetName.side_effect = get_name

        with patch.object(self.provider, "_shutdown"):
            self.provider.get_gpu_utilization()
            self.provider.get_gpu_utilization()

        self.assertEqual(self.provider._process_util_detection_failure_count, 2)
        mock_pynvml.nvmlDeviceGetProcessesUtilizationInfo.assert_not_called()

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_ppu_detection_cache_invalidated_by_gpu_count(self, mock_pynvml):
        """Test that a GPU count change triggers full detection again."""
        self._configure_nvidia_collection(
            mock_pynvml, ["NVIDIA H100", "NVIDIA H200", "MetaX PPU"]
        )

        self.provider._detect_ppu_devices(2)
        self.assertEqual(mock_pynvml.nvmlDeviceGetName.call_count, 2)

        self.provider._detect_ppu_devices(2)
        self.assertEqual(mock_pynvml.nvmlDeviceGetName.call_count, 2)

        self.provider._detect_ppu_devices(3)
        self.assertEqual(mock_pynvml.nvmlDeviceGetName.call_count, 5)
        self.assertEqual(self.provider._skip_process_util_gpu_indices, {2})

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_process_utilization_operator_override(self, mock_pynvml):
        """Test that the operator override skips only process utilization."""
        with patch.dict(os.environ, {RAY_SKIP_PROCESS_UTIL_API: "true"}):
            self.provider = NvidiaGpuProvider()
        self._configure_nvidia_collection(mock_pynvml, ["NVIDIA H100"])

        with self.assertLogs(
            "ray.dashboard.modules.reporter.gpu_providers", level="INFO"
        ) as logs:
            result = self.provider.get_gpu_utilization()

        self.assertTrue(self.provider._force_skip_process_util_api)
        self.assertFalse(self.provider._ppu_detection_done)
        # The only name lookup is the one needed to populate the GPU info.
        self.assertEqual(mock_pynvml.nvmlDeviceGetName.call_count, 1)
        mock_pynvml.nvmlDeviceGetProcessesUtilizationInfo.assert_not_called()
        self.assertEqual(result[0]["processes_pids"][1000]["gpu_memory_usage"], 128)
        self.assertIsNone(result[0]["processes_pids"][1000]["gpu_utilization"])
        self.assertTrue(
            any(RAY_SKIP_PROCESS_UTIL_API in message for message in logs.output)
        )

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_process_utilization_operator_override_values(self, mock_pynvml):
        """Test accepted and rejected values for the operator override."""
        for value, expected in (
            ("true", True),
            ("1", True),
            ("false", False),
            ("0", False),
        ):
            with self.subTest(value=value):
                with patch.dict(os.environ, {RAY_SKIP_PROCESS_UTIL_API: value}):
                    self.provider = NvidiaGpuProvider()
                self._configure_nvidia_collection(mock_pynvml, ["NVIDIA H100"])

                self.provider.get_gpu_utilization()

                self.assertEqual(self.provider._force_skip_process_util_api, expected)
                process_utilization_api = (
                    mock_pynvml.nvmlDeviceGetProcessesUtilizationInfo
                )
                if expected:
                    process_utilization_api.assert_not_called()
                else:
                    process_utilization_api.assert_called_once()
                mock_pynvml.reset_mock()

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_get_gpu_utilization_success(self, mock_pynvml):
        """Test successful GPU utilization retrieval."""
        # Mock GPU device
        mock_handle = Mock()
        mock_memory_info = Mock()
        mock_memory_info.used = 8 * MB * 1024  # 8GB used
        mock_memory_info.total = 12 * MB * 1024  # 12GB total

        mock_utilization_info = Mock()
        mock_utilization_info.gpu = 75

        mock_process = Mock()
        mock_process.pid = 1234
        mock_process.usedGpuMemory = 256 * MB

        mock_process_utilization = Mock()
        mock_process_utilization.pid = 1234
        mock_process_utilization.smUtil = 35

        # Configure mocks
        mock_pynvml.nvmlInit.return_value = None
        mock_pynvml.nvmlDeviceGetMigMode.return_value = (False, False)
        mock_pynvml.nvmlDeviceGetCount.return_value = 1
        mock_pynvml.nvmlDeviceGetHandleByIndex.return_value = mock_handle
        mock_pynvml.nvmlDeviceGetMemoryInfo.return_value = mock_memory_info
        mock_pynvml.nvmlDeviceGetUtilizationRates.return_value = mock_utilization_info
        mock_pynvml.nvmlDeviceGetComputeRunningProcesses.return_value = [mock_process]
        mock_pynvml.nvmlDeviceGetGraphicsRunningProcesses.return_value = []
        mock_pynvml.nvmlDeviceGetProcessesUtilizationInfo.return_value = [
            mock_process_utilization
        ]
        mock_pynvml.nvmlDeviceGetName.return_value = b"NVIDIA GeForce RTX 3080"
        mock_pynvml.nvmlDeviceGetUUID.return_value = (
            b"GPU-12345678-1234-1234-1234-123456789abc"
        )
        mock_pynvml.nvmlShutdown.return_value = None

        # Set up provider state
        self.provider._pynvml = mock_pynvml
        self.provider._initialized = True

        result = self.provider.get_gpu_utilization()

        self.assertEqual(len(result), 1)
        gpu_info = result[0]

        self.assertEqual(gpu_info["index"], 0)
        self.assertEqual(gpu_info["name"], "NVIDIA GeForce RTX 3080")
        self.assertEqual(gpu_info["uuid"], "GPU-12345678-1234-1234-1234-123456789abc")
        self.assertEqual(gpu_info["utilization_gpu"], 75)
        self.assertEqual(gpu_info["memory_used"], 8 * 1024)  # 8GB in MB
        self.assertEqual(gpu_info["memory_total"], 12 * 1024)  # 12GB in MB
        self.assertEqual(len(gpu_info["processes_pids"]), 1)
        self.assertEqual(gpu_info["processes_pids"][1234]["pid"], 1234)
        self.assertEqual(gpu_info["processes_pids"][1234]["gpu_memory_usage"], 256)
        self.assertEqual(gpu_info["processes_pids"][1234]["gpu_utilization"], 35)

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_process_utilization_cursor_is_microseconds(self, mock_pynvml):
        """The cursor passed to NVML is the previous poll's time in microseconds."""
        self._configure_nvidia_collection(mock_pynvml, ["NVIDIA T4"])
        cursors = []

        def record_cursor(handle, last_seen_us):
            cursors.append(last_seen_us)
            return [Mock(pid=1000, smUtil=40)]

        mock_pynvml.nvmlDeviceGetProcessesUtilizationInfo.side_effect = record_cursor

        with patch.object(self.provider, "_shutdown"):
            before_us = int(time.time() * 1_000_000)
            self.provider.get_gpu_utilization()
            between_us = int(time.time() * 1_000_000)
            self.provider.get_gpu_utilization()

        # The first poll reads the whole buffer the driver retains.
        self.assertEqual(cursors[0], 0)
        # The second poll continues from the first poll, in microseconds; a
        # millisecond cursor would be ~1000x smaller than `before_us`.
        self.assertGreaterEqual(cursors[1], before_us)
        self.assertLessEqual(cursors[1], between_us)

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_process_utilization_cursor_follows_newest_sample(self, mock_pynvml):
        """The cursor continues from the newest sample NVML returned."""
        self._configure_nvidia_collection(mock_pynvml, ["NVIDIA T4"])
        newest_us = int(time.time() * 1_000_000) + 500_000
        cursors = []

        def record_cursor(handle, last_seen_us):
            cursors.append(last_seen_us)
            return [Mock(pid=1000, smUtil=40, timeStamp=newest_us)]

        mock_pynvml.nvmlDeviceGetProcessesUtilizationInfo.side_effect = record_cursor

        with patch.object(self.provider, "_shutdown"):
            self.provider.get_gpu_utilization()
            self.provider.get_gpu_utilization()

        self.assertEqual(cursors[1], newest_us)

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_process_utilization_cursor_follows_sample_before_now(self, mock_pynvml):
        """A sample older than the poll still sets the cursor, so none is skipped."""
        self._configure_nvidia_collection(mock_pynvml, ["NVIDIA T4"])
        newest_us = int(time.time() * 1_000_000) - 200_000
        cursors = []

        def record_cursor(handle, last_seen_us):
            cursors.append(last_seen_us)
            return [Mock(pid=1000, smUtil=40, timeStamp=newest_us)]

        mock_pynvml.nvmlDeviceGetProcessesUtilizationInfo.side_effect = record_cursor

        with patch.object(self.provider, "_shutdown"):
            self.provider.get_gpu_utilization()
            self.provider.get_gpu_utilization()

        self.assertEqual(cursors[1], newest_us)

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_process_utilization_cursor_ignores_sample_far_ahead(self, mock_pynvml):
        """A sample timestamped far in the future does not become the cursor."""
        self._configure_nvidia_collection(mock_pynvml, ["NVIDIA T4"])
        cursors = []

        def record_cursor(handle, last_seen_us):
            cursors.append(last_seen_us)
            return [
                Mock(
                    pid=1000,
                    smUtil=40,
                    timeStamp=int(time.time() * 1_000_000) + 60_000_000,
                )
            ]

        mock_pynvml.nvmlDeviceGetProcessesUtilizationInfo.side_effect = record_cursor

        with patch.object(self.provider, "_shutdown"):
            before_us = int(time.time() * 1_000_000)
            self.provider.get_gpu_utilization()
            between_us = int(time.time() * 1_000_000)
            self.provider.get_gpu_utilization()

        self.assertGreaterEqual(cursors[1], before_us)
        self.assertLessEqual(cursors[1], between_us)

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_process_utilization_cursor_reset_after_clock_step(self, mock_pynvml):
        """A cursor far in the future (clock stepped backwards) restarts from now."""
        self._configure_nvidia_collection(mock_pynvml, ["NVIDIA T4"])
        cursors = []

        def record_cursor(handle, last_seen_us):
            cursors.append(last_seen_us)
            return [Mock(pid=1000, smUtil=40)]

        mock_pynvml.nvmlDeviceGetProcessesUtilizationInfo.side_effect = record_cursor
        now_us = int(time.time() * 1_000_000)
        self.provider._gpu_process_last_sample_ts[0] = now_us + 60_000_000

        self.provider.get_gpu_utilization()
        after_us = int(time.time() * 1_000_000)

        self.assertGreaterEqual(cursors[0], now_us)
        self.assertLessEqual(cursors[0], after_us)

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_process_utilization_cursor_reset_kept_when_query_fails(self, mock_pynvml):
        """After a clock step back, a failed query doesn't leave the cursor ahead."""
        self._configure_nvidia_collection(mock_pynvml, ["NVIDIA T4"])
        mock_pynvml.nvmlDeviceGetProcessesUtilizationInfo.side_effect = (
            mock_pynvml.NVMLError("Unknown Error")
        )
        now_us = int(time.time() * 1_000_000)
        self.provider._gpu_process_last_sample_ts[0] = now_us + 60_000_000

        self.provider.get_gpu_utilization()
        after_us = int(time.time() * 1_000_000)

        cursor_us = self.provider._gpu_process_last_sample_ts[0]
        self.assertGreaterEqual(cursor_us, now_us)
        self.assertLessEqual(cursor_us, after_us)

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_process_utilization_idle_window_advances_cursor(self, mock_pynvml):
        """NVML_ERROR_NOT_FOUND means no sample since the cursor: an idle window."""
        self._configure_nvidia_collection(mock_pynvml, ["NVIDIA T4"])

        class MockNVMLErrorNotFound(mock_pynvml.NVMLError):
            pass

        mock_pynvml.NVMLError_NotFound = MockNVMLErrorNotFound
        mock_pynvml.nvmlDeviceGetProcessesUtilizationInfo.side_effect = (
            MockNVMLErrorNotFound()
        )

        before_us = int(time.time() * 1_000_000)
        result = self.provider.get_gpu_utilization()
        after_us = int(time.time() * 1_000_000)

        # The process found by the running-process API is kept, without a
        # utilization value, and the window moves on.
        self.assertIsNone(result[0]["processes_pids"][1000]["gpu_utilization"])
        cursor_us = self.provider._gpu_process_last_sample_ts[0]
        self.assertGreaterEqual(cursor_us, before_us)
        self.assertLessEqual(cursor_us, after_us)

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_process_utilization_error_keeps_cursor(self, mock_pynvml):
        """Any other NVML error leaves the cursor so the next poll covers the gap."""
        self._configure_nvidia_collection(mock_pynvml, ["NVIDIA T4"])
        mock_pynvml.nvmlDeviceGetProcessesUtilizationInfo.side_effect = (
            mock_pynvml.NVMLError("boom")
        )
        self.provider._gpu_process_last_sample_ts[0] = 1234

        result = self.provider.get_gpu_utilization()

        self.assertIsNone(result[0]["processes_pids"][1000]["gpu_utilization"])
        self.assertEqual(self.provider._gpu_process_last_sample_ts[0], 1234)

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_get_gpu_utilization_with_errors(self, mock_pynvml):
        """Test GPU utilization retrieval with partial errors."""
        mock_handle = Mock()
        mock_memory_info = Mock()
        mock_memory_info.used = 4 * MB * 1024
        mock_memory_info.total = 8 * MB * 1024

        # Create mock NVML error class
        class MockNVMLError(Exception):
            pass

        mock_pynvml.NVMLError = MockNVMLError

        # Configure mocks with some failures
        mock_pynvml.nvmlInit.return_value = None
        mock_pynvml.nvmlDeviceGetCount.return_value = 1
        mock_pynvml.nvmlDeviceGetHandleByIndex.return_value = mock_handle
        mock_pynvml.nvmlDeviceGetMemoryInfo.return_value = mock_memory_info
        mock_pynvml.nvmlDeviceGetUtilizationRates.side_effect = MockNVMLError(
            "Utilization not available"
        )
        mock_pynvml.nvmlDeviceGetComputeRunningProcesses.side_effect = MockNVMLError(
            "Process info not available"
        )
        mock_pynvml.nvmlDeviceGetGraphicsRunningProcesses.side_effect = MockNVMLError(
            "Process info not available"
        )
        mock_pynvml.nvmlDeviceGetName.return_value = b"NVIDIA Tesla V100"
        mock_pynvml.nvmlDeviceGetUUID.return_value = (
            b"GPU-87654321-4321-4321-4321-ba9876543210"
        )
        mock_pynvml.nvmlShutdown.return_value = None

        # Set up provider state
        self.provider._pynvml = mock_pynvml
        self.provider._initialized = True

        result = self.provider.get_gpu_utilization()

        self.assertEqual(len(result), 1)
        gpu_info = result[0]

        self.assertEqual(gpu_info["index"], 0)
        self.assertEqual(gpu_info["name"], "NVIDIA Tesla V100")
        # No utilization sample when the query fails; never a -1 sentinel.
        self.assertIsNone(gpu_info["utilization_gpu"])
        self.assertEqual(
            gpu_info["processes_pids"], {}
        )  # Should be empty dict due to error

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_get_gpu_utilization_with_mig(self, mock_pynvml):
        """Test GPU utilization retrieval with MIG devices."""
        # Mock regular GPU handle
        mock_gpu_handle = Mock()
        mock_memory_info = Mock()
        mock_memory_info.used = 4 * MB * 1024
        mock_memory_info.total = 8 * MB * 1024

        # Mock MIG device handle and info
        mock_mig_handle = Mock()
        mock_mig_memory_info = Mock()
        mock_mig_memory_info.used = 2 * MB * 1024
        mock_mig_memory_info.total = 4 * MB * 1024

        mock_mig_utilization_info = Mock()
        mock_mig_utilization_info.gpu = 80

        # Configure mocks for MIG-enabled GPU
        mock_pynvml.nvmlInit.return_value = None
        mock_pynvml.nvmlDeviceGetCount.return_value = 1
        mock_pynvml.nvmlDeviceGetHandleByIndex.return_value = mock_gpu_handle

        # MIG mode enabled
        mock_pynvml.nvmlDeviceGetMigMode.return_value = (
            True,
            True,
        )  # (current, pending)
        mock_pynvml.nvmlDeviceGetMaxMigDeviceCount.return_value = 1  # Only 1 MIG device
        mock_pynvml.nvmlDeviceGetMigDeviceHandleByIndex.return_value = mock_mig_handle

        # MIG device info
        mock_pynvml.nvmlDeviceGetMemoryInfo.return_value = mock_mig_memory_info
        mock_pynvml.nvmlDeviceGetUtilizationRates.return_value = (
            mock_mig_utilization_info
        )
        mock_pynvml.nvmlDeviceGetComputeRunningProcesses.return_value = []
        mock_pynvml.nvmlDeviceGetGraphicsRunningProcesses.return_value = []
        mock_pynvml.nvmlDeviceGetName.return_value = b"NVIDIA A100-SXM4-40GB MIG 1g.5gb"
        mock_pynvml.nvmlDeviceGetUUID.return_value = (
            b"MIG-12345678-1234-1234-1234-123456789abc"
        )
        mock_pynvml.nvmlShutdown.return_value = None

        # Set up provider state
        self.provider._pynvml = mock_pynvml
        self.provider._initialized = True

        result = self.provider.get_gpu_utilization()

        # Should return MIG device info instead of regular GPU
        self.assertEqual(
            len(result), 1
        )  # Only one MIG device due to exception handling
        gpu_info = result[0]

        self.assertEqual(gpu_info["index"], 0)  # First MIG device (0 * 1000 + 0)
        self.assertEqual(gpu_info["name"], "NVIDIA A100-SXM4-40GB MIG 1g.5gb")
        self.assertEqual(gpu_info["uuid"], "MIG-12345678-1234-1234-1234-123456789abc")
        self.assertEqual(gpu_info["utilization_gpu"], 80)
        self.assertEqual(gpu_info["memory_used"], 2 * 1024)  # 2GB in MB
        self.assertEqual(gpu_info["memory_total"], 4 * 1024)  # 4GB in MB
        self.assertEqual(gpu_info["processes_pids"], {})

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_get_gpu_utilization_with_mig_unsupported_utilization(self, mock_pynvml):
        """A MIG slice whose utilization query is unsupported reports None."""
        mock_gpu_handle = Mock()
        mock_mig_handle = Mock()
        mock_mig_memory_info = Mock()
        mock_mig_memory_info.used = 2 * MB * 1024
        mock_mig_memory_info.total = 4 * MB * 1024

        class MockNVMLError(Exception):
            pass

        mock_pynvml.NVMLError = MockNVMLError
        mock_pynvml.nvmlInit.return_value = None
        mock_pynvml.nvmlDeviceGetCount.return_value = 1
        mock_pynvml.nvmlDeviceGetHandleByIndex.return_value = mock_gpu_handle
        mock_pynvml.nvmlDeviceGetMigMode.return_value = (True, True)
        mock_pynvml.nvmlDeviceGetMaxMigDeviceCount.return_value = 1
        mock_pynvml.nvmlDeviceGetMigDeviceHandleByIndex.return_value = mock_mig_handle
        mock_pynvml.nvmlDeviceGetMemoryInfo.return_value = mock_mig_memory_info
        # NVML does not support nvmlDeviceGetUtilizationRates on MIG devices.
        mock_pynvml.nvmlDeviceGetUtilizationRates.side_effect = MockNVMLError(
            "Not Supported"
        )
        mock_pynvml.nvmlDeviceGetComputeRunningProcesses.return_value = []
        mock_pynvml.nvmlDeviceGetGraphicsRunningProcesses.return_value = []
        mock_pynvml.nvmlDeviceGetName.return_value = b"NVIDIA A100-SXM4-40GB MIG 1g.5gb"
        mock_pynvml.nvmlDeviceGetUUID.return_value = (
            b"MIG-12345678-1234-1234-1234-123456789abc"
        )
        mock_pynvml.nvmlShutdown.return_value = None
        self.provider._pynvml = mock_pynvml
        self.provider._initialized = True

        result = self.provider.get_gpu_utilization()

        self.assertEqual(len(result), 1)
        self.assertIsNone(result[0]["utilization_gpu"])
        self.assertEqual(result[0]["memory_used"], 2 * 1024)

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_collection_errors_counted_and_drained(self, mock_pynvml):
        """Failed reads are counted by (api, error class) and reset on drain."""
        self._configure_nvidia_collection(mock_pynvml, ["NVIDIA T4"])
        mock_pynvml.nvmlDeviceGetUtilizationRates.side_effect = mock_pynvml.NVMLError(
            "Unknown Error"
        )

        with patch.object(self.provider, "_shutdown"):
            self.provider.get_gpu_utilization()
            self.assertEqual(
                self.provider.drain_collection_errors(),
                {("utilization", "MockNVMLError"): 1},
            )
            # Drained: a clean poll reports nothing.
            mock_pynvml.nvmlDeviceGetUtilizationRates.side_effect = None
            self.provider.get_gpu_utilization()
            self.assertEqual(self.provider.drain_collection_errors(), {})

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_collection_errors_skip_idle_process_utilization(self, mock_pynvml):
        """NOT_FOUND from the process utilization API is idle, not an error."""
        self._configure_nvidia_collection(mock_pynvml, ["NVIDIA T4"])

        class MockNVMLErrorNotFound(mock_pynvml.NVMLError):
            pass

        mock_pynvml.NVMLError_NotFound = MockNVMLErrorNotFound
        mock_pynvml.nvmlDeviceGetProcessesUtilizationInfo.side_effect = (
            MockNVMLErrorNotFound()
        )
        with patch.object(self.provider, "_shutdown"):
            self.provider.get_gpu_utilization()
            self.assertEqual(self.provider.drain_collection_errors(), {})

            mock_pynvml.nvmlDeviceGetProcessesUtilizationInfo.side_effect = (
                mock_pynvml.NVMLError("Unknown Error")
            )
            self.provider.get_gpu_utilization()
        self.assertEqual(
            self.provider.drain_collection_errors(),
            {("process_utilization", "MockNVMLError"): 1},
        )

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_collection_errors_skip_unsupported_apis(self, mock_pynvml):
        """An API the device or driver doesn't have is not a failure."""
        self._configure_nvidia_collection(mock_pynvml, ["NVIDIA T4"])
        not_supported = type("NVMLError_NotSupported", (mock_pynvml.NVMLError,), {})
        function_not_found = type(
            "NVMLError_FunctionNotFound", (mock_pynvml.NVMLError,), {}
        )
        mock_pynvml.nvmlDeviceGetPowerUsage.side_effect = not_supported()
        mock_pynvml.nvmlDeviceGetProcessesUtilizationInfo.side_effect = (
            function_not_found()
        )

        self.provider.get_gpu_utilization()

        self.assertEqual(self.provider.drain_collection_errors(), {})

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_collection_errors_count_init_failure(self, mock_pynvml):
        """A failed nvmlInit is counted, since every GPU metric goes absent."""
        self._configure_nvidia_collection(mock_pynvml, ["NVIDIA T4"])
        mock_pynvml.nvmlInit.side_effect = mock_pynvml.NVMLError("Driver Not Loaded")
        self.provider._initialized = False

        self.assertEqual(self.provider.get_gpu_utilization(), [])
        errors = self.provider.drain_collection_errors()
        self.assertEqual(
            [(api, count) for (api, _), count in errors.items()], [("init", 1)]
        )

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_gpm_keeps_nvml_open_between_polls(self, mock_pynvml):
        """With GPM samples allocated, NVML isn't shut down between polls."""
        self._configure_nvidia_collection(mock_pynvml, ["NVIDIA H100"])
        self._configure_gpm(mock_pynvml, [{2: (25.0, 0), 1: (40.0, 0)}])
        clock = self._gpm_clock()

        self.provider.get_gpu_utilization()
        clock["now"] += 5.0
        second = self.provider.get_gpu_utilization()[0]

        self.assertEqual(second["sm_active_percent"], 25.0)
        mock_pynvml.nvmlShutdown.assert_not_called()
        mock_pynvml.nvmlGpmSampleFree.assert_not_called()

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_nvml_shut_down_each_poll_without_gpm(self, mock_pynvml):
        """Devices without GPM keep the per-poll nvmlInit/nvmlShutdown."""
        self._configure_nvidia_collection(mock_pynvml, ["NVIDIA T4"])
        mock_pynvml.nvmlGpmQueryDeviceSupport.return_value = Mock(isSupportedDevice=0)

        self.provider.get_gpu_utilization()

        mock_pynvml.nvmlShutdown.assert_called_once()

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_gpm_samples_freed_before_shutdown_after_error(self, mock_pynvml):
        """A failed poll frees the GPM samples, then shuts NVML down."""
        self._configure_nvidia_collection(mock_pynvml, ["NVIDIA H100"])
        self._configure_gpm(mock_pynvml, [])
        self._gpm_clock()
        calls = []
        mock_pynvml.nvmlGpmSampleFree.side_effect = lambda s: calls.append(("free", s))
        mock_pynvml.nvmlShutdown.side_effect = lambda: calls.append(("shutdown",))

        self.provider.get_gpu_utilization()
        samples = list(self.provider._gpm_state["GPU-0"].samples)
        mock_pynvml.nvmlDeviceGetCount.side_effect = mock_pynvml.NVMLError(
            "Uninitialized"
        )
        self.provider.get_gpu_utilization()

        self.assertEqual(
            calls, [("free", samples[0]), ("free", samples[1]), ("shutdown",)]
        )
        state = self.provider._gpm_state["GPU-0"]
        self.assertEqual(state.samples, [])
        # Checked and allocated again after the next nvmlInit.
        self.assertIsNone(state.supported)

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_gpm_first_poll_opens_window_second_reports(self, mock_pynvml):
        """SM active and graphics engine come from GPM over the poll window."""
        self._configure_nvidia_collection(mock_pynvml, ["NVIDIA H100"])
        self._configure_gpm(mock_pynvml, [{2: (25.0, 0), 1: (40.0, 0)}])
        clock = self._gpm_clock()

        with patch.object(self.provider, "_shutdown"):
            first = self.provider.get_gpu_utilization()[0]
            clock["now"] += 5.0
            second = self.provider.get_gpu_utilization()[0]

        self.assertIsNone(first["sm_active_percent"])
        self.assertIsNone(first["sm_active_seconds"])
        self.assertEqual(second["sm_active_percent"], 25.0)
        self.assertAlmostEqual(second["sm_active_seconds"], 1.25)  # 25 % of 5 s
        self.assertEqual(second["graphics_engine_active_percent"], 40.0)
        self.assertEqual(mock_pynvml.nvmlGpmSampleAlloc.call_count, 2)
        # The two polls filled different buffers.
        buffers = [c.args[1] for c in mock_pynvml.nvmlGpmSampleGet.call_args_list]
        self.assertIsNot(buffers[0], buffers[1])
        self.assertEqual(self.provider.drain_collection_errors(), {})

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_gpm_unsupported_device_reports_nothing(self, mock_pynvml):
        """Pre-Hopper devices (or bindings without GPM) get no GPM metrics or errors."""
        self._configure_nvidia_collection(mock_pynvml, ["NVIDIA T4"])

        with patch.object(self.provider, "_shutdown"):
            self.provider.get_gpu_utilization()
            result = self.provider.get_gpu_utilization()[0]

        self.assertIsNone(result["sm_active_percent"])
        self.assertIsNone(result["graphics_engine_active_percent"])
        mock_pynvml.nvmlGpmQueryDeviceSupport.assert_called_once()
        mock_pynvml.nvmlGpmSampleGet.assert_not_called()
        self.assertEqual(self.provider.drain_collection_errors(), {})

        mock_pynvml.nvmlGpmQueryDeviceSupport.side_effect = AttributeError("no GPM")
        self.provider._gpm_state.clear()
        with patch.object(self.provider, "_shutdown"):
            result = self.provider.get_gpu_utilization()[0]
        self.assertIsNone(result["sm_active_percent"])
        mock_pynvml.nvmlGpmSampleGet.assert_not_called()

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_gpm_support_check_retried_after_transient_error(self, mock_pynvml):
        """A failed support check is counted and retried instead of disabling GPM."""
        self._configure_nvidia_collection(mock_pynvml, ["NVIDIA H100"])
        self._configure_gpm(mock_pynvml, [{2: (25.0, 0), 1: (40.0, 0)}])
        clock = self._gpm_clock()
        mock_pynvml.nvmlGpmQueryDeviceSupport.side_effect = mock_pynvml.NVMLError(
            "Unknown Error"
        )

        with patch.object(self.provider, "_shutdown"):
            self.provider.get_gpu_utilization()
            self.assertEqual(
                self.provider.drain_collection_errors(),
                {("gpm", "MockNVMLError"): 1},
            )
            mock_pynvml.nvmlGpmQueryDeviceSupport.side_effect = None
            self.provider.get_gpu_utilization()
            clock["now"] += 5.0
            result = self.provider.get_gpu_utilization()[0]

        self.assertEqual(result["sm_active_percent"], 25.0)

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_gpm_invalid_values_are_dropped_and_counted(self, mock_pynvml):
        """A failed or out-of-range metric is not reported; SM time is not credited."""
        self._configure_nvidia_collection(mock_pynvml, ["NVIDIA H100"])
        self._configure_gpm(
            mock_pynvml,
            [
                {2: (150.0, 0), 1: (30.0, 0)},  # SM out of range
                {2: (20.0, 0), 1: (0.0, 3)},  # graphics not supported by NVML
                {2: (10.0, 0), 1: (12.0, 0)},
            ],
        )
        clock = self._gpm_clock()

        with patch.object(self.provider, "_shutdown"):
            self.provider.get_gpu_utilization()
            results = []
            for _ in range(3):
                clock["now"] += 5.0
                results.append(self.provider.get_gpu_utilization()[0])

        self.assertIsNone(results[0]["sm_active_percent"])
        self.assertEqual(results[0]["graphics_engine_active_percent"], 30.0)
        # The window restarted after the bad SM value, so 20 % covers only 5 s.
        self.assertEqual(results[1]["sm_active_percent"], 20.0)
        self.assertAlmostEqual(results[1]["sm_active_seconds"], 1.0)
        self.assertIsNone(results[1]["graphics_engine_active_percent"])
        self.assertAlmostEqual(results[2]["sm_active_seconds"], 0.5)
        self.assertEqual(
            self.provider.drain_collection_errors(),
            {("gpm", "out_of_range"): 1, ("gpm", "nvml_return_3"): 1},
        )

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_gpm_repeated_failures_back_off(self, mock_pynvml):
        """Three failed samples in a row skip GPM for the next 10 polls."""
        self._configure_nvidia_collection(mock_pynvml, ["NVIDIA H100"])
        self._configure_gpm(mock_pynvml, [])
        mock_pynvml.nvmlGpmSampleGet.side_effect = mock_pynvml.NVMLError("boom")
        clock = self._gpm_clock()

        with patch.object(self.provider, "_shutdown"):
            for _ in range(13):
                clock["now"] += 5.0
                self.provider.get_gpu_utilization()

        self.assertEqual(mock_pynvml.nvmlGpmSampleGet.call_count, 3)
        self.assertEqual(
            self.provider.drain_collection_errors(), {("gpm", "MockNVMLError"): 3}
        )

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_gpm_skips_samples_closer_than_spacing(self, mock_pynvml):
        """A poll within 200 ms of the window start does not take a GPM sample."""
        self._configure_nvidia_collection(mock_pynvml, ["NVIDIA H100"])
        self._configure_gpm(mock_pynvml, [{2: (50.0, 0), 1: (60.0, 0)}])
        clock = self._gpm_clock()

        with patch.object(self.provider, "_shutdown"):
            self.provider.get_gpu_utilization()
            clock["now"] += 0.1
            skipped = self.provider.get_gpu_utilization()[0]
            clock["now"] += 4.9
            reported = self.provider.get_gpu_utilization()[0]

        self.assertIsNone(skipped["sm_active_percent"])
        self.assertEqual(mock_pynvml.nvmlGpmSampleGet.call_count, 2)
        # The reported window covers both polls.
        self.assertAlmostEqual(reported["sm_active_seconds"], 2.5)

    @patch("ray._private.thirdparty.pynvml", create=True)
    def test_gpm_disabled_makes_no_gpm_calls(self, mock_pynvml):
        """With GPM turned off the provider never touches the GPM API."""
        self.provider = NvidiaGpuProvider(enable_gpm=False)
        self._configure_nvidia_collection(mock_pynvml, ["NVIDIA H100"])
        self._configure_gpm(mock_pynvml, [])

        with patch.object(self.provider, "_shutdown"):
            self.provider.get_gpu_utilization()
            result = self.provider.get_gpu_utilization()[0]

        self.assertIsNone(result["sm_active_percent"])
        mock_pynvml.nvmlGpmQueryDeviceSupport.assert_not_called()
        mock_pynvml.nvmlGpmSampleGet.assert_not_called()


class TestAmdGpuProvider(unittest.TestCase):
    """Test AmdGpuProvider class."""

    def setUp(self):
        """Set up test fixtures."""
        self.provider = AmdGpuProvider()

    def test_get_provider_name(self):
        """Test provider name."""
        self.assertEqual(self.provider.get_provider_name(), GpuProviderType.AMD)

    @patch("ray._private.thirdparty.pyamdsmi", create=True)
    def test_is_available_success(self, mock_pyamdsmi):
        """Test is_available when AMD GPU is available."""
        mock_pyamdsmi.smi_initialize.return_value = None
        mock_pyamdsmi.smi_shutdown.return_value = None

        self.assertTrue(self.provider.is_available())
        mock_pyamdsmi.smi_initialize.assert_called_once()
        mock_pyamdsmi.smi_shutdown.assert_called_once()

    @patch("ray._private.thirdparty.pyamdsmi", create=True)
    def test_is_available_failure(self, mock_pyamdsmi):
        """Test is_available when AMD GPU is not available."""
        mock_pyamdsmi.smi_initialize.side_effect = Exception("AMD driver not found")

        self.assertFalse(self.provider.is_available())

    @patch("ray._private.thirdparty.pyamdsmi", create=True)
    def test_initialize_success(self, mock_pyamdsmi):
        """Test successful initialization."""
        mock_pyamdsmi.smi_initialize.return_value = None

        self.assertTrue(self.provider._initialize())
        self.assertTrue(self.provider._initialized)
        mock_pyamdsmi.smi_initialize.assert_called_once()

    @patch("ray._private.thirdparty.pyamdsmi", create=True)
    def test_get_gpu_utilization_success(self, mock_pyamdsmi):
        """Test successful GPU utilization retrieval."""
        mock_process = Mock()
        mock_process.process_id = 5678
        mock_process.vram_usage = 512 * MB

        # Configure mocks
        mock_pyamdsmi.smi_initialize.return_value = None
        mock_pyamdsmi.smi_get_device_count.return_value = 1
        mock_pyamdsmi.smi_get_device_id.return_value = "device_0"
        mock_pyamdsmi.smi_get_device_utilization.return_value = 85
        mock_pyamdsmi.smi_get_device_compute_process.return_value = [mock_process]
        mock_pyamdsmi.smi_get_compute_process_info_by_device.return_value = [
            mock_process
        ]
        mock_pyamdsmi.smi_get_device_name.return_value = b"AMD Radeon RX 6800 XT"
        mock_pyamdsmi.smi_get_device_unique_id.return_value = (
            "GPU-13579bdf-9abc-def0-0000-000000000000"
        )
        mock_pyamdsmi.smi_get_device_memory_used.return_value = 6 * MB * 1024
        mock_pyamdsmi.smi_get_device_memory_total.return_value = 16 * MB * 1024
        mock_pyamdsmi.smi_shutdown.return_value = None

        # Set up provider state
        self.provider._pyamdsmi = mock_pyamdsmi
        self.provider._initialized = True

        result = self.provider.get_gpu_utilization()

        self.assertEqual(len(result), 1)
        gpu_info = result[0]

        self.assertEqual(gpu_info["index"], 0)
        self.assertEqual(gpu_info["name"], "AMD Radeon RX 6800 XT")
        self.assertEqual(gpu_info["uuid"], "GPU-13579bdf-9abc-def0-0000-000000000000")
        self.assertEqual(gpu_info["utilization_gpu"], 85)
        self.assertEqual(gpu_info["memory_used"], 6 * 1024)  # 6GB in MB
        self.assertEqual(gpu_info["memory_total"], 16 * 1024)  # 16GB in MB
        self.assertEqual(len(gpu_info["processes_pids"]), 1)
        self.assertEqual(gpu_info["processes_pids"][5678]["pid"], 5678)
        self.assertEqual(gpu_info["processes_pids"][5678]["gpu_memory_usage"], 512)

    @patch("ray._private.thirdparty.pyamdsmi", create=True)
    def test_get_gpu_utilization_failure_reports_none(self, mock_pyamdsmi):
        """The shim's -1 (query failed) becomes None, not a utilization value."""
        mock_pyamdsmi.smi_initialize.return_value = None
        mock_pyamdsmi.smi_get_device_count.return_value = 1
        mock_pyamdsmi.smi_get_device_id.return_value = "device_0"
        mock_pyamdsmi.smi_get_device_utilization.return_value = -1
        mock_pyamdsmi.smi_get_device_compute_process.return_value = []
        mock_pyamdsmi.smi_get_compute_process_info_by_device.return_value = []
        mock_pyamdsmi.smi_get_device_name.return_value = b"AMD Instinct MI300X"
        mock_pyamdsmi.smi_get_device_unique_id.return_value = (
            "GPU-13579bdf-9abc-def0-0000-000000000001"
        )
        mock_pyamdsmi.smi_get_device_memory_used.return_value = 6 * MB * 1024
        mock_pyamdsmi.smi_get_device_memory_total.return_value = 16 * MB * 1024
        mock_pyamdsmi.smi_shutdown.return_value = None
        self.provider._pyamdsmi = mock_pyamdsmi
        self.provider._initialized = True

        result = self.provider.get_gpu_utilization()

        self.assertEqual(len(result), 1)
        self.assertIsNone(result[0]["utilization_gpu"])
        self.assertEqual(result[0]["memory_used"], 6 * 1024)

    @patch("ray._private.thirdparty.pyamdsmi", create=True)
    def test_collection_errors_count_failed_utilization_query(self, mock_pyamdsmi):
        """The shim signals a failed utilization query by value; count it."""
        mock_pyamdsmi.smi_initialize.return_value = None
        mock_pyamdsmi.smi_get_device_count.return_value = 1
        mock_pyamdsmi.smi_get_device_utilization.return_value = -1
        mock_pyamdsmi.smi_get_device_compute_process.return_value = []
        mock_pyamdsmi.smi_get_compute_process_info_by_device.return_value = []
        mock_pyamdsmi.smi_get_device_name.return_value = b"AMD Instinct MI300X"
        mock_pyamdsmi.smi_get_device_unique_id.return_value = "GPU-0"
        mock_pyamdsmi.smi_get_device_memory_used.return_value = MB
        mock_pyamdsmi.smi_get_device_memory_total.return_value = 2 * MB
        mock_pyamdsmi.smi_get_device_average_power.return_value = 100.0
        mock_pyamdsmi.smi_shutdown.return_value = None
        self.provider._pyamdsmi = mock_pyamdsmi
        self.provider._initialized = True

        self.provider.get_gpu_utilization()

        self.assertEqual(
            self.provider.drain_collection_errors(),
            {("utilization", "query_failed"): 1},
        )


class TestGpuMetricProvider(unittest.TestCase):
    """Test GpuMetricProvider class."""

    def setUp(self):
        """Set up test fixtures."""
        self.provider = GpuMetricProvider()

    def test_init(self):
        """Test GpuMetricProvider initialization."""
        self.assertIsNone(self.provider._provider)
        self.assertTrue(self.provider._enable_metric_report)
        self.assertEqual(len(self.provider._providers), 2)
        self.assertFalse(self.provider._initialized)

    @patch.object(GpuMetricProvider, "_detect_gpu_provider")
    def test_initialize_disabled(self, mock_detect):
        """Test disabled GPU metrics do not probe GPU providers."""
        with patch(
            "ray.dashboard.modules.reporter.gpu_providers.NvidiaGpuProvider"
        ) as mock_nvidia_provider, patch(
            "ray.dashboard.modules.reporter.gpu_providers.AmdGpuProvider"
        ) as mock_amd_provider:
            provider = GpuMetricProvider(enable_metric_report=False)

        self.assertFalse(provider.initialize())
        self.assertFalse(provider.initialize())
        self.assertEqual(provider.get_gpu_usage(), [])
        self.assertEqual(provider._providers, [])
        self.assertTrue(provider._initialized)
        self.assertFalse(provider.is_metric_report_enabled())
        mock_nvidia_provider.assert_not_called()
        mock_amd_provider.assert_not_called()
        mock_detect.assert_not_called()

    @patch.object(NvidiaGpuProvider, "is_available", return_value=True)
    @patch.object(AmdGpuProvider, "is_available", return_value=False)
    def test_detect_gpu_provider_nvidia(
        self, mock_amd_available, mock_nvidia_available
    ):
        """Test GPU provider detection when NVIDIA is available."""
        provider = self.provider._detect_gpu_provider()

        self.assertIsInstance(provider, NvidiaGpuProvider)
        mock_nvidia_available.assert_called_once()

    @patch.object(NvidiaGpuProvider, "is_available", return_value=False)
    @patch.object(AmdGpuProvider, "is_available", return_value=True)
    def test_detect_gpu_provider_amd(self, mock_amd_available, mock_nvidia_available):
        """Test GPU provider detection when AMD is available."""
        provider = self.provider._detect_gpu_provider()

        self.assertIsInstance(provider, AmdGpuProvider)
        mock_nvidia_available.assert_called_once()
        mock_amd_available.assert_called_once()

    @patch.object(NvidiaGpuProvider, "is_available", return_value=False)
    @patch.object(AmdGpuProvider, "is_available", return_value=False)
    def test_detect_gpu_provider_none(self, mock_amd_available, mock_nvidia_available):
        """Test GPU provider detection when no GPUs are available."""
        provider = self.provider._detect_gpu_provider()

        self.assertIsNone(provider)

    @patch("subprocess.check_output")
    def test_should_disable_gpu_check_true(self, mock_subprocess):
        """Test should_disable_gpu_check returns True for specific conditions."""
        mock_subprocess.return_value = ""  # Empty result means AMD GPU module not live

        class MockNVMLError(Exception):
            pass

        MockNVMLError.__name__ = "NVMLError_DriverNotLoaded"

        error = MockNVMLError("NVIDIA driver not loaded")

        result = self.provider._should_disable_gpu_check(error)
        self.assertTrue(result)

    @patch("subprocess.check_output")
    def test_should_disable_gpu_check_false_wrong_error(self, mock_subprocess):
        """Test should_disable_gpu_check returns False for wrong error type."""
        mock_subprocess.return_value = ""

        error = Exception("Some other error")

        result = self.provider._should_disable_gpu_check(error)
        self.assertFalse(result)

    @patch("subprocess.check_output")
    def test_should_disable_gpu_check_false_amd_present(self, mock_subprocess):
        """Test should_disable_gpu_check returns False when AMD GPU is present."""
        mock_subprocess.return_value = "live"  # AMD GPU module is live

        class MockNVMLError(Exception):
            pass

        MockNVMLError.__name__ = "NVMLError_DriverNotLoaded"

        error = MockNVMLError("NVIDIA driver not loaded")

        result = self.provider._should_disable_gpu_check(error)
        self.assertFalse(result)

    def test_get_gpu_usage_disabled(self):
        """Test get_gpu_usage when GPU usage check is disabled."""
        self.provider._enable_metric_report = False

        result = self.provider.get_gpu_usage()
        self.assertEqual(result, [])

    @patch.object(GpuMetricProvider, "_detect_gpu_provider")
    def test_get_gpu_usage_no_provider(self, mock_detect):
        """Test get_gpu_usage when no GPU provider is available."""
        mock_detect.return_value = None

        with patch.object(
            NvidiaGpuProvider, "_initialize", side_effect=Exception("No GPU")
        ):
            result = self.provider.get_gpu_usage()

        self.assertEqual(result, [])
        self.provider._initialized = False  # Reset for clean test
        mock_detect.assert_called_once()

    @patch.object(GpuMetricProvider, "_detect_gpu_provider")
    def test_get_gpu_usage_success(self, mock_detect):
        """Test successful get_gpu_usage."""
        mock_provider = Mock()
        mock_provider.get_gpu_utilization.return_value = [
            GpuUtilizationInfo(
                index=0,
                name="Test GPU",
                uuid="test-uuid",
                utilization_gpu=50,
                memory_used=1024,
                memory_total=2048,
                processes_pids={
                    1234: ProcessGPUInfo(
                        pid=1234, gpu_memory_usage=1024, gpu_utilization=None
                    )
                },
            )
        ]
        mock_detect.return_value = mock_provider

        result = self.provider.get_gpu_usage()

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["index"], 0)
        self.assertEqual(result[0]["name"], "Test GPU")
        mock_provider.get_gpu_utilization.assert_called_once()

    def test_gpm_switch_reaches_nvidia_provider(self):
        """enable_gpm_metrics is passed to the NVIDIA provider."""
        self.assertTrue(GpuMetricProvider()._providers[0]._enable_gpm)
        self.assertFalse(
            GpuMetricProvider(enable_gpm_metrics=False)._providers[0]._enable_gpm
        )

    def test_drain_collection_errors_no_provider(self):
        """Without a provider there is nothing to drain."""
        self.provider._provider = None
        self.assertEqual(self.provider.drain_collection_errors(), {})

    def test_get_provider_name_no_provider(self):
        """Test get_provider_name when no provider is set."""
        result = self.provider.get_provider_name()
        self.assertIsNone(result)

    def test_get_provider_name_with_provider(self):
        """Test get_provider_name when provider is set."""
        mock_provider = Mock()
        mock_provider.get_provider_name.return_value = GpuProviderType.NVIDIA
        self.provider._provider = mock_provider

        result = self.provider.get_provider_name()
        self.assertEqual(result, "nvidia")

    def test_is_metric_report_enabled(self):
        """Test is_metric_report_enabled."""
        self.assertTrue(self.provider.is_metric_report_enabled())

        self.provider._enable_metric_report = False
        self.assertFalse(self.provider.is_metric_report_enabled())


if __name__ == "__main__":
    unittest.main()
