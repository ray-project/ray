"""Unit tests for GPU providers."""

import ctypes
import os
import sys
import unittest
from unittest.mock import Mock, call, patch

from ray.dashboard.modules.reporter.gpu_providers import (
    MB,
    RAY_SKIP_PROCESS_UTIL_API,
    RAY_SKIP_PROCESS_UTIL_API_DEVICE_NAMES,
    AmdGpuProvider,
    GpuMetricProvider,
    GpuProvider,
    GpuProviderType,
    GpuUtilizationInfo,
    IntelGpuProvider,
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
        mock_pynvml.nvmlDeviceGetUUID.side_effect = lambda handle: (
            f"GPU-{gpu_indices[handle]}".encode()
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

        self.provider._pynvml = mock_pynvml
        self.provider._initialized = True
        return handles

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
        self.assertEqual(gpu_info["utilization_gpu"], -1)  # Should be -1 due to error
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


def _make_fake_pyzes():
    """A minimal in-memory stand-in for ``pyzes`` (Level Zero Sysman).

    One device with 8 GiB of device memory (2 GiB used), one COMPUTE_ALL engine group,
    one card-level power domain and one GPU temperature sensor. Engine activity
    and energy advance by a fixed step on every read.
    """
    z = type(sys)("pyzes")
    z.ZE_RESULT_SUCCESS = 0
    for name in (
        "driver_handle",
        "device_handle",
        "mem_handle",
        "pwr_handle",
        "temp_handle",
        "engine_handle",
    ):
        setattr(z, f"zes_{name}_t", type(f"zes_{name}_t", (ctypes.c_void_p,), {}))

    def struct(name, fields):
        setattr(
            z,
            name,
            type(name, (ctypes.Structure,), {"_fields_": list(fields)}),
        )

    u32, u64, i32 = ctypes.c_uint32, ctypes.c_uint64, ctypes.c_int32
    uuid = ctypes.c_ubyte * 16
    str_ = ctypes.c_char * 32
    struct("_uuid", [("id", uuid)])
    struct("_core", [("name", str_), ("uuid", z._uuid)])
    struct(
        "zes_device_properties_t",
        [
            ("stype", i32),
            ("pNext", ctypes.c_void_p),
            ("core", z._core),
            ("modelName", str_),
        ],
    )
    struct(
        "_pci_address",
        [("domain", u32), ("bus", u32), ("device", u32), ("function", u32)],
    )
    struct(
        "zes_pci_properties_t",
        [("stype", i32), ("pNext", ctypes.c_void_p), ("address", z._pci_address)],
    )
    struct(
        "zes_mem_properties_t",
        [("stype", i32), ("pNext", ctypes.c_void_p), ("location", i32)],
    )
    struct(
        "zes_mem_state_t",
        [("stype", i32), ("pNext", ctypes.c_void_p), ("free", u64), ("size", u64)],
    )
    struct(
        "zes_engine_properties_t",
        [("stype", i32), ("pNext", ctypes.c_void_p), ("type", i32)],
    )
    struct("zes_engine_stats_t", [("activeTime", u64), ("timestamp", u64)])
    struct(
        "zes_power_properties_t",
        [("stype", i32), ("pNext", ctypes.c_void_p), ("onSubdevice", ctypes.c_uint8)],
    )
    struct(
        "zes_power_ext_properties_t",
        [("stype", i32), ("pNext", ctypes.c_void_p), ("domain", i32)],
    )
    struct("zes_power_energy_counter_t", [("energy", u64), ("timestamp", u64)])
    struct(
        "zes_temp_properties_t",
        [("stype", i32), ("pNext", ctypes.c_void_p), ("type", i32)],
    )
    struct(
        "zes_process_state_t",
        [
            ("stype", i32),
            ("pNext", ctypes.c_void_p),
            ("processId", u32),
            ("memSize", u64),
        ],
    )
    for const, value in {
        "ZES_STRUCTURE_TYPE_DEVICE_PROPERTIES": 1,
        "ZES_STRUCTURE_TYPE_PCI_PROPERTIES": 2,
        "ZES_STRUCTURE_TYPE_MEM_PROPERTIES": 0xB,
        "ZES_STRUCTURE_TYPE_MEM_STATE": 0x1E,
        "ZES_STRUCTURE_TYPE_ENGINE_PROPERTIES": 5,
        "ZES_STRUCTURE_TYPE_POWER_PROPERTIES": 0xD,
        "ZES_STRUCTURE_TYPE_POWER_EXT_PROPERTIES": 0x28,
        "ZES_POWER_DOMAIN_CARD": 1,
        "ZES_POWER_DOMAIN_PACKAGE": 2,
        "ZES_STRUCTURE_TYPE_TEMP_PROPERTIES": 0x14,
        "ZES_STRUCTURE_TYPE_PROCESS_STATE": 0x16,
        "ZES_MEM_LOC_DEVICE": 1,
        "ZES_ENGINE_GROUP_ALL": 0,
        "ZES_ENGINE_GROUP_COMPUTE_ALL": 1,
        "ZES_TEMP_SENSORS_GPU": 1,
    }.items():
        setattr(z, const, value)

    clock = {"n": 0}

    def fill(count, handles, cls, n):
        count._obj.value = n
        if handles is not None:
            for i in range(n):
                handles[i] = cls(i + 1)
        return 0

    z.zesInit = lambda flags: 0
    z.zesDriverGet = lambda count, h: fill(count, h, z.zes_driver_handle_t, 1)
    z.zesDeviceGet = lambda d, count, h: fill(count, h, z.zes_device_handle_t, 1)
    z.zesDeviceEnumMemoryModules = lambda d, count, h: fill(
        count, h, z.zes_mem_handle_t, 2
    )
    z.zesDeviceEnumEngineGroups = lambda d, count, h: fill(
        count, h, z.zes_engine_handle_t, 1
    )
    z.zesDeviceEnumPowerDomains = lambda d, count, h: fill(
        count, h, z.zes_pwr_handle_t, 1
    )
    z.zesDeviceEnumTemperatureSensors = lambda d, count, h: fill(
        count, h, z.zes_temp_handle_t, 1
    )

    def device_props(d, p):
        p._obj.modelName = b"Intel(R) Arc(TM) Pro B60"
        return 0

    z.zesDeviceGetProperties = device_props

    def mem_props(m, p):
        # Module 1 is device memory, module 2 is host-visible and must be ignored.
        p._obj.location = 1 if m.value == 1 else 2
        return 0

    z.zesMemoryGetProperties = mem_props

    def mem_state(m, s):
        s._obj.size = 8 * 1024 * MB
        s._obj.free = 6 * 1024 * MB
        return 0

    z.zesMemoryGetState = mem_state

    def engine_props(e, p):
        p._obj.type = z.ZES_ENGINE_GROUP_COMPUTE_ALL
        return 0

    z.zesEngineGetProperties = engine_props

    def pci_props(d, p):
        # One bus address per device handle, in enumeration order.
        p._obj.address.bus = d.value
        return 0

    z.zesDevicePciGetProperties = pci_props

    def activity(e, s):
        clock["n"] += 1
        s._obj.timestamp = clock["n"] * 1_000_000
        s._obj.activeTime = clock["n"] * 250_000  # 25% busy
        return 0

    z.zesEngineGetActivity = activity
    z.zesPowerGetProperties = lambda p, props: 0

    def energy(p, c):
        clock["e"] = clock.get("e", 0) + 1
        c._obj.timestamp = clock["e"] * 1_000_000
        c._obj.energy = clock["e"] * 80_000_000  # 80 W
        return 0

    z.zesPowerGetEnergyCounter = energy

    def temp_props(t, p):
        p._obj.type = 1
        return 0

    z.zesTemperatureGetProperties = temp_props

    def temp_state(t, v):
        v._obj.value = 61.5
        return 0

    z.zesTemperatureGetState = temp_state

    def processes(d, count, states):
        count._obj.value = 1
        if states is not None:
            states[0].processId = 4242
            states[0].memSize = 512 * MB
        return 0

    z.zesDeviceProcessesGetState = processes
    return z


_PYZES = "ray._private.thirdparty.pyzes"


def _fill(count, handles, cls, n):
    count._obj.value = n
    if handles is not None:
        for i in range(n):
            handles[i] = cls(i + 1)
    return 0


def _fill_handles(fake, count, handles, n):
    """Mimic the two-call Level Zero enumeration for ``n`` engine groups."""
    count._obj.value = n
    if handles is not None:
        for i in range(n):
            handles[i] = fake.zes_engine_handle_t(i + 1)
    return 0


class TestIntelGpuProvider(unittest.TestCase):
    """Test IntelGpuProvider class against a fake pyzes."""

    def setUp(self):
        self.provider = IntelGpuProvider()
        self.fake = _make_fake_pyzes()
        # The real pyzes can't be imported without libze_loader, so register
        # the fake both as the module and as the package attribute.
        for patcher in (
            patch.dict(sys.modules, {_PYZES: self.fake}),
            patch(_PYZES, self.fake, create=True),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_get_provider_name(self):
        self.assertEqual(self.provider.get_provider_name(), GpuProviderType.INTEL)

    def test_is_available_success(self):
        self.assertTrue(self.provider.is_available())

    def test_is_available_init_failure(self):
        self.fake.zesInit = lambda flags: 1
        self.assertFalse(self.provider.is_available())

    def test_is_available_no_devices(self):
        self.fake.zesDeviceGet = lambda d, count, h: (
            setattr(count._obj, "value", 0) or 0
        )
        self.assertFalse(self.provider.is_available())

    def test_initialize_failure(self):
        self.fake.zesInit = lambda flags: 1
        self.assertFalse(self.provider._initialize())
        self.assertFalse(self.provider._initialized)
        self.assertEqual(self.provider.get_gpu_utilization(), [])

    def test_get_gpu_utilization_first_sample(self):
        result = self.provider.get_gpu_utilization()

        self.assertEqual(len(result), 1)
        gpu = result[0]
        self.assertEqual(gpu["index"], 0)
        self.assertEqual(gpu["name"], "Intel(R) Arc(TM) Pro B60")
        # Only the device-local memory module counts: 8 GiB total, 2 GiB used.
        self.assertEqual(gpu["memory_total"], 8 * 1024)
        self.assertEqual(gpu["memory_used"], 2 * 1024)
        # Counter-derived values need a previous sample.
        self.assertIsNone(gpu["utilization_gpu"])
        self.assertIsNone(gpu["power_mw"])
        self.assertEqual(gpu["temperature_c"], 61)
        self.assertEqual(
            gpu["processes_pids"],
            {
                4242: ProcessGPUInfo(
                    pid=4242, gpu_memory_usage=512, gpu_utilization=None
                )
            },
        )

    def test_get_gpu_utilization_second_sample(self):
        self.provider.get_gpu_utilization()
        gpu = self.provider.get_gpu_utilization()[0]

        self.assertEqual(gpu["utilization_gpu"], 25)
        self.assertEqual(gpu["power_mw"], 80_000)

    def test_no_engine_groups_leaves_utilization_unset(self):
        self.fake.zesDeviceEnumEngineGroups = lambda d, count, h: (
            setattr(count._obj, "value", 0) or 0
        )
        self.provider.get_gpu_utilization()
        gpu = self.provider.get_gpu_utilization()[0]

        self.assertIsNone(gpu["utilization_gpu"])
        # The other fields are unaffected.
        self.assertEqual(gpu["power_mw"], 80_000)
        self.assertEqual(gpu["memory_total"], 8 * 1024)

    def test_only_compute_engine_group_is_reported(self):
        """An ``ALL`` group averaging in idle copy/media engines must be ignored."""
        self.fake.zesDeviceEnumEngineGroups = lambda d, count, h: (
            _fill_handles(self.fake, count, h, 2)
        )

        def engine_props(e, p):
            # Handle 1 is the 100%-busy compute group, handle 2 the ALL average.
            p._obj.type = 1 if e.value == 1 else 0
            return 0

        def activity(e, s):
            self.clock = getattr(self, "clock", 0) + 1
            s._obj.timestamp = self.clock * 1_000_000
            busy = 1_000_000 if e.value == 1 else 110_000
            s._obj.activeTime = self.clock * busy
            return 0

        self.fake.zesEngineGetProperties = engine_props
        self.fake.zesEngineGetActivity = activity
        self.provider.get_gpu_utilization()
        gpu = self.provider.get_gpu_utilization()[0]

        self.assertEqual(gpu["utilization_gpu"], 100)

    def test_only_all_engine_group_leaves_utilization_unset(self):
        self.fake.zesEngineGetProperties = lambda e, p: (
            setattr(p._obj, "type", 0) or 0
        )
        self.provider.get_gpu_utilization()
        gpu = self.provider.get_gpu_utilization()[0]

        self.assertIsNone(gpu["utilization_gpu"])

    def _two_devices(self, bus_of):
        """Install two devices; ``bus_of`` maps device handle value -> PCI bus."""
        self.fake.zesDeviceGet = lambda d, count, h: _fill(
            count, h, self.fake.zes_device_handle_t, len(bus_of)
        )

        def pci_props(d, p):
            if bus_of[d.value] is None:
                return 1
            p._obj.address.bus = bus_of[d.value]
            return 0

        self.fake.zesDevicePciGetProperties = pci_props

        def device_props(d, p):
            p._obj.modelName = f"gpu{d.value}".encode()
            return 0

        self.fake.zesDeviceGetProperties = device_props

    def test_devices_are_indexed_in_pci_order(self):
        # Sysman lists the bus 0x1b card first; PCI order puts it last.
        self._two_devices({1: 0x1B, 2: 0x10})
        names = [g["name"] for g in self.provider.get_gpu_utilization()]

        self.assertEqual(names, ["gpu2", "gpu1"])

    def test_unreadable_pci_address_keeps_order_and_sorts_last(self):
        self._two_devices({1: None, 2: 0x10})
        names = [g["name"] for g in self.provider.get_gpu_utilization()]

        self.assertEqual(names, ["gpu2", "gpu1"])

    def test_engine_utilization_is_averaged_per_engine_interval(self):
        """Two 25%-busy engines sampled over 1 s and 2 s must still read 25%."""
        # engine handle -> (activeTime, timestamp) of the current sample, in us.
        state = {1: (0, 0), 2: (0, 0)}

        self.fake.zesDeviceEnumEngineGroups = lambda d, count, h: (
            _fill_handles(self.fake, count, h, 2)
        )

        def activity(e, s):
            s._obj.activeTime, s._obj.timestamp = state[e.value]
            return 0

        self.fake.zesEngineGetActivity = activity

        def sample(engine1, engine2):
            state[1], state[2] = engine1, engine2
            return self.provider.get_gpu_utilization()[0]["utilization_gpu"]

        self.assertIsNone(sample((0, 0), (0, 0)))
        # Engine 1 advanced 1 s (250 ms busy), engine 2 advanced 2 s (500 ms busy).
        self.assertEqual(sample((250_000, 1_000_000), (500_000, 2_000_000)), 25)

    def _power_domains(self, domains, watts):
        """Install root power domains: ``domains`` maps handle value -> domain type."""
        state = {"tick": 0}

        def enum(d, count, handles):
            count._obj.value = len(domains)
            if handles is not None:
                for i, value in enumerate(domains):
                    handles[i] = self.fake.zes_pwr_handle_t(value)
            return 0

        def props(p, out):
            ext = ctypes.cast(
                out._obj.pNext, ctypes.POINTER(self.fake.zes_power_ext_properties_t)
            )
            ext.contents.domain = domains[p.value]
            return 0

        def energy(p, c):
            c._obj.timestamp = state["tick"] * 1_000_000
            c._obj.energy = state["tick"] * watts[p.value] * 1_000_000
            return 0

        self.fake.zesDeviceEnumPowerDomains = enum
        self.fake.zesPowerGetProperties = props
        self.fake.zesPowerGetEnergyCounter = energy

        def sample(tick):
            state["tick"] = tick
            return self.provider.get_gpu_utilization()[0]["power_mw"]

        return sample

    def test_power_prefers_card_domain_over_package_first(self):
        # Package is enumerated first (80 W), the 120 W card domain second.
        sample = self._power_domains({1: 2, 2: 1}, {1: 80, 2: 120})

        self.assertIsNone(sample(1))
        self.assertEqual(sample(2), 120_000)

    def test_power_falls_back_to_package_without_card_domain(self):
        # Unknown (0) first, package second: package wins; no card telemetry.
        sample = self._power_domains({1: 0, 2: 2}, {1: 50, 2: 80})

        self.assertIsNone(sample(1))
        self.assertEqual(sample(2), 80_000)

    def test_power_domain_switch_does_not_mix_energy_counters(self):
        """A skipped first domain must not make the second diff against its counter."""
        state = {"tick": 0, "props_fail": False}

        def domains(d, count, handles):
            count._obj.value = 2
            if handles is not None:
                for i in range(2):
                    handles[i] = self.fake.zes_pwr_handle_t(i + 1)
            return 0

        self.fake.zesDeviceEnumPowerDomains = domains

        def props(p, out):
            return 1 if state["props_fail"] and p.value == 1 else 0

        def energy(p, c):
            c._obj.timestamp = state["tick"] * 1_000_000
            # Domain 1 draws 80 W; domain 2 is an unrelated counter with a huge offset.
            watts, offset = (80, 0) if p.value == 1 else (40, 5_000_000_000)
            c._obj.energy = offset + state["tick"] * watts * 1_000_000
            return 0

        self.fake.zesPowerGetProperties = props
        self.fake.zesPowerGetEnergyCounter = energy

        def sample(tick, props_fail=False):
            state.update(tick=tick, props_fail=props_fail)
            return self.provider.get_gpu_utilization()[0]["power_mw"]

        self.assertIsNone(sample(1))  # baseline for domain 1
        self.assertIsNone(sample(2, props_fail=True))  # domain 2 has no baseline yet
        self.assertEqual(sample(3), 80_000)  # domain 1 again, over two ticks

    def test_partial_engine_read_failure_resets_utilization_baseline(self):
        """A failed read on one of several compute groups must not skew the next value."""
        state = {"tick": 0, "failing": None}

        def activity(e, s):
            if e.value == state["failing"]:
                return 1
            s._obj.timestamp = state["tick"] * 1_000_000
            s._obj.activeTime = state["tick"] * 250_000  # 25% busy
            return 0

        self.fake.zesDeviceEnumEngineGroups = lambda d, count, h: (
            _fill_handles(self.fake, count, h, 2)
        )
        self.fake.zesEngineGetActivity = activity

        def sample(tick, failing=None):
            state.update(tick=tick, failing=failing)
            return self.provider.get_gpu_utilization()[0]["utilization_gpu"]

        self.assertIsNone(sample(1))  # first sample only sets the baseline
        self.assertEqual(sample(2), 25)
        # Engine 2 fails: the sum would cover engine 1 only, so report nothing.
        self.assertIsNone(sample(3, failing=2))
        # Recovery has no baseline to diff against yet, so no spike either.
        self.assertIsNone(sample(4))
        self.assertEqual(sample(5), 25)

    def test_optional_field_failure_keeps_device(self):
        def boom(*args):
            raise RuntimeError("sensor gone")

        self.fake.zesTemperatureGetState = boom
        gpu = self.provider.get_gpu_utilization()[0]

        self.assertIsNone(gpu["temperature_c"])
        self.assertEqual(gpu["memory_total"], 8 * 1024)

    def test_shutdown_resets_state(self):
        self.provider.get_gpu_utilization()
        self.provider.get_gpu_utilization()
        self.provider._shutdown()

        self.assertFalse(self.provider._initialized)
        self.assertIsNone(self.provider.get_gpu_utilization()[0]["utilization_gpu"])

    def test_metric_provider_detects_intel(self):
        metric_provider = GpuMetricProvider()
        metric_provider._providers = [IntelGpuProvider()]

        self.assertTrue(metric_provider.initialize())
        self.assertEqual(metric_provider.get_provider_name(), "intel")


class TestIntelGpuProviderWithoutDriver(unittest.TestCase):
    """Import the real vendored pyzes on a host with no libze_loader."""

    def setUp(self):
        # Force a fresh import even if pyzes was already loaded in this process.
        patcher = patch.dict(sys.modules)
        patcher.start()
        self.addCleanup(patcher.stop)
        for name in (_PYZES, f"{_PYZES}.pyzes"):
            sys.modules.pop(name, None)
        patcher = patch(
            "ctypes.CDLL", side_effect=OSError("cannot open shared object file")
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_is_available_without_driver(self):
        self.assertFalse(IntelGpuProvider().is_available())

    def test_initialize_without_driver(self):
        provider = IntelGpuProvider()

        self.assertFalse(provider._initialize())
        self.assertEqual(provider.get_gpu_utilization(), [])


class TestGpuMetricProvider(unittest.TestCase):
    """Test GpuMetricProvider class."""

    def setUp(self):
        """Set up test fixtures."""
        self.provider = GpuMetricProvider()

    def test_init(self):
        """Test GpuMetricProvider initialization."""
        self.assertIsNone(self.provider._provider)
        self.assertTrue(self.provider._enable_metric_report)
        self.assertEqual(len(self.provider._providers), 3)
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
    @patch.object(IntelGpuProvider, "is_available", return_value=True)
    def test_detect_gpu_provider_intel(
        self, mock_intel_available, mock_amd_available, mock_nvidia_available
    ):
        """Test GPU provider detection when Intel is available."""
        provider = self.provider._detect_gpu_provider()

        self.assertIsInstance(provider, IntelGpuProvider)
        mock_nvidia_available.assert_called_once()
        mock_amd_available.assert_called_once()
        mock_intel_available.assert_called_once()

    @patch.object(NvidiaGpuProvider, "is_available", return_value=False)
    @patch.object(AmdGpuProvider, "is_available", return_value=False)
    @patch.object(IntelGpuProvider, "is_available", return_value=False)
    def test_detect_gpu_provider_none(
        self, mock_intel_available, mock_amd_available, mock_nvidia_available
    ):
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
