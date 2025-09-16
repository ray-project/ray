"""Unit tests for GPU providers."""

import unittest
from unittest.mock import Mock, patch

from ray.dashboard.modules.reporter.gpu_providers import (
    MB,
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

        # Configure mocks
        mock_pynvml.nvmlInit.return_value = None
        mock_pynvml.nvmlDeviceGetCount.return_value = 1
        mock_pynvml.nvmlDeviceGetHandleByIndex.return_value = mock_handle
        mock_pynvml.nvmlDeviceGetMemoryInfo.return_value = mock_memory_info
        mock_pynvml.nvmlDeviceGetUtilizationRates.return_value = mock_utilization_info
        mock_pynvml.nvmlDeviceGetComputeRunningProcesses.return_value = [mock_process]
        mock_pynvml.nvmlDeviceGetGraphicsRunningProcesses.return_value = []
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
        mock_pyamdsmi.smi_get_device_unique_id.return_value = 0x123456789ABCDEF0
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
        self.assertEqual(gpu_info["uuid"], hex(0x123456789ABCDEF0))
        self.assertEqual(gpu_info["utilization_gpu"], 85)
        self.assertEqual(gpu_info["memory_used"], 6 * 1024)  # 6GB in MB
        self.assertEqual(gpu_info["memory_total"], 16 * 1024)  # 16GB in MB
        self.assertEqual(len(gpu_info["processes_pids"]), 1)
        self.assertEqual(gpu_info["processes_pids"][5678]["pid"], 5678)
        self.assertEqual(gpu_info["processes_pids"][5678]["gpu_memory_usage"], 512)


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
