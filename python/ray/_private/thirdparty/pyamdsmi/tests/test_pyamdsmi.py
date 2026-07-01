"""
Integration tests for the pyamdsmi wrapper functions.

Run on a machine with AMD GPU(s):
    python3 -m unittest discover \
        -s python/ray/_private/thirdparty/pyamdsmi/tests \
        -p "test_pyamdsmi.py" -v
"""

import importlib.util
import pathlib
import unittest

_mod_path = pathlib.Path(__file__).parent.parent / "pyamdsmi.py"
_spec = importlib.util.spec_from_file_location("pyamdsmi", _mod_path)
pyamdsmi = importlib.util.module_from_spec(_spec)
try:
    _spec.loader.exec_module(pyamdsmi)
    _IMPORT_ERROR = None
except Exception as e:
    _IMPORT_ERROR = e


def _try_init():
    """Attempt to initialize SMI. Returns (success, device_count)."""
    if _IMPORT_ERROR:
        return False, 0
    try:
        pyamdsmi.smi_initialize()
        count = pyamdsmi.smi_get_device_count()
        if count <= 0:
            return False, 0
        return True, count
    except Exception:
        return False, 0


_SMI_OK, _DEVICE_COUNT = _try_init()

_SKIP_REASON = (
    f"amdsmi import failed: {_IMPORT_ERROR}" if _IMPORT_ERROR
    else "No AMD GPU or amd-smi library available"
)


def skipNoGpu(fn):
    return unittest.skipUnless(_SMI_OK, _SKIP_REASON)(fn)


def _fmt(value):
    """Human-readable value or 'not supported' for -1."""
    if value == -1:
        return "not supported"
    return repr(value)


class PyAmdSmiTestBase(unittest.TestCase):
    """Base class that ensures smi_initialize() is called before each test class
    and smi_shutdown() after. This prevents test_initialize_and_shutdown from
    leaving _handles empty for all subsequent classes."""

    @classmethod
    def setUpClass(cls):
        if _SMI_OK:
            pyamdsmi.smi_initialize()

    @classmethod
    def tearDownClass(cls):
        if _SMI_OK:
            pyamdsmi.smi_shutdown()


class TestSmiLibraryInit(PyAmdSmiTestBase):

    @skipNoGpu
    def test_initialize_and_shutdown(self):
        """smi_initialize / smi_shutdown should not raise."""
        pyamdsmi.smi_shutdown()
        pyamdsmi.smi_initialize()  # restore for remaining tests in this class

    @skipNoGpu
    def test_device_count_is_positive(self):
        count = pyamdsmi.smi_get_device_count()
        print(f"\n  device_count: {count}")
        self.assertIsInstance(count, int)
        self.assertGreater(count, 0)

    @skipNoGpu
    def test_kernel_version_is_string(self):
        ver = pyamdsmi.smi_get_kernel_version()
        print(f"\n  kernel_version: {ver!r}")
        self.assertIsInstance(ver, str)


class TestSmiPerDeviceIdentity(PyAmdSmiTestBase):
    """Identity fields: id, name, uuid."""

    @skipNoGpu
    def test_device_id_is_nonnegative_int(self):
        for dev in range(_DEVICE_COUNT):
            dev_id = pyamdsmi.smi_get_device_id(dev)
            print(f"\n  dev{dev} device_id: {dev_id:#x}")
            with self.subTest(dev=dev):
                self.assertIsInstance(dev_id, int)
                self.assertGreaterEqual(dev_id, 0)

    @skipNoGpu
    def test_device_name_is_nonempty_string(self):
        for dev in range(_DEVICE_COUNT):
            name = pyamdsmi.smi_get_device_name(dev)
            print(f"\n  dev{dev} name: {name!r}")
            with self.subTest(dev=dev):
                self.assertIsInstance(name, str)
                self.assertGreater(len(name), 0)

    @skipNoGpu
    def test_device_unique_id_is_nonempty_string(self):
        for dev in range(_DEVICE_COUNT):
            uid = pyamdsmi.smi_get_device_unique_id(dev)
            print(f"\n  dev{dev} unique_id: {uid!r}")
            with self.subTest(dev=dev):
                self.assertIsInstance(uid, str)
                self.assertGreater(len(uid), 0)


class TestSmiPerDeviceUtilization(PyAmdSmiTestBase):

    @skipNoGpu
    def test_gpu_utilization_is_valid_percent(self):
        for dev in range(_DEVICE_COUNT):
            util = pyamdsmi.smi_get_device_utilization(dev)
            print(f"\n  dev{dev} gpu_utilization: {util}%")
            with self.subTest(dev=dev):
                self.assertIsInstance(util, int)
                self.assertGreaterEqual(util, 0)
                self.assertLessEqual(util, 100)

    @skipNoGpu
    def test_memory_busy_is_valid_percent_or_not_supported(self):
        for dev in range(_DEVICE_COUNT):
            busy = pyamdsmi.smi_get_device_memory_busy(dev)
            print(f"\n  dev{dev} memory_busy: {_fmt(busy)}")
            with self.subTest(dev=dev):
                self.assertIsInstance(busy, int)
                self.assertTrue(
                    busy == -1 or (0 <= busy <= 100),
                    f"Expected 0-100 or -1 (not supported), got {busy}"
                )


class TestSmiPerDeviceMemory(PyAmdSmiTestBase):

    @skipNoGpu
    def test_memory_used_is_nonnegative_int(self):
        for dev in range(_DEVICE_COUNT):
            used = pyamdsmi.smi_get_device_memory_used(dev)
            print(f"\n  dev{dev} memory_used: {used} bytes ({used // 1024 // 1024} MB)")
            with self.subTest(dev=dev):
                self.assertIsInstance(used, int)
                self.assertGreaterEqual(used, 0)

    @skipNoGpu
    def test_memory_total_is_positive_int(self):
        for dev in range(_DEVICE_COUNT):
            total = pyamdsmi.smi_get_device_memory_total(dev)
            print(f"\n  dev{dev} memory_total: {total} bytes ({total // 1024 // 1024} MB)")
            with self.subTest(dev=dev):
                self.assertIsInstance(total, int)
                self.assertGreater(total, 0)

    @skipNoGpu
    def test_memory_used_does_not_exceed_total(self):
        for dev in range(_DEVICE_COUNT):
            used = pyamdsmi.smi_get_device_memory_used(dev)
            total = pyamdsmi.smi_get_device_memory_total(dev)
            print(f"\n  dev{dev} memory_used/total: {used}/{total} bytes")
            with self.subTest(dev=dev):
                self.assertLessEqual(used, total)

    @skipNoGpu
    def test_memory_reserved_pages_is_tuple_or_not_supported(self):
        for dev in range(_DEVICE_COUNT):
            result = pyamdsmi.smi_get_device_memory_reserved_pages(dev)
            print(f"\n  dev{dev} memory_reserved_pages: {_fmt(result)}")
            with self.subTest(dev=dev):
                self.assertTrue(
                    result == -1 or isinstance(result, tuple),
                    f"Expected tuple or -1, got {type(result)}"
                )


class TestSmiPerDevicePower(PyAmdSmiTestBase):

    @skipNoGpu
    def test_average_power_is_nonnegative_or_not_supported(self):
        for dev in range(_DEVICE_COUNT):
            power = pyamdsmi.smi_get_device_average_power(dev)
            print(f"\n  dev{dev} average_power: {_fmt(power)} W")
            with self.subTest(dev=dev):
                self.assertIsInstance(power, (int, float))
                self.assertTrue(
                    power == -1 or power >= 0,
                    f"Expected nonneg or -1 (not supported), got {power}"
                )


class TestSmiPerDevicePcie(PyAmdSmiTestBase):

    @skipNoGpu
    def test_pci_id_is_nonnegative_int(self):
        for dev in range(_DEVICE_COUNT):
            pci_id = pyamdsmi.smi_get_device_pci_id(dev)
            print(f"\n  dev{dev} pci_id: {pci_id:#x}")
            with self.subTest(dev=dev):
                self.assertIsInstance(pci_id, int)
                self.assertGreaterEqual(pci_id, 0)

    @skipNoGpu
    def test_pcie_bandwidth_is_struct_or_not_supported(self):
        for dev in range(_DEVICE_COUNT):
            bw = pyamdsmi.smi_get_device_pcie_bandwidth(dev)
            print(f"\n  dev{dev} pcie_bandwidth: {_fmt(bw)}")
            with self.subTest(dev=dev):
                self.assertTrue(
                    bw == -1 or bw is not None,
                    f"Expected bandwidth struct or -1, got {bw!r}"
                )

    @skipNoGpu
    def test_pcie_throughput_is_nonnegative_or_not_supported(self):
        for dev in range(_DEVICE_COUNT):
            result = pyamdsmi.smi_get_device_pcie_throughput(dev)
            print(f"\n  dev{dev} pcie_throughput: {_fmt(result)}")
            with self.subTest(dev=dev):
                self.assertTrue(
                    result == -1 or (isinstance(result, int) and result >= 0),
                    f"Expected nonneg int or -1, got {result!r}"
                )

    @skipNoGpu
    def test_pci_replay_counter_is_nonnegative_or_not_supported(self):
        for dev in range(_DEVICE_COUNT):
            counter = pyamdsmi.smi_get_device_pci_replay_counter(dev)
            print(f"\n  dev{dev} pci_replay_counter: {_fmt(counter)}")
            with self.subTest(dev=dev):
                self.assertIsInstance(counter, int)
                self.assertTrue(
                    counter == -1 or counter >= 0,
                    f"Expected nonneg or -1 (not supported), got {counter}"
                )


class TestSmiComputeProcess(PyAmdSmiTestBase):

    @skipNoGpu
    def test_compute_process_list_is_list(self):
        procs = pyamdsmi.smi_get_device_compute_process()
        print(f"\n  compute_processes: {procs}")
        self.assertIsInstance(procs, list)

    @skipNoGpu
    def test_compute_process_pids_are_positive_ints(self):
        procs = pyamdsmi.smi_get_device_compute_process()
        for pid in procs:
            with self.subTest(pid=pid):
                self.assertIsInstance(pid, int)
                self.assertGreater(pid, 0)

    @skipNoGpu
    def test_compute_process_info_by_device_is_list(self):
        procs = pyamdsmi.smi_get_device_compute_process()
        for dev in range(_DEVICE_COUNT):
            info = pyamdsmi.smi_get_compute_process_info_by_device(dev, procs)
            print(f"\n  dev{dev} compute_process_info: {len(info)} entries")
            with self.subTest(dev=dev):
                self.assertIsInstance(info, list)

    @skipNoGpu
    def test_compute_process_info_entries_have_process_id(self):
        procs = pyamdsmi.smi_get_device_compute_process()
        if not procs:
            self.skipTest("No active compute processes")
        for dev in range(_DEVICE_COUNT):
            info = pyamdsmi.smi_get_compute_process_info_by_device(dev, procs)
            for entry in info:
                pid = entry.process_id
                print(f"\n  dev{dev} process pid: {pid}")
                with self.subTest(dev=dev):
                    self.assertIsInstance(pid, int)
                    self.assertGreater(pid, 0)


class TestSmiPartition(PyAmdSmiTestBase):
    """Compute and memory partition queries (MI300 and newer)."""

    @skipNoGpu
    def test_compute_partition_is_string(self):
        for dev in range(_DEVICE_COUNT):
            result = pyamdsmi.smi_get_device_compute_partition(dev)
            print(f"\n  dev{dev} compute_partition: {result!r}")
            with self.subTest(dev=dev):
                self.assertIsInstance(result, str)

    @skipNoGpu
    def test_memory_partition_is_string(self):
        for dev in range(_DEVICE_COUNT):
            result = pyamdsmi.smi_get_device_memory_partition(dev)
            print(f"\n  dev{dev} memory_partition: {result!r}")
            with self.subTest(dev=dev):
                self.assertIsInstance(result, str)


class TestSmiTopology(PyAmdSmiTestBase):

    @skipNoGpu
    def test_numa_node_number_is_nonnegative_int(self):
        for dev in range(_DEVICE_COUNT):
            numa = pyamdsmi.smi_get_device_topo_numa_node_number(dev)
            print(f"\n  dev{dev} numa_node: {numa}")
            with self.subTest(dev=dev):
                self.assertIsInstance(numa, int)
                self.assertGreaterEqual(numa, 0)

    @skipNoGpu
    def test_link_type_between_devices_is_tuple_or_not_supported(self):
        if _DEVICE_COUNT < 2:
            self.skipTest("Need at least 2 GPUs for topology tests")
        result = pyamdsmi.smi_get_device_link_type(0, 1)
        print(f"\n  dev0->dev1 link_type: {_fmt(result)}")
        self.assertTrue(
            result == -1 or (isinstance(result, tuple) and len(result) == 2),
            f"Expected (hops, type) tuple or -1, got {result!r}"
        )

    @skipNoGpu
    def test_link_weight_between_devices_is_nonnegative_or_not_supported(self):
        if _DEVICE_COUNT < 2:
            self.skipTest("Need at least 2 GPUs for topology tests")
        result = pyamdsmi.smi_get_device_topo_link_weight(0, 1)
        print(f"\n  dev0->dev1 link_weight: {_fmt(result)}")
        self.assertTrue(
            result == -1 or (isinstance(result, int) and result >= 0),
            f"Expected nonneg int or -1, got {result!r}"
        )

    @skipNoGpu
    def test_p2p_accessible_is_bool_or_not_supported(self):
        if _DEVICE_COUNT < 2:
            self.skipTest("Need at least 2 GPUs for topology tests")
        result = pyamdsmi.smi_is_device_p2p_accessible(0, 1)
        print(f"\n  dev0->dev1 p2p_accessible: {_fmt(result)}")
        self.assertIn(result, [True, False, -1])


class TestSmiXgmi(PyAmdSmiTestBase):

    @skipNoGpu
    def test_xgmi_error_status_is_nonneg_or_not_supported(self):
        for dev in range(_DEVICE_COUNT):
            result = pyamdsmi.smi_get_device_xgmi_error_status(dev)
            print(f"\n  dev{dev} xgmi_error_status: {_fmt(result)}")
            with self.subTest(dev=dev):
                self.assertTrue(
                    result == -1 or (isinstance(result, int) and result >= 0),
                    f"Expected nonneg int or -1, got {result!r}"
                )

    @skipNoGpu
    def test_xgmi_hive_id_is_nonneg_or_not_supported(self):
        for dev in range(_DEVICE_COUNT):
            result = pyamdsmi.smi_get_device_xgmi_hive_id(dev)
            print(f"\n  dev{dev} xgmi_hive_id: {_fmt(result)}")
            with self.subTest(dev=dev):
                self.assertTrue(
                    result == -1 or (isinstance(result, int) and result >= 0),
                    f"Expected nonneg int or -1, got {result!r}"
                )


if __name__ == "__main__":
    unittest.main()
