# MIT License
#
# Copyright (c) 2023 Advanced Micro Devices, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

# Python bindings shim: wraps the amd-smi Python interface with the same
# smi_* function signatures previously provided by the rocm-smi ctypes binding.

import logging
from collections import namedtuple

# Defer amdsmi import so that importing this module never raises ModuleNotFoundError
# on systems without the amdsmi package (non-AMD machines, CI, etc.).
# smi_initialize() will raise ImportError if amdsmi is not installed.
try:
    import amdsmi
    from amdsmi import (
        AmdSmiInitFlags,
        AmdSmiMemoryType,
        AmdSmiLibraryException,
    )
    _AMDSMI_AVAILABLE = True
except ImportError:
    amdsmi = None
    AmdSmiInitFlags = None
    AmdSmiMemoryType = None
    AmdSmiLibraryException = Exception  # so except clauses below don't NameError
    _AMDSMI_AVAILABLE = False

# Module-level handle list, indexed by integer device index.
# Populated during smi_initialize(), cleared on smi_shutdown().
_handles = []


# ── Namedtuple used by smi_get_compute_process_info_by_device ────────────────
# Keeps the same attribute interface (.process_id, .vram_usage) that
# gpu_providers.py expects on the returned objects.
ProcessInfo = namedtuple("ProcessInfo", ["process_id", "vram_usage"])


# ── Init / shutdown ───────────────────────────────────────────────────────────

def smi_initialize():
    """Initialize amd-smi and populate the device handle list."""
    if not _AMDSMI_AVAILABLE:
        raise ImportError(
            "amdsmi package is not installed. "
            "Install with: pip install amdsmi"
        )
    global _handles
    amdsmi.amdsmi_init(AmdSmiInitFlags.INIT_AMD_GPUS)
    _handles = amdsmi.amdsmi_get_processor_handles()


def smi_shutdown():
    """Shut down amd-smi and clear the handle list."""
    global _handles
    _handles = []
    amdsmi.amdsmi_shut_down()


# ── Device enumeration ────────────────────────────────────────────────────────

def smi_get_device_count():
    """Return the number of AMD GPU devices."""
    return len(_handles)


# ── Identity ──────────────────────────────────────────────────────────────────

def smi_get_device_id(dev):
    """Return the 16-bit device ID for GPU dev."""
    try:
        return amdsmi.amdsmi_get_gpu_id(_handles[dev])
    except AmdSmiLibraryException as e:
        logging.debug("smi_get_device_id(%d) failed: %s", dev, e)
        return -1


def smi_get_device_name(dev):
    """Return the market name string for GPU dev (e.g. 'Instinct MI300X')."""
    try:
        return amdsmi.amdsmi_get_gpu_asic_info(_handles[dev])["market_name"]
    except AmdSmiLibraryException as e:
        logging.debug("smi_get_device_name(%d) failed: %s", dev, e)
        return ""


def smi_get_device_unique_id(dev):
    """Return the UUID string for GPU dev (e.g. 'GPU-xxxxxxxx-xxxx-...')."""
    try:
        return amdsmi.amdsmi_get_gpu_device_uuid(_handles[dev])
    except AmdSmiLibraryException as e:
        logging.debug("smi_get_device_unique_id(%d) failed: %s", dev, e)
        return ""


# ── Utilization ───────────────────────────────────────────────────────────────

def smi_get_device_utilization(dev):
    """Return GPU busy percent (0-100) for GPU dev."""
    try:
        return amdsmi.amdsmi_get_gpu_busy_percent(_handles[dev])
    except AmdSmiLibraryException as e:
        logging.debug("smi_get_device_utilization(%d) failed: %s", dev, e)
        return -1


def smi_get_device_memory_busy(dev):
    """Return memory controller busy percent (0-100) for GPU dev."""
    try:
        activity = amdsmi.amdsmi_get_gpu_activity(_handles[dev])
        val = activity.get("umc_activity", "N/A")
        return val if val != "N/A" else -1
    except AmdSmiLibraryException as e:
        logging.debug("smi_get_device_memory_busy(%d) failed: %s", dev, e)
        return -1


# ── Memory ────────────────────────────────────────────────────────────────────

def smi_get_device_memory_used(dev, type="VRAM"):
    """Return used VRAM for GPU dev in bytes."""
    try:
        return amdsmi.amdsmi_get_gpu_memory_usage(_handles[dev], AmdSmiMemoryType.VRAM)
    except AmdSmiLibraryException as e:
        logging.debug("smi_get_device_memory_used(%d) failed: %s", dev, e)
        return -1


def smi_get_device_memory_total(dev, type="VRAM"):
    """Return total VRAM for GPU dev in bytes."""
    try:
        return amdsmi.amdsmi_get_gpu_memory_total(_handles[dev], AmdSmiMemoryType.VRAM)
    except AmdSmiLibraryException as e:
        logging.debug("smi_get_device_memory_total(%d) failed: %s", dev, e)
        return -1


def smi_get_device_memory_reserved_pages(dev):
    """Return reserved/bad memory pages info for GPU dev, or -1 if unsupported."""
    try:
        pages = amdsmi.amdsmi_get_gpu_memory_reserved_pages(_handles[dev])
        return (len(pages), pages)
    except AmdSmiLibraryException as e:
        logging.debug("smi_get_device_memory_reserved_pages(%d) failed: %s", dev, e)
        return -1


# ── Power ─────────────────────────────────────────────────────────────────────

def smi_get_device_average_power(dev):
    """Return average socket power for GPU dev in watts (float), or -1."""
    try:
        info = amdsmi.amdsmi_get_power_info(_handles[dev])
        val = info["average_socket_power"]
        return float(val) if val != "N/A" else -1
    except AmdSmiLibraryException as e:
        logging.debug("smi_get_device_average_power(%d) failed: %s", dev, e)
        return -1


# ── Processes ─────────────────────────────────────────────────────────────────

def smi_get_device_compute_process():
    """Return list of PIDs running compute workloads on any GPU."""
    try:
        procs = amdsmi.amdsmi_get_gpu_compute_process_info()
        return [p["process_id"] for p in procs]
    except AmdSmiLibraryException as e:
        logging.debug("smi_get_device_compute_process() failed: %s", e)
        return []


def smi_get_compute_process_info_by_device(device_id: int, proc_ids: list) -> list:
    """Return ProcessInfo(process_id, vram_usage) for compute processes on GPU device_id.

    proc_ids is the compute-only PID list from smi_get_device_compute_process().
    amdsmi_get_gpu_process_list returns all processes on the device (including graphics),
    so filter to proc_ids to return only compute processes, matching the old API contract.
    vram_usage is in bytes to match the contract expected by gpu_providers.py.
    """
    try:
        compute_pids = set(proc_ids)
        entries = amdsmi.amdsmi_get_gpu_process_list(_handles[device_id])
        result = []
        for e in entries:
            if e["pid"] not in compute_pids:
                continue
            # vram_mem is in bytes in amdsmi_get_gpu_process_list
            vram_bytes = e.get("memory_usage", {}).get("vram_mem", 0)
            result.append(ProcessInfo(
                process_id=e["pid"],
                vram_usage=vram_bytes,
            ))
        return result
    except AmdSmiLibraryException as e:
        logging.debug(
            "smi_get_compute_process_info_by_device(%d) failed: %s", device_id, e
        )
        return []


# ── Driver / kernel version ───────────────────────────────────────────────────

def smi_get_kernel_version():
    """Return the kernel driver version string."""
    try:
        if not _handles:
            return ""
        info = amdsmi.amdsmi_get_gpu_driver_info(_handles[0])
        return info.get("driver_version", "")
    except AmdSmiLibraryException as e:
        logging.debug("smi_get_kernel_version() failed: %s", e)
        return ""


# ── PCIe ─────────────────────────────────────────────────────────────────────

def smi_get_device_pcie_bandwidth(dev):
    """Return PCIe bandwidth info struct for GPU dev, or -1 if unsupported."""
    try:
        return amdsmi.amdsmi_get_gpu_pci_bandwidth(_handles[dev])
    except AmdSmiLibraryException as e:
        logging.debug("smi_get_device_pcie_bandwidth(%d) failed: %s", dev, e)
        return -1


def smi_get_device_pci_id(dev):
    """Return the BDF ID (Bus/Device/Function) for GPU dev as an int."""
    try:
        return amdsmi.amdsmi_get_gpu_bdf_id(_handles[dev])
    except AmdSmiLibraryException as e:
        logging.debug("smi_get_device_pci_id(%d) failed: %s", dev, e)
        return -1


def smi_get_device_topo_numa_affinity(dev):
    """Return NUMA affinity for GPU dev."""
    try:
        return amdsmi.amdsmi_get_gpu_topo_numa_affinity(_handles[dev])
    except AmdSmiLibraryException as e:
        logging.debug("smi_get_device_topo_numa_affinity(%d) failed: %s", dev, e)
        return -1


def smi_get_device_pcie_throughput(dev):
    """Return PCIe throughput (sent + received) * max_packet_size for GPU dev."""
    try:
        info = amdsmi.amdsmi_get_gpu_pci_throughput(_handles[dev])
        return (info["sent"] + info["received"]) * info["max_pkt_sz"]
    except AmdSmiLibraryException as e:
        logging.debug("smi_get_device_pcie_throughput(%d) failed: %s", dev, e)
        return -1


def smi_get_device_pci_replay_counter(dev):
    """Return PCIe replay counter for GPU dev."""
    try:
        return amdsmi.amdsmi_get_gpu_pci_replay_counter(_handles[dev])
    except AmdSmiLibraryException as e:
        logging.debug("smi_get_device_pci_replay_counter(%d) failed: %s", dev, e)
        return -1


# ── Compute partition ─────────────────────────────────────────────────────────

def smi_get_device_compute_partition(dev):
    """Return the current compute partition string for GPU dev."""
    try:
        return amdsmi.amdsmi_get_gpu_compute_partition(_handles[dev])
    except AmdSmiLibraryException as e:
        logging.debug("smi_get_device_compute_partition(%d) failed: %s", dev, e)
        return ""


def smi_set_device_compute_partition(dev, partition):
    """Set the compute partition for GPU dev. partition is an AmdSmiComputePartitionType."""
    try:
        amdsmi.amdsmi_set_gpu_compute_partition(_handles[dev], partition)
        return True
    except AmdSmiLibraryException as e:
        logging.debug("smi_set_device_compute_partition(%d) failed: %s", dev, e)
        return False


def smi_reset_device_compute_partition(dev):
    """Reset the compute partition for GPU dev to its boot state."""
    try:
        amdsmi.amdsmi_set_gpu_accelerator_partition_profile(_handles[dev], 0)
        return True
    except AmdSmiLibraryException as e:
        logging.debug("smi_reset_device_compute_partition(%d) failed: %s", dev, e)
        return False


# ── Memory partition ──────────────────────────────────────────────────────────

def smi_get_device_memory_partition(dev):
    """Return the current memory partition string for GPU dev."""
    try:
        return amdsmi.amdsmi_get_gpu_memory_partition(_handles[dev])
    except AmdSmiLibraryException as e:
        logging.debug("smi_get_device_memory_partition(%d) failed: %s", dev, e)
        return ""


def smi_set_device_memory_partition(dev, partition):
    """Set the memory partition for GPU dev. partition is an AmdSmiMemoryPartitionType."""
    try:
        amdsmi.amdsmi_set_gpu_memory_partition(_handles[dev], partition)
        return True
    except AmdSmiLibraryException as e:
        logging.debug("smi_set_device_memory_partition(%d) failed: %s", dev, e)
        return False


def smi_reset_device_memory_partition(dev):
    """Reset the memory partition for GPU dev to its boot state."""
    try:
        amdsmi.amdsmi_set_gpu_memory_partition_mode(_handles[dev], 0)
        return True
    except AmdSmiLibraryException as e:
        logging.debug("smi_reset_device_memory_partition(%d) failed: %s", dev, e)
        return False


# ── Hardware topology ─────────────────────────────────────────────────────────

def smi_get_device_topo_numa_node_number(dev):
    """Return the NUMA node number for GPU dev."""
    try:
        return amdsmi.amdsmi_topo_get_numa_node_number(_handles[dev])
    except AmdSmiLibraryException as e:
        logging.debug("smi_get_device_topo_numa_node_number(%d) failed: %s", dev, e)
        return -1


def smi_get_device_topo_link_weight(dev_src, dev_dst):
    """Return the link weight between two GPUs."""
    try:
        return amdsmi.amdsmi_topo_get_link_weight(_handles[dev_src], _handles[dev_dst])
    except AmdSmiLibraryException as e:
        logging.debug(
            "smi_get_device_topo_link_weight(%d, %d) failed: %s", dev_src, dev_dst, e
        )
        return -1


def smi_get_device_minmax_bandwidth(dev_src, dev_dst):
    """Return (min_bandwidth, max_bandwidth) between two GPUs, or -1."""
    try:
        result = amdsmi.amdsmi_get_minmax_bandwidth_between_processors(
            _handles[dev_src], _handles[dev_dst]
        )
        return (result["min_bandwidth"], result["max_bandwidth"])
    except AmdSmiLibraryException as e:
        logging.debug(
            "smi_get_device_minmax_bandwidth(%d, %d) failed: %s", dev_src, dev_dst, e
        )
        return -1


def smi_get_device_link_type(dev_src, dev_dst):
    """Return (hops, link_type) between two GPUs, or -1."""
    try:
        result = amdsmi.amdsmi_topo_get_link_type(_handles[dev_src], _handles[dev_dst])
        return (result["hops"], result["type"])
    except AmdSmiLibraryException as e:
        logging.debug(
            "smi_get_device_link_type(%d, %d) failed: %s", dev_src, dev_dst, e
        )
        return -1


def smi_is_device_p2p_accessible(dev_src, dev_dst):
    """Return True if two GPUs are P2P accessible, False if not, -1 on error."""
    try:
        return amdsmi.amdsmi_is_P2P_accessible(_handles[dev_src], _handles[dev_dst])
    except AmdSmiLibraryException as e:
        logging.debug(
            "smi_is_device_p2p_accessible(%d, %d) failed: %s", dev_src, dev_dst, e
        )
        return -1


# ── XGMI ─────────────────────────────────────────────────────────────────────

def smi_get_device_xgmi_error_status(dev):
    """Return the XGMI error status (AmdSmiXgmiStatus int value) for GPU dev."""
    try:
        status = amdsmi.amdsmi_gpu_xgmi_error_status(_handles[dev])
        return status.value
    except AmdSmiLibraryException as e:
        logging.debug("smi_get_device_xgmi_error_status(%d) failed: %s", dev, e)
        return -1


def smi_reset_device_xgmi_error(dev):
    """Reset XGMI error status for GPU dev. Returns True on success."""
    try:
        amdsmi.amdsmi_reset_gpu_xgmi_error(_handles[dev])
        return True
    except AmdSmiLibraryException as e:
        logging.debug("smi_reset_device_xgmi_error(%d) failed: %s", dev, e)
        return False


def smi_get_device_xgmi_hive_id(dev):
    """Return the XGMI hive ID for GPU dev."""
    try:
        return amdsmi.amdsmi_get_xgmi_info(_handles[dev])["xgmi_hive_id"]
    except AmdSmiLibraryException as e:
        logging.debug("smi_get_device_xgmi_hive_id(%d) failed: %s", dev, e)
        return -1
