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

# Python bindings for ROCm-SMI library
from ctypes import *
from os.path import join, realpath, isfile
import os
import logging
import subprocess
import sys
import threading
from enum import IntEnum, auto



## Error checking
class ROCMLError_NotSupported(Exception):
    pass


class ROCMLError_FunctionNotFound(Exception):
    pass


class ROCMLError_LibraryNotFound(Exception):
    pass


class ROCMLError_DriverNotLoaded(Exception):
    pass


class ROCMLError_Unknown(Exception):
    pass


class ROCMLError_Uninitialized(Exception):
    pass


class ROCMLState(IntEnum):
    UNINITIALIZED = auto()
    """No attempt yet made to initialize PyROCML"""
    INITIALIZED = auto()
    """PyROCML was successfully initialized"""
    DISABLED_PYROCML_NOT_AVAILABLE = auto()
    """PyROCML not installed"""
    DISABLED_CONFIG = auto()
    """PyROCML diagnostics disabled by ``distributed.diagnostics.rocml`` config setting"""
    DISABLED_LIBRARY_NOT_FOUND = auto()
    """PyROCML available, but ROCML not installed"""


LIBROCM_NAME = 'librocm_smi64.so'
RSMI_MAX_BUFFER_LENGTH = 256

# Policy enums
RSMI_MAX_NUM_FREQUENCIES = 32


class rsmi_status_t(c_int):
    RSMI_STATUS_SUCCESS = 0x0
    RSMI_STATUS_INVALID_ARGS = 0x1
    RSMI_STATUS_NOT_SUPPORTED = 0x2
    RSMI_STATUS_FILE_ERROR = 0x3
    RSMI_STATUS_PERMISSION = 0x4
    RSMI_STATUS_OUT_OF_RESOURCES = 0x5
    RSMI_STATUS_INTERNAL_EXCEPTION = 0x6
    RSMI_STATUS_INPUT_OUT_OF_BOUNDS = 0x7
    RSMI_STATUS_INIT_ERROR = 0x8
    RSMI_INITIALIZATION_ERROR = RSMI_STATUS_INIT_ERROR
    RSMI_STATUS_NOT_YET_IMPLEMENTED = 0x9
    RSMI_STATUS_NOT_FOUND = 0xA
    RSMI_STATUS_INSUFFICIENT_SIZE = 0xB
    RSMI_STATUS_INTERRUPT = 0xC
    RSMI_STATUS_UNEXPECTED_SIZE = 0xD
    RSMI_STATUS_NO_DATA = 0xE
    RSMI_STATUS_UNKNOWN_ERROR = 0xFFFFFFFF


#Dictionary of rsmi ret codes and it's verbose output
rsmi_status_verbose_err_out = {
    rsmi_status_t.RSMI_STATUS_SUCCESS: 'Operation was successful',
    rsmi_status_t.RSMI_STATUS_INVALID_ARGS: 'Invalid arguments provided',
    rsmi_status_t.RSMI_STATUS_NOT_SUPPORTED: 'Not supported on the given system',
    rsmi_status_t.RSMI_STATUS_FILE_ERROR: 'Problem accessing a file',
    rsmi_status_t.RSMI_STATUS_PERMISSION: 'Permission denied',
    rsmi_status_t.RSMI_STATUS_OUT_OF_RESOURCES: 'Unable to acquire memory or other resource',
    rsmi_status_t.RSMI_STATUS_INTERNAL_EXCEPTION: 'An internal exception was caught',
    rsmi_status_t.RSMI_STATUS_INPUT_OUT_OF_BOUNDS: 'Provided input is out of allowable or safe range',
    rsmi_status_t.RSMI_INITIALIZATION_ERROR: 'Error occured during rsmi initialization',
    rsmi_status_t.RSMI_STATUS_NOT_YET_IMPLEMENTED: 'Requested function is not implemented on this setup',
    rsmi_status_t.RSMI_STATUS_NOT_FOUND: 'Item searched for but not found',
    rsmi_status_t.RSMI_STATUS_INSUFFICIENT_SIZE: 'Insufficient resources available',
    rsmi_status_t.RSMI_STATUS_INTERRUPT: 'Interrupt occured during execution',
    rsmi_status_t.RSMI_STATUS_UNEXPECTED_SIZE: 'Unexpected amount of data read',
    rsmi_status_t.RSMI_STATUS_NO_DATA: 'No data found for the given input',
    rsmi_status_t.RSMI_STATUS_UNKNOWN_ERROR: 'Unknown error occured'
}


class rsmi_init_flags_t(c_int):
    RSMI_INIT_FLAG_ALL_GPUS = 0x1


class rsmi_memory_type_t(c_int):
    RSMI_MEM_TYPE_FIRST = 0
    RSMI_MEM_TYPE_VRAM = RSMI_MEM_TYPE_FIRST
    RSMI_MEM_TYPE_VIS_VRAM = 1
    RSMI_MEM_TYPE_GTT = 2
    RSMI_MEM_TYPE_LAST = RSMI_MEM_TYPE_GTT


# memory_type_l includes names for with rsmi_memory_type_t
# Usage example to get corresponding names:
# memory_type_l[rsmi_memory_type_t.RSMI_MEM_TYPE_VRAM] will return string 'vram'
memory_type_l = ['VRAM', 'VIS_VRAM', 'GTT']


class rsmi_retired_page_record_t(Structure):
    _fields_ = [('page_address', c_uint64),
                ('page_size', c_uint64),
                ('status', c_int)]


class rsmi_sw_component_t(c_int):
    RSMI_SW_COMP_FIRST = 0x0
    RSMI_SW_COMP_DRIVER = RSMI_SW_COMP_FIRST
    RSMI_SW_COMP_LAST = RSMI_SW_COMP_DRIVER


class rsmi_frequencies_t(Structure):
    _fields_ = [('num_supported', c_int32),
                ('current', c_uint32),
                ('frequency', c_uint64 * RSMI_MAX_NUM_FREQUENCIES)]


class rsmi_pcie_bandwidth_t(Structure):
    _fields_ = [('transfer_rate', rsmi_frequencies_t),
                ('lanes', c_uint32 * RSMI_MAX_NUM_FREQUENCIES)]


class rsmi_process_info_t(Structure):
    _fields_ = [('process_id', c_uint32),
                ('pasid', c_uint32),         # PSA: Power Spectrum Analysis ?
                ('vram_usage', c_uint64),
                ('sdma_usage', c_uint64),    # SDMA: System Direct Memory Access
                ('cu_occupancy', c_uint32)]


class rsmi_xgmi_status_t(c_int):
    RSMI_XGMI_STATUS_NO_ERRORS = 0
    RSMI_XGMI_STATUS_ERROR = 1
    RSMI_XGMI_STATUS_MULTIPLE_ERRORS = 2


class rsmi_io_link_type(c_int):
    RSMI_IOLINK_TYPE_UNDEFINED      = 0
    RSMI_IOLINK_TYPE_HYPERTRANSPORT = 1
    RSMI_IOLINK_TYPE_PCIEXPRESS     = 2
    RSMI_IOLINK_TYPE_AMBA           = 3
    RSMI_IOLINK_TYPE_MIPI           = 4
    RSMI_IOLINK_TYPE_QPI_1_1        = 5
    RSMI_IOLINK_TYPE_RESERVED1      = 6
    RSMI_IOLINK_TYPE_RESERVED2      = 7
    RSMI_IOLINK_TYPE_RAPID_IO       = 8
    RSMI_IOLINK_TYPE_INFINIBAND     = 9
    RSMI_IOLINK_TYPE_RESERVED3      = 10
    RSMI_IOLINK_TYPE_XGMI           = 11
    RSMI_IOLINK_TYPE_XGOP           = 12
    RSMI_IOLINK_TYPE_GZ             = 13
    RSMI_IOLINK_TYPE_ETHERNET_RDMA  = 14
    RSMI_IOLINK_TYPE_RDMA_OTHER     = 15
    RSMI_IOLINK_TYPE_OTHER          = 16
    RSMI_IOLINK_TYPE_NUMIOLINKTYPES = 17
    RSMI_IOLINK_TYPE_SIZE           = 0xFFFFFFFF


## Library loading
rocm_lib = None
lib_load_lock = threading.Lock()
_rocm_lib_refcount = 0


## Function access, to prevent lib_load_lock deadlock
_rocml_get_function_ptr_cache = dict()

def _rocml_get_function_ptr(name):
    global rocm_lib

    if name in _rocml_get_function_ptr_cache:
        return _rocml_get_function_ptr_cache[name]

    lib_load_lock.acquire()
    try:
        # ensure library was loaded
        if rocm_lib == None:
            raise ROCMLError_Uninitialized
        try:
            _rocml_get_function_ptr_cache[name] = getattr(rocm_lib, name)
            return _rocml_get_function_ptr_cache[name]
        except AttributeError:
            raise ROCMLError_FunctionNotFound
    finally:
        # lock is always freed
        lib_load_lock.release()


def _load_rocm_library():
    """Load ROCm library if not already loaded"""
    global rocm_lib

    if rocm_lib == None:

        lib_load_lock.acquire()

        try:
            if rocm_lib == None:
                try:
                    if sys.platform[:3] == 'win':
                        raise ROCMLError_NotSupported('Windows platform is not supported yet')
                    else:
                        # assume linux
                        path_librocm = _find_lib_rocm()
                        cdll.LoadLibrary(path_librocm)
                        rocm_lib = CDLL(path_librocm)
                except OSError:
                    raise ROCMLError_LibraryNotFound('ROCm library not found')
                if rocm_lib == None:
                    raise ROCMLError_LibraryNotFound('ROCm library not found')
        finally:
            lib_load_lock.release()


def _find_lib_rocm():
    """search for librocm and returns path
    if search fails, returns empty string
    """
    rocm_path = os.environ.get('ROCM_PATH', '/opt/rocm')
    rocm_lib_path = join(rocm_path, f'lib/{LIBROCM_NAME}')
    return rocm_lib_path if isfile(rocm_lib_path) else ''


def _driver_initialized():
    """ Returns true if amdgpu is found in the list of initialized modules
    """
    initialized = ''
    try:
        initialized = str(subprocess.check_output("cat /sys/module/amdgpu/initstate |grep live", shell=True, stderr=subprocess.DEVNULL))
    except subprocess.CalledProcessError:
        pass
    return len(initialized) > 0


def smi_initialize():
    """Initialize ROCm binding of SMI"""
    _load_rocm_library()

    if _driver_initialized():
        ret_init = rocm_lib.rsmi_init(0)
        if ret_init != 0:
            logging.debug("ROCm SMI init returned value: %s", ret_init)
            raise RuntimeError('ROCm SMI initialization failed')
    else:
        raise RuntimeError('ROCm driver initilization failed')

    # update reference count
    global _rocm_lib_refcount
    lib_load_lock.acquire()
    _rocm_lib_refcount += 1
    lib_load_lock.release()


def rsmi_ret_ok(my_ret):
    """ Returns true if RSMI call status is 0 (success)

    @param device: DRM device identifier
    @param my_ret: Return of RSMI call (rocm_smi_lib API)
    @param metric: Parameter of GPU currently being analyzed
    """
    if my_ret != rsmi_status_t.RSMI_STATUS_SUCCESS:
        err_str = c_char_p()
        rocm_lib.rsmi_status_string(my_ret, byref(err_str))
        logging.debug("ROCm RSMI error: %s", err_str.value.decode())
        return False
    return True


def smi_shutdown():
    """leave the library loaded, but shutdown the interface"""
    rsmi_ret_ok(rocm_lib.rsmi_shut_down())

    # update reference count
    global _rocm_lib_refcount
    lib_load_lock.acquire()
    _rocm_lib_refcount -= 1
    lib_load_lock.release()


def smi_get_kernel_version():
    """returns ROCm kernerl driver version"""
    ver_str = create_string_buffer(256)
    ret = rocm_lib.rsmi_version_str_get(rsmi_sw_component_t.RSMI_SW_COMP_DRIVER, ver_str, 256)
    return ver_str.value.decode() if rsmi_ret_ok(ret) else ''

def smi_get_device_id(dev):
    """returns device id of the device as 64bit integer"""
    uid = c_uint64()
    ret = rocm_lib.rsmi_dev_id_get(dev, byref(uid))
    return uid.value if rsmi_ret_ok(ret) else -1

def smi_get_device_count():
    """returns a list of GPU devices """
    num_device = c_uint32(0)
    ret = rocm_lib.rsmi_num_monitor_devices(byref(num_device))
    return num_device.value if rsmi_ret_ok(ret) else -1


def smi_get_device_name(dev):
    """returns the name of a GPU device"""
    series = create_string_buffer(RSMI_MAX_BUFFER_LENGTH)
    ret = rocm_lib.rsmi_dev_name_get(dev, series, RSMI_MAX_BUFFER_LENGTH)
    return series.value.decode() if rsmi_ret_ok(ret) else ''


def smi_get_device_unique_id(dev):
    """returns unique id of the device as 64bit integer"""
    uid = c_uint64()
    ret = rocm_lib.rsmi_dev_unique_id_get(dev, byref(uid))
    return uid.value if rsmi_ret_ok(ret) else -1

def smi_get_device_utilization(dev):
    """returns GPU device busy percent of device_id dev"""
    busy_percent = c_uint32()
    ret = rocm_lib.rsmi_dev_busy_percent_get(dev, byref(busy_percent))
    return busy_percent.value if rsmi_ret_ok(ret) else -1


def smi_get_device_memory_used(dev, type='VRAM'):
    """returns used memory of device_id dev in bytes"""
    type_idx = memory_type_l.index(type)
    used = c_uint64()
    ret = rocm_lib.rsmi_dev_memory_usage_get(dev, type_idx, byref(used))
    return used.value if rsmi_ret_ok(ret) else -1


def smi_get_device_memory_total(dev, type='VRAM'):
    """returns total memory of device_id dev in bytes"""
    type_idx = memory_type_l.index(type)
    total = c_uint64()
    ret = rocm_lib.rsmi_dev_memory_total_get(dev, type_idx, byref(total))
    return total.value if rsmi_ret_ok(ret) else -1


def smi_get_device_memory_busy(dev):
    """returns percentage of time any device memory is being used"""
    busy_percent = c_uint32()
    ret = rocm_lib.rsmi_dev_memory_busy_percent_get(dev, byref(busy_percent))
    return busy_percent.value if rsmi_ret_ok(ret) else -1


def smi_get_device_memory_reserved_pages(dev):
    """returns info about reserved memory pages"""
    num_pages = c_uint32()
    records = rsmi_retired_page_record_t()
    ret = rocm_lib.rsmi_dev_memory_reserved_pages_get(dev, byref(num_pages), byref(records))
    return (num_pages.value, records) if rsmi_ret_ok(ret) else -1


# PCIE functions
def smi_get_device_pcie_bandwidth(dev):
    """returns list of possible pcie bandwidths for the device in bytes/sec"""
    bandwidth = rsmi_pcie_bandwidth_t()
    ret = rocm_lib.rsmi_dev_pci_bandwidth_get(dev, byref(bandwidth))
    return bandwidth if rsmi_ret_ok(ret) else -1


def smi_get_device_pci_id(dev):
    """returns unique PCI ID of the device in 64bit Hex with format:
       BDFID = ((DOMAIN & 0xffffffff) << 32) | ((BUS & 0xff) << 8) |
                    ((DEVICE & 0x1f) <<3 ) | (FUNCTION & 0x7)
    """
    bdfid = c_uint64()
    ret = rocm_lib.rsmi_dev_pci_id_get(dev, byref(bdfid))
    return bdfid.value if rsmi_ret_ok(ret) else -1


def smi_get_device_topo_numa_affinity(dev):
    """returns the NUMA node associated with the device"""
    numa_node = c_uint32()
    ret = reocm_lib.rsmi_topo_numa_affinity_get(dev, byref(numa_node))
    return numa_node.value if rsmi_ret_ok(ret) else -1


def smi_get_device_pcie_throughput(dev):
    """returns measured pcie throughput for the device in bytes/sec"""
    sent = c_uint64()
    recv = c_uint64()
    max_pkt_sz = c_uint64()
    ret = rocm_lib.rsmi_dev_pci_throughput_get(dev, byref(sent), byref(recv), byref(max_pkt_sz))
    return (recv.value + sent.value) * max_pkt_sz.value if rsmi_ret_ok(ret) else -1


def smi_get_device_pci_replay_counter(dev):
    """return PCIe replay counter of the device"""
    counter = c_uint64()
    ret = rocm_lib.rsmi_dev_pci_replay_counter_get(dev, byref(counter))
    return counter.value if rsmi_ret_ok(ret) else -1


# Compute partition functions
def smi_get_device_compute_partition(dev):
    """returns the compute partition of the device"""
    partition = create_string_buffer(RSMI_MAX_BUFFER_LENGTH)
    ret = rocm_lib.rsmi_dev_compute_partition_get(dev, byref(partition), RSMI_MAX_BUFFER_LENGTH)
    return partition.value.decode() if rsmi_ret_ok(ret) else ''


def smi_set_device_compute_partition(dev, partition):
    """modifies the compute partition of the selected device"""
    ret = rocm_lib.rsmi_dev_compute_partition_set(dev, partition)
    return rsmi_ret_ok(ret)


def smi_reset_device_compute_partition(dev):
    """reverts the compute partition of the selected device to its boot state"""
    ret = rocm_lib.rsmi_dev_compute_partition_reset(dev)
    return rsmi_ret_ok(ret)


# Memory partition functions
def smi_get_device_memory_partition(dev):
    """returns the memory partition of the device"""
    partition = create_string_buffer(RSMI_MAX_BUFFER_LENGTH)
    ret = rocm_lib.rsmi_dev_memory_partition_get(dev, byref(partition), RSMI_MAX_BUFFER_LENGTH)
    return partition.value.decode() if rsmi_ret_ok(ret) else ''


def smi_set_device_memory_partition(dev, partition):
    """modifies the memory partition of the selected device"""
    ret = rocm_lib.rsmi_dev_memory_partition_set(dev, partition)
    return rsmi_ret_ok(ret)


def smi_reset_device_memory_partition(dev):
    """reverts the memory partition of the selected device to its boot state"""
    ret = rocm_lib.rsmi_dev_memory_partition_reset(dev)
    return rsmi_ret_ok(ret)


# Hardware Topology functions
def smi_get_device_topo_numa_node_number(dev):
    """returns the NUMA node associated with the device"""
    numa_node = c_uint32()
    ret = rocm_lib.rsmi_topo_get_numa_node_number(dev, byref(numa_node))
    return numa_node.value if rsmi_ret_ok(ret) else -1


def smi_get_device_topo_link_weight(dev_src, dev_dst):
    """returns the weight of the link between two devices"""
    weight = c_uint64()
    ret = rocm_lib.rsmi_topo_get_link_weight(dev_src, dev_dst, byref(weight))
    return weight.value if rsmi_ret_ok(ret) else -1


def smi_get_device_minmax_bandwidth(dev_src, dev_dst):
    """returns the minimum and maximum io link bandwidth between two devices
    API works if src and dst are connected via XGMI and are 1 hop away.
    """
    assert smi_get_device_link_type(dev_src, dev_dst)[0] == 1, 'Devices must be 1 hop away'
    min_bandwidth = c_uint64()
    max_bandwidth = c_uint64()
    ret = rocm_lib.rsmi_minmax_bandwidth_get(dev_src, dev_dst, byref(min_bandwidth), byref(max_bandwidth))
    return (min_bandwidth.value, max_bandwidth.value) if rsmi_ret_ok(ret) else -1


def smi_get_device_link_type(dev_src, dev_dst):
    """returns the hops and the type of link between two devices"""
    hops = c_uint64()
    link_type = rsmi_io_link_type()
    ret = rocm_lib.rsmi_topo_get_link_type(dev_src, dev_dst, byref(hops), byref(link_type))
    return (hops.value, link_type.value) if rsmi_ret_ok(ret) else -1


def smi_is_device_p2p_accessible(dev_src, dev_dst):
    """returns true if two devices are p2p accessible"""
    accessible = c_bool()
    ret = rocm_lib.rsmi_is_P2P_accessible(dev_src, dev_dst, byref(accessible))
    return accessible.value if rsmi_ret_ok(ret) else -1


def smi_get_device_compute_process():
    """returns list of process ids running compute on the system"""
    num_procs = c_uint32()
    ret = rocm_lib.rsmi_compute_process_info_get(None, byref(num_procs))
    if rsmi_ret_ok(ret):
        buff_sz = num_procs.value + 10
        proc_info = (rsmi_process_info_t * buff_sz)()
        ret2 = rocm_lib.rsmi_compute_process_info_get(byref(proc_info), byref(num_procs))

        return [proc_info[i].process_id for i in range(num_procs.value)] if rsmi_ret_ok(ret2) else []
    else:
        return []


def smi_get_device_average_power(dev):
    """returns average power of device_id dev"""
    power = c_uint32()
    ret = rocm_lib.rsmi_dev_power_ave_get(dev, 0, byref(power))

    return power.value * 1e-6 if rsmi_ret_ok(ret) else -1


# XGMI fuctions
def smi_get_device_xgmi_error_status(dev):
    """returns XGMI error status for a device"""
    status = rsmi_xgmi_status_t()
    ret = rocm_lib.rsmi_dev_xgmi_error_status(dev, byref(status))
    return status.value if rsmi_ret_ok(ret) else -1


def smi_reset_device_xgmi_error(dev):
    """resets XGMI error status for a device"""
    ret = rocm_lib.rsmi_dev_xgmi_error_reset(dev)
    return rsmi_ret_ok(ret)


def smi_get_device_xgmi_hive_id(dev):
    """returns XGMI hive ID for a device"""
    hive_id = c_uint64()
    ret = rocm_lib.rsmi_dev_xgmi_hive_id_get(dev, byref(hive_id))
    return hive_id.value if rsmi_ret_ok(ret) else -1
