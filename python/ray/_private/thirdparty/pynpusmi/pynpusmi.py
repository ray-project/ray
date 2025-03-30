import ctypes
import threading
import sys
from os.path import isfile
from ctypes import *  # 

ASCEND_910 = "910"
ASCEND_310 = "310"
class DCMIError_DriverNotLoaded(Exception):
    pass
class DCMIError_NotSupported(DCMIError_DriverNotLoaded):
    pass
class DCMIError_LibraryNotFound(DCMIError_DriverNotLoaded):
    pass
class DCMIError_Unknown(Exception):
    pass

dcmi_lib = None
lib_load_lock = threading.Lock()
_dcmi_lib_refcount = 0

def _load_dcmi_library():
    """Load DCMI library if not already loaded"""
    global dcmi_lib

    if dcmi_lib == None:

        lib_load_lock.acquire()

        try:
            if dcmi_lib == None:
                try:
                    if sys.platform[:3] == 'win':
                        raise DCMIError_NotSupported('Windows platform is not supported yet')
                    else:
                        # assume linux
                        path_libdcmi = _find_lib_dcmi()
                        cdll.LoadLibrary(path_libdcmi)
                        dcmi_lib = CDLL(path_libdcmi)
                except OSError:
                    raise DCMIError_LibraryNotFound('DCMI library not found')
                if dcmi_lib == None:
                    raise DCMIError_LibraryNotFound('DCMI library not found')
        finally:
            lib_load_lock.release()

def _find_lib_dcmi():
    dcmi_lib_path = "/usr/local/Ascend/driver/lib64/driver/libdcmi.so"
    return dcmi_lib_path if isfile(dcmi_lib_path) else ''

def smi_initialize():
    """Initialize DCMI binding of SMI"""
    _load_dcmi_library()
    ret_init = dcmi_lib.dcmi_init()
    if ret_init != 0:
        raise RuntimeError('DCMI initialization failed')
    # update reference count
    global _dcmi_lib_refcount
    lib_load_lock.acquire()
    _dcmi_lib_refcount += 1
    lib_load_lock.release()

class dcmi_proc_mem_info(Structure):
    _fields_ = [
        ("proc_id", c_int),
        ("proc_mem_usage", c_ulong),
    ]

class mem_struct(Structure):
    _fields_ = [
        ("memory_size", c_ulonglong),
        ("freq", c_uint),
        ("memory_usage", c_ulonglong),
        ("temp", c_int),
        ("bandwith_util_rate", c_uint),
    ]


class dcmi_chip_info(ctypes.Structure):
    _fields_ = [
        ("chip_type", ctypes.c_char * 32),
        ("chip_name", ctypes.c_char * 32),
        ("chip_ver", ctypes.c_char * 32),
        ("aicore_cnt", ctypes.c_uint32),
    ]


class dcmi_memory_info(ctypes.Structure):
    _fields_ = [
        ("memory_size", c_ulonglong),
        ("memory_available", c_ulonglong),
        ("freq", c_uint),
        ("hugepagesize", c_ulong),
        ("hugepages_total", c_ulong),
        ("hugepages_free", c_ulong),
        ("utiliza", c_uint),
        ("reserve", ctypes.c_char * 60),
    ]

class FriendlyObject(object):
    def __init__(self, dictionary):
        for x in dictionary:
            setattr(self, x, dictionary[x])
    
    def __str__(self):
        return self.__dict__.__str__()

def structToFriendlyObject(struct):
    d = {}
    for x in struct._fields_:
        key = x[0]
        value = getattr(struct, key)
        if isinstance(value, bytes):
            value = value.decode('utf-8')
        d[key] = value
    obj = FriendlyObject(d)
    return obj

def check_dcmi_ret(my_ret):
    if my_ret != 0x0:
        raise DCMIError_Unknown('Failed to invoke the driver interface.')

def dcmi_get_card_list():
    card_num = c_int(0)
    card_list = (c_int * 16)()
    status = dcmi_lib.dcmi_get_card_list(byref(card_num), byref(card_list), 16)
    check_dcmi_ret(status)
    return (card_num.value, card_list)

def dcmi_get_device_num_in_card(card_id):
    device_count = c_int(0)
    ##
    # dcmi_get_device_utilization_rate(card_id, device_id, input_type, &utilization_rate)
    # input_type 1：内存 2：AI CORE 3：AI CPU 4：控制CPU 5：内存带宽 6：HBM 8：DDR 10：HBM带宽 12：vector core
    ##
    status = dcmi_lib.dcmi_get_device_num_in_card(card_id, byref(device_count))
    check_dcmi_ret(status)
    return device_count.value

def dcmi_get_device_utilization_rate(card_id, device_id):
    utilization_npu = c_uint(0)
    status = dcmi_lib.dcmi_get_device_utilization_rate(
        card_id, device_id, 2, byref(utilization_npu)
    )
    check_dcmi_ret(status)
    return utilization_npu.value

def dcmi_get_device_chip_info(card_id, device_id):
    chip_info = dcmi_chip_info()
    status = dcmi_lib.dcmi_get_device_chip_info(
        ctypes.c_int32(card_id), ctypes.c_int32(device_id), ctypes.byref(chip_info)
    )
    check_dcmi_ret(status)
    return structToFriendlyObject(chip_info)

def dcmi_get_device_memory_info_310(card_id, device_id):
    memory_info = dcmi_memory_info()
    status = dcmi_lib.dcmi_get_device_memory_info_v3(
        ctypes.c_int32(card_id), ctypes.c_int32(device_id), ctypes.byref(memory_info)
    )
    check_dcmi_ret(status)
    return structToFriendlyObject(memory_info)

def dcmi_get_device_memory_info_910(card_id, device_id):
    memory_info = mem_struct()
    status = dcmi_lib.dcmi_get_hbm_info(
        ctypes.c_int32(card_id), ctypes.c_int32(device_id), byref(memory_info)
    )
    check_dcmi_ret(status)
    return structToFriendlyObject(memory_info)

def get_npu_process_list(card_id, device_id):
    info = (dcmi_proc_mem_info * 32)()
    proc_num = c_int(0)
    
    status = dcmi_lib.dcmi_get_device_resource_info(
        card_id, device_id, byref(info), byref(proc_num)
    )
    check_dcmi_ret(status)
    return [
        structToFriendlyObject(info[i]) 
        for i in range(proc_num.value)
    ]

def get_device_type(card_id, device_id):
    device_info = dcmi_get_device_chip_info(card_id, device_id)
    device_name = device_info.chip_name

    if device_name.startswith(ASCEND_310):
        return ASCEND_310
    if device_name.startswith(ASCEND_910):
        return ASCEND_910
    return ""