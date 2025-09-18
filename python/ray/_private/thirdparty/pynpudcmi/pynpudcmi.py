import threading
import string
import sys
from ctypes import *

dcmi_handle = None

libLoadLock = threading.Lock()

DCMI_OK                                 = 0
DCMI_ERR_CODE_INVALID_PARAMETER        = -8001
DCMI_ERR_CODE_OPER_NOT_PERMITTED       = -8002
DCMI_ERR_CODE_MEM_OPERATE_FAIL         = -8003
DCMI_ERR_CODE_SECURE_FUN_FAIL          = -8004
DCMI_ERR_CODE_INNER_ERR                = -8005
DCMI_ERR_CODE_TIME_OUT                 = -8006
DCMI_ERR_CODE_INVALID_DEVICE_ID        = -8007
DCMI_ERR_CODE_DEVICE_NOT_EXIST         = -8008
DCMI_ERR_CODE_IOCTL_FAIL               = -8009
DCMI_ERR_CODE_SEND_MSG_FAIL            = -8010
DCMI_ERR_CODE_RECV_MSG_FAIL            = -8011
DCMI_ERR_CODE_NOT_REDAY                = -8012   # (sic – name in docs)
DCMI_ERR_CODE_NOT_SUPPORT_IN_CONTAINER = -8013
DCMI_ERR_CODE_RESET_FAIL               = -8015
DCMI_ERR_CODE_ABORT_OPERATE            = -8016
DCMI_ERR_CODE_IS_UPGRADING             = -8017
DCMI_ERR_CODE_RESOURCE_OCCUPIED        = -8020
DCMI_ERR_CODE_CONFIG_INFO_NOT_EXIST    = -8023
DCMI_ERR_CODE_NOT_SUPPORT              = -8255

_dcmi_err_strings = {
    DCMI_OK:                                 "Execution succeeded",
    DCMI_ERR_CODE_INVALID_PARAMETER:         "Incorrect input parameter",
    DCMI_ERR_CODE_OPER_NOT_PERMITTED:        "Permission error",
    DCMI_ERR_CODE_MEM_OPERATE_FAIL:          "Failed to operate the memory API",
    DCMI_ERR_CODE_SECURE_FUN_FAIL:           "Failed to execute the secure function",
    DCMI_ERR_CODE_INNER_ERR:                 "Internal error",
    DCMI_ERR_CODE_TIME_OUT:                  "Response timeout",
    DCMI_ERR_CODE_INVALID_DEVICE_ID:         "Invalid device ID",
    DCMI_ERR_CODE_DEVICE_NOT_EXIST:          "The device does not exist",
    DCMI_ERR_CODE_IOCTL_FAIL:                "ioctl returned failure",
    DCMI_ERR_CODE_SEND_MSG_FAIL:             "Failed to send the message",
    DCMI_ERR_CODE_RECV_MSG_FAIL:             "Failed to receive the message",
    DCMI_ERR_CODE_NOT_REDAY:                 "Not ready — please try again",
    DCMI_ERR_CODE_NOT_SUPPORT_IN_CONTAINER:  "API not supported in container",
    DCMI_ERR_CODE_RESET_FAIL:                "Reset failed",
    DCMI_ERR_CODE_ABORT_OPERATE:             "Reset cancelled",
    DCMI_ERR_CODE_IS_UPGRADING:              "Device is upgrading",
    DCMI_ERR_CODE_RESOURCE_OCCUPIED:         "Device resources are occupied",
    DCMI_ERR_CODE_CONFIG_INFO_NOT_EXIST:     "Requested configuration does not exist",
    DCMI_ERR_CODE_NOT_SUPPORT:               "Device/function not supported",
}

DCMI_UTIL_QUERY_MEMORY          = 1
DCMI_UTIL_QUERY_AI_CORE         = 2
DCMI_UTIL_QUERY_AI_CPU          = 3
DCMI_UTIL_QUERY_CTRL_CPU        = 4
DCMI_UTIL_QUERY_MEMORY_BANDWIDH = 5
DCMI_UTIL_QUERY_ON_CHIP_MEMORY  = 6
DCMI_UTIL_QUERY_DDR             = 8

MAX_PROC_NUM_IN_DEVICE = 32

MAX_CHIP_NAME_LEN = 32

class DCMIError(Exception):
    """Base class for all DCMI exceptions."""
    _valClassMapping = {}

    def __new__(cls, value):
        # Promote to the specific subclass if one exists
        if cls is DCMIError:
            cls = DCMIError._valClassMapping.get(value, cls)
        obj = super().__new__(cls)
        obj.value = value
        return obj

    def __str__(self):
        return _dcmi_err_strings.get(
            self.value, f"DCMI Error with code {self.value}"
        )

    def __eq__(self, other):
        return getattr(other, "value", None) == self.value

def dcmiExceptionClass(code: int):
    """Return the concrete DCMIError subclass for *code* (or raise ValueError)."""
    try:
        return DCMIError._valClassMapping[code]
    except KeyError as exc:
        raise ValueError(f"DCMI error code {code} is not recognised") from exc

def _extractDCMIErrorsAsClasses():
    """
    Dynamically create DCMIError_<Name> subclasses for every DCMI_ERR_CODE_* symbol.
    This replicates NVML’s approach so callers can `except DCMIError_ResetFail: ...`.
    """
    this_mod = sys.modules[__name__]
    symbol_names = [n for n in dir(this_mod) if n.startswith("DCMI_ERR_CODE_") or n == "DCMI_OK"]

    for sym in symbol_names:
        code = getattr(this_mod, sym)
        # Build a CamelCase class name, e.g. DCMIError_InvalidParameter
        class_name = "DCMIError_" + string.capwords(
            sym.replace("DCMI_ERR_CODE_", "").replace("DCMI_OK", "Ok"),
            "_"
        ).replace("_", "")

        def gen_new(val):
            def __new__(subcls):
                return DCMIError.__new__(subcls, val)
            return __new__

        new_cls = type(class_name, (DCMIError,), {"__new__": gen_new(code)})
        new_cls.__module__ = __name__
        setattr(this_mod, class_name, new_cls)
        DCMIError._valClassMapping[code] = new_cls

_extractDCMIErrorsAsClasses()

def _dcmiCheckReturn(ret: int):
    if ret != DCMI_OK:
        raise DCMIError(ret)
    return ret

def _check_driver_location():
    if sys.platform[:3] == "win":
        # Doesn't support on windows
        raise OSError("NPU is not supported on Windows")
    else:
        import os
        dcmi_path = os.environ.get("DCMI_PATH", "/usr/local/Ascend/driver/lib64/driver/libdcmi.so")
        if not os.path.exists(dcmi_path):
            raise OSError("DCMI library not found")
        return dcmi_path

def dcmi_init():
    global dcmi_handle

    if dcmi_handle is None:
        libLoadLock.acquire()

        try:
            if dcmi_handle is None:
                dcmi_path = _check_driver_location()
                dcmi_handle = cdll.LoadLibrary(dcmi_path)
                _dcmiCheckReturn(dcmi_handle.dcmi_init())

        finally:
            libLoadLock.release()

class dcmi_chip_info_v2(Structure):
    _fields_ = [
        ("chip_type",  c_ubyte * MAX_CHIP_NAME_LEN),
        ("chip_name",  c_ubyte * MAX_CHIP_NAME_LEN),
        ("chip_ver",   c_ubyte * MAX_CHIP_NAME_LEN),
        ("aicore_cnt", c_uint),
        ("npu_name",   c_ubyte * MAX_CHIP_NAME_LEN)
    ]

class dcmi_proc_mem_info(Structure):
    _fields_ = [
        ("proc_id",        c_int),
        ("proc_mem_usage", c_ulong)
    ]

class dcmi_hbm_info(Structure):
    _fields_ = [
        ("memory_size",         c_ulonglong),
        ("freq",                c_uint),
        ("memory_usage",        c_ulonglong),
        ("temp",                c_int),
        ("bandwidth_util_rate", c_uint)
    ]

class dcmi_memory_info(Structure):
    _fields_ = [
        ("memory_size", c_ulonglong), # The unit is MB
        ("freq", c_uint), # MHz
        ("utiliza", c_uint)
    ]

def dcmi_get_device_chip_info_v2(card_id, device_id):
    if dcmi_handle is None:
        raise DCMIError(DCMI_ERR_CODE_NOT_REDAY)

    info = dcmi_chip_info_v2()
    ret = dcmi_handle.dcmi_get_device_chip_info_v2(card_id, device_id, byref(info))

    _dcmiCheckReturn(ret)

    return info

def dcmi_get_device_resource_info(card_id, device_id):
    if dcmi_handle is None:
        raise DCMIError(DCMI_ERR_CODE_NOT_REDAY)

    info = (dcmi_proc_mem_info*MAX_PROC_NUM_IN_DEVICE)()
    proc_num = c_int(0)

    ret = dcmi_handle.dcmi_get_device_resource_info(card_id, device_id, byref(info), byref(proc_num))

    _dcmiCheckReturn(ret)

    return info[0:proc_num.value]

def dcmi_get_device_memory_info_v2(card_id, device_id):
    if dcmi_handle is None:
        raise DCMIError(DCMI_ERR_CODE_NOT_REDAY)

    info = dcmi_memory_info()
    ret = dcmi_handle.dcmi_get_device_memory_info_v2(card_id, device_id, byref(info))

    _dcmiCheckReturn(ret)

    return info

def dcmi_get_card_list():
    if dcmi_handle is None:
        raise DCMIError(DCMI_ERR_CODE_NOT_REDAY)

    card_num = c_int(0)
    card_list = (c_int*64)() # Up to 64 npus

    ret = dcmi_handle.dcmi_get_card_list(byref(card_num), byref(card_list), 64)

    _dcmiCheckReturn(ret)

    return card_list[0:card_num.value]

def dcmi_get_device_id_in_card(card_id):
    if dcmi_handle is None:
        raise DCMIError(DCMI_ERR_CODE_NOT_REDAY)

    device_id_max = c_int(0)
    mcu_id = c_int(0)
    cpu_id = c_int(0)
    ret = dcmi_handle.dcmi_get_device_id_in_card(card_id, byref(device_id_max), byref(mcu_id), byref(cpu_id))

    _dcmiCheckReturn(ret)

    return device_id_max.value, mcu_id.value, cpu_id.value

def dcmi_get_device_temperature(card_id, device_id):
    if dcmi_handle is None:
        raise DCMIError(DCMI_ERR_CODE_NOT_REDAY)

    val = c_int(0)
    ret = dcmi_handle.dcmi_get_device_temperature(card_id, device_id, byref(val))

    _dcmiCheckReturn(ret)

    return val.value

def dcmi_get_device_power_info(card_id, device_id):
    if dcmi_handle is None:
        raise DCMIError(DCMI_ERR_CODE_NOT_REDAY)

    val = c_int(0)
    ret = dcmi_handle.dcmi_get_device_power_info(card_id, device_id, byref(val))

    _dcmiCheckReturn(ret)

    return val.value

def dcmi_get_device_hbm_info(card_id, device_id):
    if dcmi_handle is None:
        raise DCMIError(DCMI_ERR_CODE_NOT_REDAY)

    info = dcmi_hbm_info()
    ret = dcmi_handle.dcmi_get_device_hbm_info(card_id, device_id, byref(info))

    _dcmiCheckReturn(ret)

    return info

def dcmi_get_device_utilization_rate(card_id, device_id, input_type):
    """input_type can be 1, 2, 3, 4, 5, 6, or 8.

    1: memory
    2: AI Core
    3: AI CPU
    4: Ctrl CPU
    5: memory bandwidth
    6: on-chip memory
    8: DDR
    """
    if dcmi_handle is None:
        raise DCMIError(DCMI_ERR_CODE_NOT_REDAY)

    val = c_int(0)
    ret = dcmi_handle.dcmi_get_device_utilization_rate(card_id, device_id, input_type, byref(val))

    _dcmiCheckReturn(ret)

    return val.value

def dcmi_get_capability_group_aicore_usage(card_id, device_id, group_id):
    if dcmi_handle is None:
        raise DCMIError(DCMI_ERR_CODE_NOT_REDAY)

    val = c_int(0)
    ret = dcmi_handle.dcmi_get_capability_group_aicore_usage(card_id, device_id, group_id, byref(val))

    _dcmiCheckReturn(ret)

    return val.value