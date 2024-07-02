import psutil
import uuid
from ctypes import cdll, c_int, c_uint, c_char, Structure, c_ulong, c_ulonglong, byref

try:
    handle = cdll.LoadLibrary("/usr/local/Ascend/driver/lib64/driver/libdcmi.so")
    handle.dcmi_init()
except Exception:
    handle = None


class dcmi_proc_mem_info(Structure):
    _fields_ = [
        ("proc_id", c_int),
        ("proc_mem_usage", c_ulong),
    ]


class mem_struct_910(Structure):
    _fields_ = [
        ("memory_size", c_ulonglong),
        ("freq", c_uint),
        ("memory_usage", c_ulonglong),
        ("temp", c_int),
        ("bandwith_util_rate", c_uint),
    ]


class mem_struct_310(Structure):
    _fields_ = [
        ("memory_size", c_ulonglong),
        ("memory_usage", c_ulonglong),
    ]


class DcmiChipInfo(Structure):
    _fields_ = [
        ("chip_type", c_char * 32),
        ("chip_name", c_char * 32),
        ("chip_ver", c_char * 32),
        ("aicore_cnt", c_uint),
    ]


def get_npu_process_list(card_id):
    info = (dcmi_proc_mem_info * 32)()
    proc_num = c_int(0)
    handle.dcmi_get_device_resource_info(card_id, 0, byref(info), byref(proc_num))
    result = []
    for i in range(proc_num.value):
        result.append(
            {
                "pid": info[i].proc_id,
                "npuMemoryUsage": int(info[i].proc_mem_usage / 1024 / 1024),
            }
        )
    return result


def _get_npu_info():
    if handle is None:
        return []
    npus = []

    card_num = c_int(0)
    card_list = (c_int * 16)()
    handle.dcmi_get_card_list(byref(card_num), byref(card_list), 16)
    chip_info = DcmiChipInfo()
    handle.dcmi_get_device_chip_info(card_list[0], 0, byref(chip_info))
    chip_name = chip_info.chip_name.decode("utf-8")
    my_struct = None
    if chip_name.startswith("910"):
        my_struct = mem_struct_910()
    elif chip_name.startswith("310P"):
        my_struct = mem_struct_310()

    for i in range(card_num.value):
        card_id = card_list[i]
        uuid_str = str(uuid.uuid1())
        temperatureNpu = c_int(0)
        handle.dcmi_get_device_temperature(card_id, 0, byref(temperatureNpu))
        utilizationNpu = c_uint(0)
        handle.dcmi_get_device_utilization_rate(card_id, 0, 2, byref(utilizationNpu))
        powerDraw = c_int(0)
        handle.dcmi_get_device_power_info(card_id, 0, byref(powerDraw))
        handle.dcmi_get_hbm_info(card_id, 0, byref(my_struct))

        NPUProcessStats = get_npu_process_list(card_id)
        for p in NPUProcessStats:
            p1 = psutil.Process(p["pid"])
            p["username"] = p1.username()

        npu_data = {
            "uuid": uuid_str,
            "index": card_id,
            "name": f"npu_{card_id}",
            "temperatureNpu": temperatureNpu.value,
            "utilizationNpu": utilizationNpu.value,
            "powerDraw": powerDraw.value / 10,
            "memoryUsed": my_struct.memory_usage,
            "memoryTotal": my_struct.memory_size,
            "processes": NPUProcessStats,
        }
        npus.append(npu_data)
    return npus


if __name__ == "__main__":
    _get_npu_info()
