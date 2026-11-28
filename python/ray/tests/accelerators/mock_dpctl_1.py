def get_devices(backend, device_type):
    assert (backend, device_type) == ("level_zero", "gpu")
    return [SyclDevice(f"{backend}:{device_type}:{i}") for i in range(6)]


class SyclDevice:
    def __init__(self, info):
        pass

    @property
    def name(self):
        return "Intel(R) Data Center GPU Max 1550"
