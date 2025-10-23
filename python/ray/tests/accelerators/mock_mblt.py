NUM_DEVICES = 4

class _Accelerator:
    def __init__(self, dev_no=0):
        if dev_no < 0 or dev_no >= NUM_DEVICES:
            raise RuntimeError("No such device")
        self.dev_no = dev_no

    def get_available_cores(self):
        # 존재 확인 용도. 내용은 중요치 않음.
        return [("dummy_core", 0)]

class accelerator:
    Accelerator = _Accelerator
