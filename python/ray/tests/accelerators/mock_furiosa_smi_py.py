class _MockArch:
    """Mock for the PyO3-generated Arch enum."""

    def __init__(self, name: str = "Rngd"):
        self._name = name

    def __str__(self) -> str:
        # PyO3 unit enums commonly stringify like "Arch.Rngd".
        return f"Arch.{self._name}"


class _MockDeviceInfo:
    def __init__(self, arch_name: str = "Rngd", index: int = 0):
        self._arch_name = arch_name
        self._index = index

    def arch(self) -> _MockArch:
        return _MockArch(self._arch_name)

    def name(self) -> str:
        return f"npu{self._index}"

    def index(self) -> int:
        return self._index


class _MockDevice:
    def __init__(self, idx: int, arch_name: str = "Rngd"):
        self._idx = idx
        self._arch_name = arch_name

    def device_info(self) -> _MockDeviceInfo:
        return _MockDeviceInfo(arch_name=self._arch_name, index=self._idx)


def list_devices():
    return [_MockDevice(i) for i in range(8)]
