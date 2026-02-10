from typing import Dict, List, Optional, Tuple

from ray._common.pydantic_compat import PYDANTIC_INSTALLED, BaseModel

if PYDANTIC_INSTALLED:

    # TODO(aguo): Use these pydantic models in the dashboard API as well.
    class ProcessGPUInfo(BaseModel):
        """
        Information about GPU usage for a single process.
        NOTE: Backwards compatibility for this model must be maintained.
        If broken, the downstream dashboard API and UI code will break.
        If you must make a backwards-incompatible change, you must make sure
        to update the relevant code in the dashboard API and UI as well.
        """

        pid: int
        gpuMemoryUsage: int  # in MB
        gpuUtilization: Optional[int] = None  # percentage

    class GpuUtilizationInfo(BaseModel):
        """
        GPU utilization information for a single GPU device.
        NOTE: Backwards compatibility for this model must be maintained.
        If broken, the downstream dashboard API and UI code will break.
        If you must make a backwards-incompatible change, you must make sure
        to update the relevant code in the dashboard API and UI as well.
        """

        index: int
        name: str
        uuid: str
        utilizationGpu: Optional[int] = None  # percentage
        memoryUsed: int  # in MB
        memoryTotal: int  # in MB
        processesPids: Optional[
            List[ProcessGPUInfo]
        ] = None  # converted to list in _compose_stats_payload

    class TpuUtilizationInfo(BaseModel):
        """
        TPU utilization information for a single TPU device.
        NOTE: Backwards compatibility for this model must be maintained.
        If broken, the downstream dashboard API and UI code will break.
        If you must make a backwards-incompatible change, you must make sure
        to update the relevant code in the dashboard API and UI as well.
        """

        index: int
        name: str
        tpuType: str
        tpuTopology: str
        tensorcoreUtilization: int  # percentage
        hbmUtilization: int  # percentage
        dutyCycle: int  # percentage
        memoryUsed: int  # in bytes
        memoryTotal: int  # in bytes

    class CpuTimes(BaseModel):
        """
        CPU times information based on psutil.scputimes.
        NOTE: Backwards compatibility for this model must be maintained.
        If broken, the downstream dashboard API and UI code will break.
        If you must make a backwards-incompatible change, you must make sure
        to update the relevant code in the dashboard API and UI as well.
        """

        user: float
        system: float
        childrenUser: float
        childrenSystem: float

    class MemoryInfo(BaseModel):
        """
        Memory information based on psutil.svmem.
        NOTE: Backwards compatibility for this model must be maintained.
        If broken, the downstream dashboard API and UI code will break.
        If you must make a backwards-incompatible change, you must make sure
        to update the relevant code in the dashboard API and UI as well.
        """

        rss: float
        vms: float
        pfaults: Optional[float] = None
        pageins: Optional[float] = None

    class MemoryFullInfo(MemoryInfo):
        """
        Memory full information based on psutil.smem.
        NOTE: Backwards compatibility for this model must be maintained.
        If broken, the downstream dashboard API and UI code will break.
        If you must make a backwards-incompatible change, you must make sure
        to update the relevant code in the dashboard API and UI as well.
        """

        uss: float

    class ProcessInfo(BaseModel):
        """
        Process information from psutil.
        NOTE: Backwards compatibility for this model must be maintained.
        If broken, the downstream dashboard API and UI code will break.
        If you must make a backwards-incompatible change, you must make sure
        to update the relevant code in the dashboard API and UI as well.
        """

        pid: int
        createTime: float
        cpuPercent: float
        cpuTimes: Optional[CpuTimes]  # psutil._pslinux.scputimes object
        cmdline: List[str]
        memoryInfo: Optional[MemoryInfo]  # psutil._pslinux.svmem object
        memoryFullInfo: Optional[MemoryFullInfo]  # psutil._pslinux.smem object
        numFds: Optional[int] = None  # Not available on Windows
        gpuMemoryUsage: Optional[int] = None  # in MB, added by _get_workers
        gpuUtilization: Optional[int] = None  # percentage, added by _get_workers

    # Note: The actual data structure uses tuples for some fields, not structured objects
    # These are type aliases to document the tuple structure
    MemoryUsage = Tuple[
        int, int, float, int
    ]  # (total, available, percent, used) in bytes
    LoadAverage = Tuple[
        Tuple[float, float, float], Optional[Tuple[float, float, float]]
    ]  # (load, perCpuLoad)
    NetworkStats = Tuple[int, int]  # (sent, received) in bytes
    DiskIOStats = Tuple[
        int, int, int, int
    ]  # (readBytes, writeBytes, readCount, writeCount)
    DiskIOSpeed = Tuple[
        float, float, float, float
    ]  # (readSpeed, writeSpeed, readIops, writeIops)
    NetworkSpeed = Tuple[float, float]  # (sendSpeed, receiveSpeed) in bytes/sec

    class DiskUsage(BaseModel):
        """
        Disk usage information based on psutil.diskusage.
        NOTE: Backwards compatibility for this model must be maintained.
        If broken, the downstream dashboard API and UI code will break.
        If you must make a backwards-incompatible change, you must make sure
        to update the relevant code in the dashboard API and UI as well.
        """

        total: int
        used: int
        free: int
        percent: float

    class StatsPayload(BaseModel):
        """
        Main stats payload returned by _compose_stats_payload.
        NOTE: Backwards compatibility for this model must be maintained.
        If broken, the downstream dashboard API and UI code will break.
        If you must make a backwards-incompatible change, you must make sure
        to update the relevant code in the dashboard API and UI as well.
        """

        now: float  # POSIX timestamp
        hostname: str
        ip: str
        cpu: float  # CPU usage percentage
        cpus: Tuple[int, int]  # (logicalCpuCount, physicalCpuCount)
        mem: MemoryUsage  # (total, available, percent, used) in bytes
        shm: Optional[int] = None  # shared memory in bytes, None if not available
        workers: List[ProcessInfo]
        raylet: Optional[ProcessInfo] = None
        agent: Optional[ProcessInfo] = None
        bootTime: float  # POSIX timestamp
        loadAvg: LoadAverage  # (load, perCpuLoad) where load is (1min, 5min, 15min)
        disk: Dict[str, DiskUsage]  # mount point -> psutil disk usage object
        diskIo: DiskIOStats  # (readBytes, writeBytes, readCount, writeCount)
        diskIoSpeed: DiskIOSpeed  # (readSpeed, writeSpeed, readIops, writeIops)
        gpus: List[GpuUtilizationInfo]
        tpus: List[TpuUtilizationInfo]
        network: NetworkStats  # (sent, received) in bytes
        networkSpeed: NetworkSpeed  # (sendSpeed, receiveSpeed) in bytes/sec
        cmdline: List[str]  # deprecated field from raylet
        gcs: Optional[ProcessInfo] = None  # only present on head node

else:
    StatsPayload = None
