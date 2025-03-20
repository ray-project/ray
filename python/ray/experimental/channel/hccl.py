import ctypes
import torch

# Load the HCCL shared library
hccl_lib = ctypes.CDLL("libhccl.so")

HCCL_ROOT_INFO_BYTES = 4108


class HcclRootInfo(ctypes.Structure):
    """
    Structure representing the HCCL root information.
    This is used for initializing the HCCL communication group.
    """

    _fields_ = [("internal", ctypes.c_byte * HCCL_ROOT_INFO_BYTES)]


class HcclComm(ctypes.Structure):
    """
    Structure representing an HCCL communicator.
    """

    pass


class aclrtStream(ctypes.Structure):
    """
    Structure representing an ACL runtime stream.
    """

    pass


# HcclResult
HCCL_ERR_STR = {
    0: "HCCL_SUCCESS",  # success
    1: "HCCL_E_PARA",  # parameter error
    2: "HCCL_E_PTR",  # empty pointer
    3: "HCCL_E_MEMORY",  # memory error
    4: "HCCL_E_INTERNAL",  # internal error
    5: "HCCL_E_NOT_SUPPORT",  # not support feature
    6: "HCCL_E_NOT_FOUND",  # not found specific resource
    7: "HCCL_E_UNAVAIL",  # resource unavailable
    8: "HCCL_E_SYSCALL",  # call system interface error
    9: "HCCL_E_TIMEOUT",  # timeout
    10: "HCCL_E_OPEN_FILE_FAILURE",  # open file fail
    11: "HCCL_E_TCP_CONNECT",  # tcp connect fail
    12: "HCCL_E_ROCE_CONNECT",  # roce connect fail
    13: "HCCL_E_TCP_TRANSFER",  # tcp transfer fail
    14: "HCCL_E_ROCE_TRANSFER",  # roce transfer fail
    15: "HCCL_E_RUNTIME",  # call runtime api fail
    16: "HCCL_E_DRV",  # call driver api fail
    17: "HCCL_E_PROFILING",  # call profiling api fail
    18: "HCCL_E_CCE",  # call cce api fail
    19: "HCCL_E_NETWORK",  # call network api fail
    20: "HCCL_E_AGAIN",  # try again
    21: "HCCL_E_REMOTE",  # error cqe
    22: "HCCL_E_SUSPENDING",  # error communicator suspending
    23: "HCCL_E_RESERVED",  # reserved
}


# HcclDataType
TORCH_HCCL_DTYPE_MAP = {
    torch.bool: 0,  # HCCL_DATA_TYPE_INT8
    # INT types
    torch.int: 2,  # HCCL_DATA_TYPE_INT32
    torch.uint8: 7,  # HCCL_DATA_TYPE_UINT8
    torch.int8: 0,  # HCCL_DATA_TYPE_INT8
    torch.int32: 2,  # HCCL_DATA_TYPE_INT32
    torch.int64: 5,  # HCCL_DATA_TYPE_INT64
    torch.long: 5,  # HCCL_DATA_TYPE_INT64
    # FLOAT types
    torch.half: 3,  # HCCL_DATA_TYPE_FP16
    torch.float: 4,  # HCCL_DATA_TYPE_FP32
    torch.float16: 3,  # HCCL_DATA_TYPE_FP16
    torch.float32: 4,  # HCCL_DATA_TYPE_FP32
    torch.float64: 10,  # HCCL_DATA_TYPE_FP64
    torch.double: 10,  # HCCL_DATA_TYPE_FP64
    torch.bfloat16: 11,  # HCCL_DATA_TYPE_BFP16
}

"""
HcclReduceOp in hccl, which is same as ReduceOp
typedef enum {
   HCCL_REDUCE_SUM = 0,    /**< sum */
   HCCL_REDUCE_PROD = 1,   /**< prod */
   HCCL_REDUCE_MAX = 2,    /**< max */
   HCCL_REDUCE_MIN = 3,    /**< min */
   HCCL_REDUCE_RESERVED    /**< reserved */
} HcclReduceOp;
"""

# HcclResult HcclGetRootInfo(HcclRootInfo *rootInfo);
hccl_lib.HcclGetRootInfo.argtypes = [ctypes.POINTER(HcclRootInfo)]
hccl_lib.HcclGetRootInfo.restype = ctypes.c_int

# HcclResult HcclCommInitRootInfo(uint32_t nRanks, const HcclRootInfo *rootInfo,
#                                   uint32_t rank, HcclComm *comm);
hccl_lib.HcclCommInitRootInfo.argtypes = [
    ctypes.c_uint32,
    ctypes.POINTER(HcclRootInfo),
    ctypes.c_uint32,
    ctypes.POINTER(ctypes.POINTER(HcclComm)),
]
hccl_lib.HcclCommInitRootInfo.restype = ctypes.c_int

# HcclResult HcclCommDestroy(HcclComm comm);
hccl_lib.HcclCommDestroy.argtypes = [ctypes.POINTER(HcclComm)]
hccl_lib.HcclCommDestroy.restype = ctypes.c_int

# HcclResult HcclSend(void* sendBuf, uint64_t count, HcclDataType dataType,
#                       uint32_t destRank, HcclComm comm, aclrtStream stream);
hccl_lib.HcclSend.argtypes = [
    ctypes.c_void_p,
    ctypes.c_uint64,
    ctypes.c_int,
    ctypes.c_uint32,
    ctypes.POINTER(HcclComm),
    ctypes.POINTER(aclrtStream),
]
hccl_lib.HcclSend.restype = ctypes.c_int

# HcclResult HcclRecv(void* recvBuf, uint64_t count, HcclDataType dataType,
#                       uint32_t srcRank, HcclComm comm, aclrtStream stream);
hccl_lib.HcclRecv.argtypes = [
    ctypes.c_void_p,
    ctypes.c_uint64,
    ctypes.c_int,
    ctypes.c_uint32,
    ctypes.POINTER(HcclComm),
    ctypes.POINTER(aclrtStream),
]
hccl_lib.HcclRecv.restype = ctypes.c_int

# HcclResult HcclAllReduce(void *sendBuf, void *recvBuf, uint64_t count,
#                           HcclDataType dataType,  HcclReduceOp op,
#                           HcclComm comm, aclrtStream stream);
hccl_lib.HcclAllReduce.argtypes = [
    ctypes.c_void_p,
    ctypes.c_void_p,
    ctypes.c_uint64,
    ctypes.c_int,
    ctypes.c_int,
    ctypes.POINTER(HcclComm),
    ctypes.POINTER(aclrtStream),
]
hccl_lib.HcclAllReduce.restype = ctypes.c_int

# HcclResult HcclReduce(void *sendBuf, void *recvBuf, uint64_t count,
#                       HcclDataType dataType, HcclReduceOp op,
#                       uint32_t root, HcclComm comm, aclrtStream stream);
hccl_lib.HcclReduce.argtypes = [
    ctypes.c_void_p,
    ctypes.c_void_p,
    ctypes.c_uint64,
    ctypes.c_int,
    ctypes.c_int,
    ctypes.c_uint32,
    ctypes.POINTER(HcclComm),
    ctypes.POINTER(aclrtStream),
]
hccl_lib.HcclReduce.restype = ctypes.c_int

# HcclResult HcclBroadcast(void *buf, uint64_t count,
#                           HcclDataType dataType, uint32_t root,
#                           HcclComm comm, aclrtStream stream);
hccl_lib.HcclBroadcast.argtypes = [
    ctypes.c_void_p,
    ctypes.c_uint64,
    ctypes.c_int,
    ctypes.c_uint32,
    ctypes.POINTER(HcclComm),
    ctypes.POINTER(aclrtStream),
]
hccl_lib.HcclBroadcast.restype = ctypes.c_int

# HcclResult HcclReduceScatter(void *sendBuf, void *recvBuf, uint64_t recvCount,
#                               HcclDataType dataType, HcclReduceOp op,
#                               HcclComm comm, aclrtStream stream);
hccl_lib.HcclReduceScatter.argtypes = [
    ctypes.c_void_p,
    ctypes.c_void_p,
    ctypes.c_uint64,
    ctypes.c_int,
    ctypes.c_int,
    ctypes.POINTER(HcclComm),
    ctypes.POINTER(aclrtStream),
]
hccl_lib.HcclReduceScatter.restype = ctypes.c_int

# HcclResult HcclAllReduce(void *sendBuf, void *recvBuf, uint64_t count,
#                           HcclDataType dataType, HcclReduceOp op,
#                           HcclComm comm, aclrtStream stream);
hccl_lib.HcclAllReduce.argtypes = [
    ctypes.c_void_p,
    ctypes.c_void_p,
    ctypes.c_uint64,
    ctypes.c_int,
    ctypes.c_int,
    ctypes.POINTER(HcclComm),
    ctypes.POINTER(aclrtStream),
]
hccl_lib.HcclAllReduce.restype = ctypes.c_int

# const char *HcclGetErrorString(HcclResult code);
hccl_lib.HcclGetErrorString.argtypes = [ctypes.c_int]
hccl_lib.HcclGetErrorString.restype = ctypes.c_char_p

# HcclResult enum (simplified)
HCCL_SUCCESS = 0


class HcclError(RuntimeError):
    """
    Exception raised when an HCCL operation fails.
    """

    def __init__(self, status):
        self.status = status
        error_message = hccl_lib.HcclGetErrorString(status).decode()
        super(HcclError, self).__init__(
            "%s: %s" % (HCCL_ERR_STR[status], error_message)
        )

    def __reduce__(self):
        return (type(self), (self.status,))


def check_hccl_status(status):
    """
    Checks the status of an HCCL operation and raises an error if it fails.
    """
    if status != HCCL_SUCCESS:
        raise HcclError(status)


def get_unique_id():
    """
    Retrieves a communication ID for initializing HCCL communication.
    """
    root_info = HcclRootInfo()
    status = hccl_lib.HcclGetRootInfo(root_info)
    check_hccl_status(status)
    ret = tuple([root_info.internal[i] for i in range(HCCL_ROOT_INFO_BYTES)])
    return ret


def get_hccl_tensor_dtype(tensor):
    """
    Maps a PyTorch tensor data type to the corresponding HCCL data type.
    """
    return TORCH_HCCL_DTYPE_MAP[tensor.dtype]


class HCCLCommunicator:
    """
    Manages HCCL communication between multiple devices.
    """

    def __init__(self, ndev, comm_id, rank):
        self._comm = ctypes.POINTER(HcclComm)()
        root_info = HcclRootInfo()
        for i in range(HCCL_ROOT_INFO_BYTES):
            root_info.internal[i] = comm_id[i]
        status = hccl_lib.HcclCommInitRootInfo(
            ndev, ctypes.byref(root_info), rank, ctypes.byref(self._comm)
        )
        check_hccl_status(status)

    def destroy(self):
        """Destroys the HCCL communicator."""
        if self._comm:
            hccl_lib.HcclCommDestroy(self._comm)
            self._comm = None

    def send(self, sendbuf, count, datatype, peer, stream):
        """Performs an HCCL send operation."""
        status = hccl_lib.HcclSend(
            ctypes.cast(sendbuf, ctypes.c_void_p),
            count,
            datatype,
            peer,
            self._comm,
            ctypes.cast(stream, ctypes.POINTER(aclrtStream)),
        )
        check_hccl_status(status)

    def recv(self, recvbuf, count, datatype, peer, stream):
        """Performs an HCCL receive operation."""
        status = hccl_lib.HcclRecv(
            ctypes.cast(recvbuf, ctypes.c_void_p),
            count,
            datatype,
            peer,
            self._comm,
            ctypes.cast(stream, ctypes.POINTER(aclrtStream)),
        )
        check_hccl_status(status)

    def allReduce(self, sendbuf, recvbuf, count, datatype, op, stream):
        """Performs an HCCL allreduce operation."""
        status = hccl_lib.HcclAllReduce(
            ctypes.cast(sendbuf, ctypes.c_void_p),
            ctypes.cast(recvbuf, ctypes.c_void_p),
            count,
            datatype,
            op,
            self._comm,
            ctypes.cast(stream, ctypes.POINTER(aclrtStream)),
        )
        check_hccl_status(status)

    def reduce(self, sendbuf, recvbuf, count, datatype, op, root, stream):
        """Performs an HCCL reduce operation."""
        status = hccl_lib.HcclReduce(
            ctypes.cast(sendbuf, ctypes.c_void_p),
            ctypes.cast(recvbuf, ctypes.c_void_p),
            count,
            datatype,
            op,
            root,
            self._comm,
            ctypes.cast(stream, ctypes.POINTER(aclrtStream)),
        )
        check_hccl_status(status)

    def bcast(self, buf, count, datatype, root, stream):
        """Performs an HCCL bcast operation."""
        status = hccl_lib.HcclBroadcast(
            ctypes.cast(buf, ctypes.c_void_p),
            count,
            datatype,
            root,
            self._comm,
            ctypes.cast(stream, ctypes.POINTER(aclrtStream)),
        )
        check_hccl_status(status)

    def reduceScatter(self, sendbuf, recvbuf, recvcount, datatype, op, stream):
        """Performs an HCCL reducescatter operation."""
        status = hccl_lib.HcclReduceScatter(
            ctypes.cast(sendbuf, ctypes.c_void_p),
            ctypes.cast(recvbuf, ctypes.c_void_p),
            recvcount,
            datatype,
            op,
            self._comm,
            ctypes.cast(stream, ctypes.POINTER(aclrtStream)),
        )
        check_hccl_status(status)

    def allGather(self, sendbuf, recvbuf, count, datatype, stream):
        """Performs an HCCL reducegather operation."""
        status = hccl_lib.HcclAllGather(
            ctypes.cast(sendbuf, ctypes.c_void_p),
            ctypes.cast(recvbuf, ctypes.c_void_p),
            count,
            datatype,
            self._comm,
            ctypes.cast(stream, ctypes.POINTER(aclrtStream)),
        )
        check_hccl_status(status)
