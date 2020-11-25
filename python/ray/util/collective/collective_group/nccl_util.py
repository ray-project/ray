try:
    from cupy.cuda.nccl import get_unique_id
    from cupy.cuda.nccl import get_version
except ImportError:
    raise
