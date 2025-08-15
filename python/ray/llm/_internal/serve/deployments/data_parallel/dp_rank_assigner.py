import asyncio
import datetime
import logging

from ray import serve

logger = logging.getLogger(__name__)


@serve.deployment(num_replicas=1)
class DPRankAssigner:
    """
    Data Parallel Rank Assigner.

    This class is used to assign a rank to each replica in the data parallel
    deployment.
    """

    def __init__(self, dp_size: int):
        self.dp_size = dp_size
        self.lock = asyncio.Lock()
        self.next_rank = 0
        timestamp_str = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S-%f")
        self.data_parallel_master_ip_key = (
            "data_parallel_master_ip_key_DP_" + str(dp_size) + "_" + timestamp_str
        )
        self.data_parallel_port_key = (
            "data_parallel_port_key_DP_" + str(dp_size) + "_" + timestamp_str
        )
        logger.info(
            "DP rank assigner initialized with keys: \n"
            f"data_parallel_master_ip_key: {self.data_parallel_master_ip_key}\n"
            f"data_parallel_port_key: {self.data_parallel_port_key}"
        )

    async def register(self, replica_ctx: "serve.context.ReplicaContext"):
        async with self.lock:
            if self.next_rank >= self.dp_size:
                raise ValueError(
                    f"Attempted to assign rank {self.next_rank} but dp_size is {self.dp_size}"
                )
            # TODO(rui): instead of using the naive increment approach,
            # we should use the Ray Serve Replica Rank API to assign ranks.
            rank = self.next_rank
            self.next_rank += 1
            return rank

    def get_dp_metadata_keys(self):
        return self.data_parallel_master_ip_key, self.data_parallel_port_key
