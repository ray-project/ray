from typing import List, Tuple, Optional, Dict

from ray.data.block import BlockMetadata


class DatasetStats:
    def __init__(
            self, *,
            stages: Dict[str, List[BlockMetadata]],
            parent: Optional["DatasetStats"]):
        self.stages: Dict[str, List[BlockMetadata]] = stages
        self.parent: Optional["DatasetStats"] = parent
        self.local_wall_time_s: Optional[float] = None
        self.local_cpu_time_s: Optional[float] = None

    def summary_string(self) -> str:
        out = ""
        if self.parent:
            out += self.parent.summary_string()
            out += "\n"
        for stage_name, block_exec_stats in self.stages.items():
            out += "Stage: {}\n".format(stage_name)
            out += str(block_exec_stats)
        return out
