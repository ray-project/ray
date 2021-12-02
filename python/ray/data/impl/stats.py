from typing import List, Optional, Dict
import collections
import numpy as np

import ray
from ray.data.block import BlockMetadata


def fmt(seconds: float) -> str:
    if seconds > 1:
        return str(round(seconds, 2)) + "s"
    elif seconds > 0.001:
        return str(round(seconds * 1000, 2)) + "ms"
    else:
        return str(round(seconds * 1000 * 1000, 2)) + "us"


class DatasetStats:
    def __init__(self,
                 *,
                 stages: Dict[str, List[BlockMetadata]],
                 parent: Optional["DatasetStats"],
                 stats_actor=None):
        self.stages: Dict[str, List[BlockMetadata]] = stages
        self.parent: Optional["DatasetStats"] = parent
        self.number: int = 0 if not parent else parent.number + 1
        self.time_total_s: float = -1
        self.stats_actor = stats_actor

        # Iteration stats, filled out if the user iterates over the dataset.
        self.iter_wait_s: float = 0
        self.iter_process_s: float = 0
        self.iter_user_s: float = 0
        self.iter_total_s: float = 0

    def summary_string(self) -> str:
        if self.stats_actor:
            # XXX this is a super hack, clean it up
            stats_map = ray.get(self.stats_actor.get.remote())
            for i, metadata in stats_map.items():
                self.stages["read"][i] = metadata
        out = ""
        if self.parent:
            out += self.parent.summary_string()
            out += "\n"
        for stage_name, metadata in self.stages.items():
            out += "Stage {} {}: ".format(self.number, stage_name)
            out += self.summarize_blocks(metadata)
        out += self.summarize_iter()
        return out

    def summarize_iter(self) -> str:
        out = ""
        if self.iter_total_s or self.iter_wait_s or self.iter_process_s:
            out += "\nOutput iterator time breakdown:\n"
            out += "* In ray.wait(): {}\n".format(fmt(self.iter_wait_s))
            out += "* In format_batch(): {}\n".format(fmt(self.iter_process_s))
            out += "* In user code: {}\n".format(fmt(self.iter_user_s))
            out += "* Total time: {}\n".format(fmt(self.iter_total_s))
        return out

    def summarize_blocks(self, blocks: List[BlockMetadata]) -> str:
        exec_stats = [m.exec_stats for m in blocks if m.exec_stats is not None]
        out = "{}/{} blocks executed in {}s\n".format(
            len(exec_stats), len(blocks), round(self.time_total_s, 2))

        if exec_stats:
            out += ("* Wall time: {} min, {} max, {} mean, {} total\n".format(
                fmt(min([e.wall_time_s for e in exec_stats])),
                fmt(max([e.wall_time_s for e in exec_stats])),
                fmt(np.mean([e.wall_time_s for e in exec_stats])),
                fmt(sum([e.wall_time_s for e in exec_stats]))))

            out += ("* CPU time: {} min, {} max, {} mean, {} total\n".format(
                fmt(min([e.cpu_time_s for e in exec_stats])),
                fmt(max([e.cpu_time_s for e in exec_stats])),
                fmt(np.mean([e.cpu_time_s for e in exec_stats])),
                fmt(sum([e.cpu_time_s for e in exec_stats]))))

        output_num_rows = [
            m.num_rows for m in blocks if m.num_rows is not None
        ]
        if output_num_rows:
            out += ("* Output num rows: {} min, {} max, {} mean, {} total\n".
                    format(
                        min(output_num_rows), max(output_num_rows),
                        int(np.mean(output_num_rows)), sum(output_num_rows)))

        output_size_bytes = [
            m.size_bytes for m in blocks if m.size_bytes is not None
        ]
        if output_size_bytes:
            out += ("* Output size bytes: {} min, {} max, {} mean, {} total\n".
                    format(
                        min(output_size_bytes), max(output_size_bytes),
                        int(np.mean(output_size_bytes)),
                        sum(output_size_bytes)))

        if exec_stats:
            node_counts = collections.defaultdict(int)
            for s in exec_stats:
                node_counts[s.node_id] += 1
            out += (
                "* Tasks per node: {} min, {} max, {} mean; {} nodes used\n".
                format(
                    min(node_counts.values()), max(node_counts.values()),
                    int(np.mean(list(node_counts.values()))),
                    len(node_counts)))

        return out
