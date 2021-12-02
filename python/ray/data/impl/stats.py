from typing import List, Optional, Dict, Set, Tuple
import time
import collections
import numpy as np

import ray
from ray.data.block import BlockMetadata
from ray.data.impl.block_list import BlockList


def fmt(seconds: float) -> str:
    if seconds > 1:
        return str(round(seconds, 2)) + "s"
    elif seconds > 0.001:
        return str(round(seconds * 1000, 2)) + "ms"
    else:
        return str(round(seconds * 1000 * 1000, 2)) + "us"


class _DatasetStatsBuilder:
    def __init__(self, stage_name: str, parent: "DatasetStats"):
        self.stage_name = stage_name
        self.parent = parent
        self.start_time = time.monotonic()

    def build(self, final_blocks: BlockList) -> "DatasetStats":
        stats = DatasetStats(
            stages={self.stage_name: final_blocks.get_metadata()}, parent=self.parent)
        stats.time_total_s = time.monotonic() - self.start_time
        return stats


class DatasetStats:
    def __init__(self,
                 *,
                 stages: Dict[str, List[BlockMetadata]],
                 parent: Optional["DatasetStats"],
                 stats_actor=None):
        self.stages: Dict[str, List[BlockMetadata]] = stages
        self.parent: Optional["DatasetStats"] = parent
        self.number: int = 0 if not parent else parent.number + 1
        self.dataset_uuid: str = None
        self.time_total_s: float = -1
        self.stats_actor = stats_actor

        # Iteration stats, filled out if the user iterates over the dataset.
        self.iter_wait_s: float = 0
        self.iter_process_s: float = 0
        self.iter_user_s: float = 0
        self.iter_total_s: float = 0

    def child_builder(self, name: str) -> _DatasetStatsBuilder:
        return _DatasetStatsBuilder(name, self)

    def TODO(self, name: str) -> "DatasetStats":
        return DatasetStats(stages={name + "_TODO": []}, parent=self)

    def summary_string(self, already_printed: Set[str] = None) -> str:
        if self.stats_actor:
            # XXX this is a super hack, clean it up
            stats_map, self.time_total_s = ray.get(
                self.stats_actor.get.remote())
            for i, metadata in stats_map.items():
                self.stages["read"][i] = metadata
        out = ""
        if self.parent:
            out += self.parent.summary_string(already_printed)
            out += "\n"
        for stage_name, metadata in self.stages.items():
            out += "Stage {} {}: ".format(self.number, stage_name)
            if already_printed and self.dataset_uuid in already_printed:
                out += "[execution cached]"
            else:
                if already_printed is not None:
                    already_printed.add(self.dataset_uuid)
                out += self.summarize_blocks(metadata)
        out += self.summarize_iter()
        return out

    def summarize_iter(self) -> str:
        out = ""
        if self.iter_total_s or self.iter_wait_s or self.iter_process_s:
            out += "\nDataset iterator time breakdown:\n"
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


class DatasetPipelineStats:
    def __init__(self, *, max_history: int = 3):
        self.max_history: int = max_history
        self.history_buffer: List[Tuple[int, DatasetStats]] = []
        self.count = 0
        self.wait_time_s = []

        # Iteration stats, filled out if the user iterates over the pipeline.
        self.iter_wait_s: float = 0
        self.iter_user_s: float = 0
        self.iter_total_s: float = 0

    def add(self, stats: DatasetStats) -> None:
        self.history_buffer.append((self.count, stats))
        if len(self.history_buffer) > self.max_history:
            self.history_buffer.pop(0)
        self.count += 1

    def summary_string(self) -> str:
        already_printed = set()
        out = ""
        for i, stats in self.history_buffer:
            out += "== Pipeline Window {} ==\n".format(i)
            out += stats.summary_string(already_printed)
            out += "\n"
        out += "##### Overall Pipeline Time Breakdown #####\n"
        # Drop the first sample since there's no pipelining there.
        wait_time_s = self.wait_time_s[1:]
        if wait_time_s:
            out += ("* Time stalled waiting for next dataset: "
                    "{} min, {} max, {} mean, {} total\n".format(
                        fmt(min(wait_time_s)), fmt(max(wait_time_s)),
                        fmt(np.mean(wait_time_s)), fmt(sum(wait_time_s))))
        out += "* Time in dataset iterator: {}\n".format(fmt(self.iter_wait_s))
        out += "* Time in user code: {}\n".format(fmt(self.iter_user_s))
        out += "* Total time: {}\n".format(fmt(self.iter_total_s))
        return out
