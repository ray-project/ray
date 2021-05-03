import asyncio
import csv
from dataclasses import dataclass
import math
import os
from typing import List
import timeit

import numpy as np
from smart_open import open

import ray


# TODO(Clark): Convert this stats data model to be based on a Pandas DataFrame
# instead of nested data classes.

#
# Stats data classes.
#


@dataclass
class StageStats:
    task_durations: List[float]
    stage_duration: float


@dataclass
class MapStats(StageStats):
    read_durations: List[float]


@dataclass
class ReduceStats(StageStats):
    pass


@dataclass
class ConsumeStats(StageStats):
    consume_times: List[float]


@dataclass
class ThrottleStats:
    wait_duration: float


@dataclass
class EpochStats:
    duration: float
    map_stats: MapStats
    reduce_stats: ReduceStats
    consume_stats: ConsumeStats
    throttle_stats: ThrottleStats


@dataclass
class TrialStats:
    epoch_stats: List[EpochStats]
    duration: float


#
# Shuffling data loader stats collectors.
#


class EpochStatsCollector_:
    def __init__(self, num_maps, num_reduces, num_consumes):
        self._num_maps = num_maps
        self._num_reduces = num_reduces
        self._num_consumes = num_consumes
        self._duration = None
        self._epoch_start_time = None
        self._maps_started = 0
        self._maps_done = 0
        self._map_durations = []
        self._read_durations = []
        self._reduces_started = 0
        self._reduces_done = 0
        self._reduce_durations = []
        self._consumes_started = 0
        self._consumes_done = 0
        self._consume_times = []
        self._consume_durations = []
        self._throttle_duration = None
        self._map_stage_start_time = None
        self._reduce_stage_start_time = None
        self._consume_stage_start_time = None
        self._map_stage_duration = None
        self._reduce_stage_duration = None
        self._consume_stage_duration = None

        self._epoch_done_ev = asyncio.Event()

    def epoch_start(self):
        self._epoch_start_time = timeit.default_timer()

    def _epoch_done(self, end_time):
        assert self._epoch_start_time is not None
        self._duration = end_time - self._epoch_start_time
        self._epoch_done_ev.set()

    def map_start(self):
        if self._maps_started == 0:
            self._map_stage_start(timeit.default_timer())
        self._maps_started += 1

    def map_done(self, duration, read_duration):
        self._maps_done += 1
        self._map_durations.append(duration)
        self._read_durations.append(read_duration)
        if self._maps_done == self._num_maps:
            self._map_stage_done(timeit.default_timer())

    def reduce_start(self):
        if self._reduces_started == 0:
            self._reduce_stage_start(timeit.default_timer())
        self._reduces_started += 1

    def reduce_done(self, duration):
        self._reduces_done += 1
        self._reduce_durations.append(duration)
        if self._reduces_done == self._num_reduces:
            self._reduce_stage_done(timeit.default_timer())

    def consume_start(self):
        if self._consumes_started == 0:
            self._consume_stage_start(timeit.default_timer())
        self._consumes_started += 1

    def consume_done(self, duration, trial_time_to_consume):
        self._consumes_done += 1
        self._consume_durations.append(duration)
        self._consume_times.append(trial_time_to_consume)
        if self._consumes_done == self._num_consumes:
            self._consume_stage_done(timeit.default_timer())

    def _map_stage_start(self, start_time):
        self._map_stage_start_time = start_time

    def _map_stage_done(self, end_time):
        assert self._map_stage_start_time is not None
        self._map_stage_duration = end_time - self._map_stage_start_time

    def _reduce_stage_start(self, start_time):
        self._reduce_stage_start_time = start_time

    def _reduce_stage_done(self, end_time):
        assert self._reduce_stage_start_time is not None
        self._reduce_stage_duration = end_time - self._reduce_stage_start_time
        self._epoch_done(end_time)

    def _consume_stage_start(self, start_time):
        self._consume_stage_start_time = start_time

    def _consume_stage_done(self, end_time):
        assert self._consume_stage_start_time is not None
        self._consume_stage_duration = (
            end_time - self._consume_stage_start_time)

    def throttle_done(self, duration):
        self._throttle_duration = duration

    def get_map_stats(self):
        assert self._map_stage_duration is not None
        assert len(self._map_durations) == self._num_maps
        assert len(self._read_durations) == self._num_maps
        return MapStats(
            self._map_durations,
            self._map_stage_duration,
            self._read_durations)

    def get_reduce_stats(self):
        assert len(self._reduce_durations) == self._num_reduces
        assert self._reduce_stage_duration is not None
        return ReduceStats(self._reduce_durations, self._reduce_stage_duration)

    def get_consume_stats(self):
        assert len(self._consume_durations) == self._num_consumes
        assert len(self._consume_times) == self._num_consumes
        return ConsumeStats(
            self._consume_durations,
            self._consume_stage_duration,
            self._consume_times,
        )

    def get_throttle_stats(self):
        if self._throttle_duration is None:
            self._throttle_duration = 0
        return ThrottleStats(self._throttle_duration)

    async def get_stats(self):
        await self._epoch_done_ev.wait()
        assert self._duration is not None
        assert self._maps_done == self._num_maps
        assert self._reduces_done == self._num_reduces
        assert self._consumes_done == self._num_consumes
        return EpochStats(
            self._duration,
            self.get_map_stats(),
            self.get_reduce_stats(),
            self.get_consume_stats(),
            self.get_throttle_stats())


class TrialStatsCollector_:
    def __init__(
            self, num_epochs, num_maps, num_reduces, num_consumes):
        self._collectors = [
            EpochStatsCollector_(num_maps, num_reduces, num_consumes)
            for _ in range(num_epochs)]
        self._duration = None

        self._trial_done_ev = asyncio.Event()

    def trial_done(self, duration):
        self._duration = duration
        self._trial_done_ev.set()

    def epoch_throttle_done(self, epoch, duration):
        self._collectors[epoch].throttle_done(duration)

    async def epoch_start(self, epoch):
        self._collectors[epoch].epoch_start()

    def map_start(self, epoch):
        self._collectors[epoch].map_start()

    def map_done(self, epoch, duration, read_duration):
        self._collectors[epoch].map_done(duration, read_duration)

    def reduce_start(self, epoch):
        self._collectors[epoch].reduce_start()

    def reduce_done(self, epoch, duration):
        self._collectors[epoch].reduce_done(duration)

    def consume_start(self, epoch):
        self._collectors[epoch].consume_start()

    def consume_done(self, epoch, duration, trial_time_to_consume):
        self._collectors[epoch].consume_done(duration, trial_time_to_consume)

    async def get_stats(self):
        await self._trial_done_ev.wait()
        epoch_stats = await asyncio.gather(
            *[
                collector.get_stats()
                for collector in self._collectors])
        assert self._duration is not None
        return TrialStats(
            epoch_stats,
            self._duration)


TrialStatsCollector = ray.remote(TrialStatsCollector_)


#
# Stats processing utilities.
#


def process_stats(
        all_stats,
        overwrite_stats,
        stats_dir,
        no_epoch_stats,
        no_consume_stats,
        use_from_disk_shuffler,
        num_rows,
        num_row_groups_per_file,
        batch_size,
        num_reducers,
        num_trainers,
        num_epochs,
        max_concurrent_epochs):
    stats_list, store_stats_list = zip(*all_stats)
    times = [stats.duration for stats in stats_list]
    mean = np.mean(times)
    std = np.std(times)
    store_bytes_used = [
        getattr(store_stats_sample, "object_store_bytes_used", 0)
        for trial_store_stats in store_stats_list
        for _, store_stats_sample in trial_store_stats]
    num_store_stats_samples = sum(
        len(trial_store_stats) for trial_store_stats in store_stats_list)
    max_utilization = human_readable_size(np.max(store_bytes_used))
    throughput_std = np.std([num_epochs * num_rows / time for time in times])
    batch_throughput_std = np.std([
        (num_epochs * num_rows / batch_size) / time for time in times])
    print(f"\nMean over {len(times)} trials: {mean:.3f}s +- {std}")
    print(f"Mean throughput over {len(times)} trials: "
          f"{num_epochs * num_rows / mean:.2f} rows/s +- {throughput_std:.2f}")
    print(f"Mean batch throughput over {len(times)} trials: "
          f"{(num_epochs * num_rows / batch_size) / mean:.2f} batches/s +- "
          f"{batch_throughput_std:.2f}")
    print(f"Max object store utilization over {num_store_stats_samples} "
          f"samples: {max_utilization}\n")

    shuffle_type = (
        "from_disk" if use_from_disk_shuffler else "from_memory")
    overwrite_stats = overwrite_stats
    if stats_dir.startswith("s3"):
        write_mode = "w"
    else:
        write_mode = "w+" if overwrite_stats else "a+"
    stats_dir = stats_dir
    hr_num_rows = human_readable_big_num(num_rows)
    hr_batch_size = human_readable_big_num(batch_size)
    filename = (
        f"trial_stats_{shuffle_type}_{hr_num_rows}_rows_{hr_batch_size}_"
        "batch_size.csv")
    filename = os.path.join(stats_dir, filename)
    write_header = (
        overwrite_stats or not os.path.exists(filename) or
        os.path.getsize(filename) == 0)
    print(f"Writing out trial stats to {filename}.")
    # TODO(Clark): Add per-mapper, per-reducer, and per-trainer stat CSVs.

    # TODO(Clark): Add throttling stats to benchmark stats.
    with open(filename, write_mode) as f:
        fieldnames = [
            "shuffle_type",
            "num_row_groups_per_file",
            "num_reducers",
            "num_trainers",
            "num_epochs",
            "max_concurrent_epochs",
            "trial",
            "duration",
            "row_throughput",
            "batch_throughput",
            "avg_object_store_utilization_duration",  # across epochs
            "max_object_store_utilization_duration",  # across epochs
            "min_object_store_utilization_duration",  # across epochs
            "avg_epoch_duration",  # across epochs
            "std_epoch_duration",  # across epochs
            "max_epoch_duration",  # across epochs
            "min_epoch_duration",  # across epochs
            "avg_map_stage_duration",  # across epochs
            "std_map_stage_duration",  # across epochs
            "max_map_stage_duration",  # across epochs
            "min_map_stage_duration",  # across epochs
            "avg_reduce_stage_duration",  # across epochs
            "std_reduce_stage_duration",  # across epochs
            "max_reduce_stage_duration",  # across epochs
            "min_reduce_stage_duration",  # across epochs
            "avg_consume_stage_duration",  # across epochs
            "std_consume_stage_duration",  # across epochs
            "max_consume_stage_duration",  # across epochs
            "min_consume_stage_duration",  # across epochs
            "avg_map_task_duration",  # across epochs, mappers
            "std_map_task_duration",  # across epochs, mappers
            "max_map_task_duration",  # across epochs, mappers
            "min_map_task_duration",  # across epochs, mappers
            "avg_read_duration",  # across epochs, mappers
            "std_read_duration",  # across epochs, mappers
            "max_read_duration",  # across epochs, mappers
            "min_read_duration",  # across epochs, mappers
            "avg_reduce_task_duration",  # across epochs, reducers
            "std_reduce_task_duration",  # across epochs, reducers
            "max_reduce_task_duration",  # across epochs, reducers
            "min_reduce_task_duration",  # across epochs, reducers
            "avg_consume_task_duration",  # across epochs, consumers
            "std_consume_task_duration",  # across epochs, consumers
            "max_consume_task_duration",  # across epochs, consumers
            "min_consume_task_duration",  # across epochs, consumers
            "avg_time_to_consume",  # across epochs, consumers
            "std_time_to_consume",  # across epochs, consumers
            "max_time_to_consume",  # across epochs, consumers
            "min_time_to_consume"]  # across epochs, consumers
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        row = {
            "shuffle_type": shuffle_type,
            "num_row_groups_per_file": num_row_groups_per_file,
            "num_reducers": num_reducers,
            "num_trainers": num_trainers,
            "num_epochs": num_epochs,
            "max_concurrent_epochs": max_concurrent_epochs,
        }
        for trial, (stats, store_stats) in enumerate(all_stats):
            row["trial"] = trial
            row["duration"] = stats.duration
            row["row_throughput"] = num_epochs * num_rows / stats.duration
            row["batch_throughput"] = (
                num_epochs * num_rows / batch_size / stats.duration)

            # Get the stats from each epoch.
            epoch_durations = []
            map_task_durations = []
            map_stage_durations = []
            read_durations = []
            reduce_task_durations = []
            reduce_stage_durations = []
            consume_task_durations = []
            consume_stage_durations = []
            consume_times = []
            for epoch_stats in stats.epoch_stats:
                epoch_durations.append(epoch_stats.duration)
                # Map stage.
                map_stats = epoch_stats.map_stats
                map_stage_durations.append(map_stats.stage_duration)
                map_task_durations.extend(map_stats.task_durations)
                read_durations.extend(map_stats.read_durations)
                # Reduce stage.
                reduce_stats = epoch_stats.reduce_stats
                reduce_stage_durations.append(reduce_stats.stage_duration)
                reduce_task_durations.extend(reduce_stats.task_durations)
                # Consume stage.
                consume_stats = epoch_stats.consume_stats
                consume_task_durations.extend(consume_stats.task_durations)
                consume_stage_durations.append(
                    consume_stats.stage_duration)
                consume_times.extend(consume_stats.consume_times)

            # Trial store stats.
            store_bytes_used = [
                getattr(store_stats_, "object_store_bytes_used", 0)
                for _, store_stats_ in store_stats]
            row["avg_object_store_utilization_duration"] = np.mean(
                store_bytes_used)
            row["max_object_store_utilization_duration"] = np.max(
                store_bytes_used)
            row["min_object_store_utilization_duration"] = np.min(
                store_bytes_used)

            # Calculate the trial stats.
            row["avg_epoch_duration"] = np.mean(epoch_durations)
            row["std_epoch_duration"] = np.std(epoch_durations)
            row["max_epoch_duration"] = np.max(epoch_durations)
            row["min_epoch_duration"] = np.min(epoch_durations)
            row["avg_map_stage_duration"] = np.mean(map_stage_durations)
            row["std_map_stage_duration"] = np.std(map_stage_durations)
            row["max_map_stage_duration"] = np.max(map_stage_durations)
            row["min_map_stage_duration"] = np.min(map_stage_durations)
            row["avg_map_task_duration"] = np.mean(map_task_durations)
            row["std_map_task_duration"] = np.std(map_task_durations)
            row["max_map_task_duration"] = np.max(map_task_durations)
            row["min_map_task_duration"] = np.min(map_task_durations)
            row["avg_reduce_stage_duration"] = np.mean(reduce_stage_durations)
            row["std_reduce_stage_duration"] = np.std(reduce_stage_durations)
            row["max_reduce_stage_duration"] = np.max(reduce_stage_durations)
            row["min_reduce_stage_duration"] = np.min(reduce_stage_durations)
            row["avg_consume_stage_duration"] = np.mean(
                consume_stage_durations)
            row["std_consume_stage_duration"] = np.std(consume_stage_durations)
            row["max_consume_stage_duration"] = np.max(consume_stage_durations)
            row["min_consume_stage_duration"] = np.min(consume_stage_durations)
            row["avg_reduce_task_duration"] = np.mean(reduce_task_durations)
            row["std_reduce_task_duration"] = np.std(reduce_task_durations)
            row["max_reduce_task_duration"] = np.max(reduce_task_durations)
            row["min_reduce_task_duration"] = np.min(reduce_task_durations)
            row["avg_consume_task_duration"] = np.mean(consume_task_durations)
            row["std_consume_task_duration"] = np.std(consume_task_durations)
            row["max_consume_task_duration"] = np.max(consume_task_durations)
            row["min_consume_task_duration"] = np.min(consume_task_durations)
            row["avg_time_to_consume"] = np.mean(consume_times)
            row["std_time_to_consume"] = np.std(consume_times)
            row["max_time_to_consume"] = np.max(consume_times)
            row["min_time_to_consume"] = np.min(consume_times)
            row["avg_read_duration"] = np.mean(read_durations)
            row["std_read_duration"] = np.std(read_durations)
            row["max_read_duration"] = np.max(read_durations)
            row["min_read_duration"] = np.min(read_durations)
            writer.writerow(row)

    if not no_epoch_stats:
        filename = (
            f"epoch_stats_{shuffle_type}_{hr_num_rows}_rows_{hr_batch_size}_"
            "batch_size.csv")
        filename = os.path.join(stats_dir, filename)
        write_header = (
            overwrite_stats or not os.path.exists(filename) or
            os.path.getsize(filename) == 0)
        print(f"Writing out epoch stats to {filename}.")
        # TODO(Clark): Add per-mapper, per-reducer, and per-trainer stat CSVs.
        with open(filename, write_mode) as f:
            fieldnames = [
                "shuffle_type",
                "num_row_groups_per_file",
                "num_reducers",
                "num_trainers",
                "num_epochs",
                "max_concurrent_epochs",
                "trial",
                "epoch",
                "duration",
                "map_stage_duration",
                "reduce_stage_duration",
                "consume_stage_duration",
                "avg_map_task_duration",  # across mappers
                "std_map_task_duration",  # across mappers
                "max_map_task_duration",  # across mappers
                "min_map_task_duration",  # across mappers
                "avg_read_duration",  # across mappers
                "std_read_duration",  # across mappers
                "max_read_duration",  # across mappers
                "min_read_duration",  # across mappers
                "avg_reduce_task_duration",  # across reducers
                "std_reduce_task_duration",  # across reducers
                "max_reduce_task_duration",  # across reducers
                "min_reduce_task_duration",  # across reducers
                "avg_consume_task_duration",  # across consumers
                "std_consume_task_duration",  # across consumers
                "max_consume_task_duration",  # across consumers
                "min_consume_task_duration",  # across consumers
                "avg_time_to_consume",  # across consumers
                "std_time_to_consume",  # across consumers
                "max_time_to_consume",  # across consumers
                "min_time_to_consume"]  # across consumers
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if write_header:
                writer.writeheader()
            row = {
                "shuffle_type": shuffle_type,
                "num_row_groups_per_file": num_row_groups_per_file,
                "num_reducers": num_reducers,
                "num_trainers": num_trainers,
                "num_epochs": num_epochs,
                "max_concurrent_epochs": max_concurrent_epochs,
            }
            for trial, (trial_stats, trial_store_stats) in enumerate(
                    all_stats):
                row["trial"] = trial
                for epoch, stats in enumerate(trial_stats.epoch_stats):
                    row["epoch"] = epoch
                    row["duration"] = stats.duration
                    row["map_stage_duration"] = (
                        stats.map_stats.stage_duration)
                    row["reduce_stage_duration"] = (
                        stats.reduce_stats.stage_duration)
                    row["consume_stage_duration"] = (
                        stats.consume_stats.stage_duration)
                    row["avg_map_task_duration"] = np.mean(
                        stats.map_stats.task_durations)
                    row["std_map_task_duration"] = np.std(
                        stats.map_stats.task_durations)
                    row["max_map_task_duration"] = np.max(
                        stats.map_stats.task_durations)
                    row["min_map_task_duration"] = np.min(
                        stats.map_stats.task_durations)
                    row["avg_read_duration"] = np.mean(
                        stats.map_stats.read_durations)
                    row["std_read_duration"] = np.std(
                        stats.map_stats.read_durations)
                    row["max_read_duration"] = np.max(
                        stats.map_stats.read_durations)
                    row["min_read_duration"] = np.min(
                        stats.map_stats.read_durations)
                    row["avg_reduce_task_duration"] = np.mean(
                        stats.reduce_stats.task_durations)
                    row["std_reduce_task_duration"] = np.std(
                        stats.reduce_stats.task_durations)
                    row["max_reduce_task_duration"] = np.max(
                        stats.reduce_stats.task_durations)
                    row["min_reduce_task_duration"] = np.min(
                        stats.reduce_stats.task_durations)
                    row["avg_consume_task_duration"] = np.mean(
                        stats.consume_stats.task_durations)
                    row["std_consume_task_duration"] = np.std(
                        stats.consume_stats.task_durations)
                    row["max_consume_task_duration"] = np.max(
                        stats.consume_stats.task_durations)
                    row["min_consume_task_duration"] = np.min(
                        stats.consume_stats.task_durations)
                    row["avg_time_to_consume"] = np.mean(
                        stats.consume_stats.consume_times)
                    row["std_time_to_consume"] = np.std(
                        stats.consume_stats.consume_times)
                    row["max_time_to_consume"] = np.max(
                        stats.consume_stats.consume_times)
                    row["min_time_to_consume"] = np.min(
                        stats.consume_stats.consume_times)
                    writer.writerow(row)
    if not no_consume_stats:
        filename = (
            f"consume_stats_{shuffle_type}_{hr_num_rows}_rows_{hr_batch_size}_"
            "batch_size.csv")
        filename = os.path.join(stats_dir, filename)
        print(f"Writing out consume stats to {filename}.")
        write_header = (
            overwrite_stats or not os.path.exists(filename) or
            os.path.getsize(filename) == 0)
        with open(filename, write_mode) as f:
            fieldnames = [
                "shuffle_type",
                "num_row_groups_per_file",
                "num_reducers",
                "num_trainers",
                "num_epochs",
                "max_concurrent_epochs",
                "trial",
                "epoch",
                "consumer",
                "consume_task_duration",
                "consume_time"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if write_header:
                writer.writeheader()
            row = {
                "shuffle_type": shuffle_type,
                "num_row_groups_per_file": num_row_groups_per_file,
                "num_reducers": num_reducers,
                "num_trainers": num_trainers,
                "num_epochs": num_epochs,
                "max_concurrent_epochs": max_concurrent_epochs,
            }
            for trial, (trial_stats, trial_store_stats) in enumerate(
                    all_stats):
                row["trial"] = trial
                for epoch, stats in enumerate(trial_stats.epoch_stats):
                    row["epoch"] = epoch
                    consume_stats = stats.consume_stats
                    consume_task_durations = consume_stats.task_durations
                    consume_times = consume_stats.consume_times
                    for consumer, (duration, consume_time) in enumerate(
                            zip(consume_task_durations, consume_times)):
                        row["consumer"] = consumer
                        row["consume_task_duration"] = duration
                        row["consume_time"] = consume_time
                        writer.writerow(row)


UNITS = ["", "K", "M", "B", "T", "Q"]


def human_readable_big_num(num):
    idx = int(math.log10(num) // 3)
    unit = UNITS[idx]
    new_num = num / 10 ** (3 * idx)
    if new_num % 1 == 0:
        return f"{int(new_num)}{unit}"
    else:
        return f"{new_num:.1f}{unit}"


def human_readable_size(num, precision=1, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0 or unit == "Zi":
            break
        num /= 1024.0
    return f"{num:.{precision}f}{unit}{suffix}"


_STUB = None
MAX_MESSAGE_LENGTH = ray._config.max_grpc_message_size()


def _get_raylet_address():
    node = ray.worker.global_worker.node
    return f"{node.raylet_ip_address}:{node.node_manager_port}"


def _get_raylet_stub():
    global _STUB
    if _STUB is None:
        import grpc
        from ray.core.generated import node_manager_pb2_grpc
        raylet_address = _get_raylet_address()
        channel = grpc.insecure_channel(
            raylet_address,
            options=[
                ("grpc.max_send_message_length", MAX_MESSAGE_LENGTH),
                ("grpc.max_receive_message_length", MAX_MESSAGE_LENGTH),
            ],
        )
        _STUB = node_manager_pb2_grpc.NodeManagerServiceStub(channel)
    return _STUB


def get_store_stats(timeout=5):
    from ray.core.generated import node_manager_pb2

    stub = _get_raylet_stub()
    reply = stub.FormatGlobalMemoryInfo(
        node_manager_pb2.FormatGlobalMemoryInfoRequest(
            include_memory_info=False),
        timeout=timeout)
    return reply.store_stats


def collect_store_stats(
        store_stats,
        done_event,
        utilization_sample_period,
        do_print=True,
        fetch_timeout=10):
    is_done = False
    while not is_done:
        get_time = timeit.default_timer()
        stats = get_store_stats(timeout=fetch_timeout)
        if do_print:
            print(f"Current object store utilization: "
                  f"{human_readable_size(stats.object_store_bytes_used)}")
        store_stats.append((get_time, stats))
        is_done = done_event.wait(timeout=utilization_sample_period)
