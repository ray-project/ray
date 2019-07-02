from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import boto3
import logging
from statistics import mean
import time

import ray
import ray.experimental.streaming.benchmarks.macro.nexmark.data_generator as dg
import ray.experimental.streaming.benchmarks.utils as utils

from ray.experimental.streaming.batched_queue import BatchedQueue
from ray.experimental.streaming.benchmarks.macro.nexmark.event import Auction
from ray.experimental.streaming.benchmarks.macro.nexmark.event import Bid
from ray.experimental.streaming.benchmarks.macro.nexmark.event import Person
from ray.experimental.streaming.benchmarks.macro.nexmark.event import Record
from ray.experimental.streaming.communication import QueueConfig
from ray.experimental.streaming.streaming import Environment

logger = logging.getLogger(__name__)
logger.setLevel("INFO")

parser = argparse.ArgumentParser()

parser.add_argument("--simulate-cluster", default=False,
                    action='store_true',
                    help="simulate a Ray cluster on a single machine")
parser.add_argument("--pin-processes", default=False,
                    action='store_true',
                    help="whether to pin python processes to cores or not")
parser.add_argument("--input-file", required=True,
                    help="path to the event file")
parser.add_argument("--dump-file", default="",
                    help="a file to dump the chrome timeline")
parser.add_argument("--queue-based", default=False,
                    action='store_true',
                    help="queue-based execution")
parser.add_argument("--fetch-data", default=False,
                    action='store_true',
                    help="fecth data from S3")
parser.add_argument("--omit-extra", default=False,
                    action='store_true',
                    help="omit extra field from events")
parser.add_argument("--records", default=-1,
                help="maximum number of records to replay from each source.")
parser.add_argument("--sink-instances", default=1,
                    help="the number of sink instances after the source")
parser.add_argument("--source-type", default="auction",
                    choices=["auctions","bids","persons"],
                    help="source type")
parser.add_argument("--sample-period", default=1000,
                    help="every how many input records latency is measured.")

# Queue-related parameters
parser.add_argument("--queue-size", default=8,
                    help="the queue size in number of batches")
parser.add_argument("--batch-size", default=1000,
                    help="the batch size in number of elements")
parser.add_argument("--flush-timeout", default=0.1,
                    help="the timeout to flush a batch")

if __name__ == "__main__":

    args = parser.parse_args()

    in_file = str(args.input_file)
    dump_filename = str(args.dump_file)
    simulate_cluster = bool(args.simulate_cluster)
    fetch_data = bool(args.fetch_data)
    omit_extra = bool(args.omit_extra)
    max_records = int(args.records)
    task_based = not bool(args.queue_based)
    sink_instances = int(args.sink_instances)
    source_type = str(args.source_type)
    sample_period = int(args.sample_period)
    max_queue_size = int(args.queue_size)
    max_batch_size = int(args.batch_size)
    batch_timeout = float(args.flush_timeout)
    pin_processes = bool(args.pin_processes)

    logger.info("== Parameters ==")
    logger.info("Simulate cluster: {}".format(simulate_cluster))
    logger.info("Source type: {}".format(source_type))
    logger.info("Fetch data: {}".format(fetch_data))
    logger.info("Omit extra: {}".format(omit_extra))
    logger.info("Maximum number of records: {}".format(max_records))
    logger.info("Task-based execution: {}".format(task_based))
    logger.info("Input file: {}".format(in_file))
    logger.info("Dump file: {}".format(dump_filename))
    logger.info("Sink instances: {}".format(sink_instances))
    logger.info("Latency sample period: {}".format(sample_period))
    logger.info("Max queue size: {}".format(max_queue_size))
    logger.info("Max batch size: {}".format(max_batch_size))
    logger.info("Batch timeout: {}".format(batch_timeout))
    logger.info("Pin processes: {}".format(pin_processes))

    if pin_processes:
        logger.info("Waiting for python processes to come up...")
        time.sleep(5)  # Wait a bit for Ray to start
        utils.pin_processes()

    if fetch_data:
        logger.info("Fetching data...")
        s3 = boto3.resource('s3')
        local_file_name = source_type + ".data"
        s3.meta.client.download_file('nexmark', source_type, local_file_name)

    if simulate_cluster:
        stage_parallelism = [sink_instances]
        utils.start_virtual_cluster(1, 1, 1000000000, 1000000000,
                                    stage_parallelism, 1,
                                    pin_processes)
    else:  # Connect to existing ray cluster
        ray.init(redis_address="localhost:6379")

    # Use pickle for BatchedQueue
    ray.register_custom_serializer(BatchedQueue, use_pickle=True)

    # Batched queue configuration
    queue_config = QueueConfig(max_queue_size,
                        max_batch_size, batch_timeout)

    # Create streaming environment, construct and run dataflow
    env = Environment()
    env.set_queue_config(queue_config)
    env.enable_logging()  # Enable logging to measure throughput

    source = None

    if source_type == "auctions":  # Add the auction source
        source = env.source(dg.NexmarkEventGenerator(in_file, "Auction",
                                                -1,  # Unbounded rate
                                                sample_period=sample_period,
                                                max_records=max_records,
                                                omit_extra=omit_extra),
                            name="auctions",
                            batch_size=max_batch_size,
                            placement=["Node_0"])
    elif source_type ==  "bids":  # Add the bid source
        source = env.source(dg.NexmarkEventGenerator(in_file, "Bid",
                                                -1,  # Unbounded rate
                                                sample_period=sample_period,
                                                max_records=max_records,
                                                omit_extra=omit_extra),
                                    name="bids",
                                    batch_size=max_batch_size,
                                    placement=["Node_0"])
    else:  # Add the person source
        assert source_type == "persons"
        source = env.source(dg.NexmarkEventGenerator(in_file, "Person",
                                                -1,  # Unbounded rate
                                                sample_period=sample_period,
                                                max_records=max_records,
                                                omit_extra=omit_extra),
                            name="persons",
                            batch_size=max_batch_size,
                            placement=["Node_0"])
    assert source is not None

    # Connect a dummy sink to measure latency as well
    _ = source.sink(dg.LatencySink(),
                    name="sink",
                    placement=["Node_0"] * sink_instances).set_parallelism(
                                                            sink_instances)

    start = time.time()
    dataflow = env.execute()
    ray.get(dataflow.termination_status())  # Wait until source is exhausted

    # Collect throughputs from source
    raw_rates = ray.get(dataflow.logs_of(source_type))
    rates = [rate for _, _, out_rates in raw_rates for rate in out_rates]
    latencies = []
    raw_latencies = ray.get(dataflow.state_of("sink"))
    latencies = [l for _, latencies in raw_latencies for l in latencies]
    if len(rates) > 0:
        logger.info("Mean source rate: {}".format(mean(rates)))
    else:
        logger.info("No rates found (maybe logging is off?)")
    if len(latencies) > 0:
        logger.info("Mean latency: {}".format(mean(latencies)))
    else:
        logger.info(
            "No latencies found (maybe the sample period is too large?)")

    logger.info("Elapsed time: {}".format(time.time() - start))

    # Dump timeline
    if dump_filename:
        ray.global_state.chrome_tracing_dump(dump_filename)

    utils.shutdown_ray(sleep=2)
