from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import boto3
import logging
import numpy as np
import time

import ray
import ray.experimental.streaming.benchmarks.macro.nexmark.data_generator as dg
import ray.experimental.streaming.benchmarks.utils as utils

from ray.experimental.streaming.benchmarks.macro.nexmark.event import Auction
from ray.experimental.streaming.benchmarks.macro.nexmark.event import Bid
from ray.experimental.streaming.benchmarks.macro.nexmark.event import Person
from ray.experimental.streaming.benchmarks.macro.nexmark.event import Record
from ray.experimental.streaming.communication import QueueConfig
from ray.experimental.streaming.streaming import Environment

logger = logging.getLogger(__name__)
logger.setLevel("INFO")

parser = argparse.ArgumentParser()

parser.add_argument("--queue-size", default=10,
                    help="the queue size in number of batches")
# The batch size is estimated based on the Bid's size, so that
# each batch corresponds to a buffer of around 32K bytes. For auctions,
# the respective batch size is 42 whereas for persons is 210.
parser.add_argument("--batch-size", default=120,
                    help="the batch size in number of elements")
parser.add_argument("--flush-timeout", default=0.1,
                    help="the timeout to flush a batch")

parser.add_argument("--simulate-cluster", default=False,
                    action='store_true',
                    help="simulate a Ray cluster on a single machine")
parser.add_argument("--pin-processes", default=False,
                    action='store_true',
                    help="whether to pin python processes to cores or not")
parser.add_argument("--records", default=-1,
                help="maximum number of records to replay from each source.")

parser.add_argument("--fetch-data", default=False,
                    action='store_true',
                    help="fecth data from S3")
parser.add_argument("--omit-extra", default=False,
                    action='store_true',
                    help="omit extra field from events")
parser.add_argument("--source-type", default="auction",
                    choices=["auctions","bids","persons"],
                    help="source type")
parser.add_argument("--sample-period", default=1000,
                    help="every how many input records latency is measured.")

parser.add_argument("--input-file", required=True,
                    help="path to the event file")
parser.add_argument("--dump-file", default="",
                    help="a file to dump the chrome timeline")

parser.add_argument("--sink-instances", default=1,
                    help="the number of sink instances after the source")




if __name__ == "__main__":

    args = parser.parse_args()

    max_queue_size = int(args.queue_size)
    max_batch_size = int(args.batch_size)
    batch_timeout = float(args.flush_timeout)

    simulate_cluster = bool(args.simulate_cluster)
    pin_processes = bool(args.pin_processes)
    max_records = int(args.records)

    fetch_data = bool(args.fetch_data)
    omit_extra = bool(args.omit_extra)
    source_type = str(args.source_type)
    sample_period = int(args.sample_period)

    in_file = str(args.input_file)
    dump_filename = str(args.dump_file)

    sink_instances = int(args.sink_instances)

    logger.info("== Parameters ==")
    logger.info("Max queue size: {}".format(max_queue_size))
    logger.info("Max batch size: {}".format(max_batch_size))
    logger.info("Batch timeout: {}".format(batch_timeout))

    logger.info("Simulate cluster: {}".format(simulate_cluster))
    logger.info("Pin processes: {}".format(pin_processes))
    logger.info("Maximum number of records: {}".format(max_records))

    logger.info("Source type: {}".format(source_type))
    logger.info("Fetch data: {}".format(fetch_data))
    logger.info("Omit extra: {}".format(omit_extra))
    logger.info("Latency sampling period: {}".format(sample_period))

    logger.info("Input file: {}".format(in_file))
    logger.info("Dump file: {}".format(dump_filename))

    logger.info("Sink instances: {}".format(sink_instances))

    if pin_processes:
        logger.info("Waiting for python processes to come up...")
        time.sleep(5)  # Wait a bit for Ray to start
        utils.pin_processes()

    if fetch_data:
        logger.info("Fetching data from S3...")
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

    # Create streaming environment, construct and run dataflow
    env = Environment()
    # Virtual queue configuration
    queue_config = QueueConfig(max_queue_size, max_batch_size, batch_timeout)
    env.set_queue_config(queue_config)
    env.enable_logging()  # Enable logging to measure latency and throughput

    source_stream = None

    if source_type == "auctions":  # Add the auction source
        source_stream = env.source(dg.NexmarkEventGenerator(in_file,
                                                "Auction",
                                                -1,  # Unbounded rate
                                                sample_period=sample_period,
                                                max_records=max_records,
                                                omit_extra=omit_extra),
                            name="auctions",
                            batch_size=max_batch_size,
                            placement=["Node_0"])
    elif source_type ==  "bids":  # Add the bid source
        source_stream = env.source(dg.NexmarkEventGenerator(in_file, "Bid",
                                                -1,  # Unbounded rate
                                                sample_period=sample_period,
                                                max_records=max_records,
                                                omit_extra=omit_extra),
                                    name="bids",
                                    batch_size=max_batch_size,
                                    placement=["Node_0"])
    else:  # Add the person source
        assert source_type == "persons"
        source_stream = env.source(dg.NexmarkEventGenerator(in_file, "Person",
                                                -1,  # Unbounded rate
                                                sample_period=sample_period,
                                                max_records=max_records,
                                                omit_extra=omit_extra),
                            name="persons",
                            batch_size=max_batch_size,
                            placement=["Node_0"])
    assert source_stream is not None

    # Connect a dummy sink to measure latency as well
    _ = source_stream.sink(dg.EventLatencySink(),
                    name="sink",
                    placement=["Node_0"] * sink_instances).set_parallelism(
                                                            sink_instances)

    start = time.time()
    dataflow = env.execute()
    # Wait until source is exhausted
    ray.get(dataflow.termination_status())

    # Collect throughputs from source
    raw_rates = ray.get(dataflow.logs_of(source_type))
    rates = [rate for _, _, out_rates in raw_rates for rate in out_rates]
    latencies = []
    raw_latencies = ray.get(dataflow.state_of("sink"))
    latencies = [l for _, latencies in raw_latencies for l in latencies]
    if len(rates) > 0:
        logger.info("Mean source rate: {}".format(np.mean(rates)))
    else:
        logger.info("No rates found. Is logging enabled?")
    if len(latencies) > 0:
        logger.info("Mean latency: {}".format(np.mean(latencies)))
    else:
        message = "Maybe the sample period is too large?"
        logger.info(
            "No latencies found. Is logging enabled? " + message)

    logger.info("Elapsed time: {}".format(time.time() - start))

    # Dump chrome timeline
    if dump_filename:
        ray.global_state.chrome_tracing_dump(dump_filename)

    utils.shutdown_ray(sleep=2)
