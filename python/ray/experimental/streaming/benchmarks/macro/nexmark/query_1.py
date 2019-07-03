from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import logging
import math
import string
import sys
import time

import ray
import ray.experimental.streaming.benchmarks.macro.nexmark.data_generator as dg
import ray.experimental.streaming.benchmarks.utils as utils

from ray.experimental.streaming.benchmarks.macro.nexmark.event import Bid
from ray.experimental.streaming.benchmarks.macro.nexmark.event import Record
from ray.experimental.streaming.communication import QueueConfig
from ray.experimental.streaming.streaming import Environment

logger = logging.getLogger(__name__)
logger.setLevel("INFO")

parser = argparse.ArgumentParser()

parser.add_argument("--redis-shards", default=1,
                    help="total number of Redis shards")
parser.add_argument("--redis-max-memory", default=10**9,
                    help="max amount of memory per Redis shard")
parser.add_argument("--plasma-memory", default=10**9,
                    help="amount of memory to start plasma with")
parser.add_argument("--queue-size", default=10,
                    help="the queue size in number of batches")
parser.add_argument("--batch-size", default=1000,
                    help="the batch size in number of records")
parser.add_argument("--flush-timeout", default=0.1,  # 100ms
                    help="the timeout (in seconds) to flush a batch")

parser.add_argument("--simulate-cluster", default=False,
                    action='store_true',
                    help="simulate a Ray cluster on a single machine")
parser.add_argument("--pin-processes", default=False,
                    action='store_true',
                    help="whether to pin python processes to cores or not")
parser.add_argument("--nodes", default=1,
                    help="total number of nodes in the cluster")
parser.add_argument("--records", default=-1,
                help="maximum number of records emitted by the source.")
parser.add_argument("--source-rate", default=-1,
                    type=lambda x: float(x) or
                                parser.error("Source rate cannot be zero."),
                    help="source output rate (records/s)")
parser.add_argument("--sources", default=1,
                    help="number of bid sources")

parser.add_argument("--fetch-data", default=False,
                    action='store_true',
                    help="fecth data from S3")
parser.add_argument("--omit-extra", default=False,
                    action='store_true',
                    help="omit extra field from events")
parser.add_argument("--sample-period", default=1000,
                    help="every how many input records latency is measured.")
parser.add_argument("--enable-logging", default=False,
                    action='store_true',
                    help="whether to log actor latency and throughput")

parser.add_argument("--bids-file", required=True,
                    help="Path to the bids file")
parser.add_argument("--latency-file", default="latencies",
                    help="a prefix for the latency log files")
parser.add_argument("--throughput-file", default="throughputs",
                    help="a prefix for the rate log files")
parser.add_argument("--dump-file", default="",
                    help="a prefix for the chrome dump file")
parser.add_argument("--placement-file", default="",
            help="Path to the file containing the explicit actor placement")

parser.add_argument("--map-instances", default=1,
                    help="the number of instances of the map operator")


# The mapper function that transforms bid prices from dollars to euros
def map_function(batch):
    for bid in batch:
        bid["price"] = bid["price"] * 0.9
    return batch

if __name__ == "__main__":

    args = parser.parse_args()

    num_redis_shards = int(args.redis_shards)
    redis_max_memory = int(args.redis_max_memory)
    plasma_memory = int(args.plasma_memory)
    max_queue_size = int(args.queue_size)
    max_batch_size = int(args.batch_size)
    batch_timeout = float(args.flush_timeout)

    simulate_cluster = bool(args.simulate_cluster)
    pin_processes = bool(args.pin_processes)
    num_nodes = int(args.nodes)
    max_records = int(args.records)
    source_rate = float(args.source_rate)
    num_sources = int(args.sources)

    fetch_data = bool(args.fetch_data)
    omit_extra = bool(args.omit_extra)
    sample_period = int(args.sample_period)
    logging = bool(args.enable_logging)

    bids_file = str(args.bids_file)
    latency_filename = str(args.latency_file)
    throughput_filename = str(args.throughput_file)
    dump_filename = str(args.dump_file)
    placement_file = str(args.placement_file)

    map_instances = int(args.map_instances)

    logger.info("== Parameters ==")
    logger.info("Number of Redis shards: {}".format(num_redis_shards))
    logger.info("Max memory per Redis shard: {}".format(redis_max_memory))
    logger.info("Plasma memory: {}".format(plasma_memory))
    logger.info("Max queue size: {}".format(max_queue_size))
    logger.info("Max batch size: {}".format(max_batch_size))
    logger.info("Batch timeout: {}".format(batch_timeout))

    logger.info("Simulate cluster: {}".format(simulate_cluster))
    logger.info("Pin processes: {}".format(pin_processes))
    logger.info("Number of nodes: {}".format(num_nodes))
    logger.info("Maximum number of records: {}".format(max_records))
    message = (" (as fast as it gets)") if source_rate < 0 else ""
    logger.info("Source rate: {}".format(source_rate) + message)
    logger.info("Number of sources: {}".format(num_sources))

    logger.info("Fetch data: {}".format(fetch_data))
    logger.info("Omit extra: {}".format(omit_extra))
    logger.info("Latency sampling period: {}".format(sample_period))
    logger.info("Logging: {}".format(logging))

    logger.info("Bids file: {}".format(bids_file))
    logger.info("Latency file prefix: {}".format(latency_filename))
    logger.info("Throughput file prefix: {}".format(throughput_filename))
    logger.info("Dump file prefix: {}".format(dump_filename))
    logger.info("Placement file: {}".format(placement_file))

    logger.info("Map parallelism: {}".format(map_instances))

    if fetch_data:
        logger.info("Fetching data from S3...")
        s3 = boto3.resource('s3')
        s3.meta.client.download_file('nexmark', "bids", "bids.data")

    # Number of actors per dataflow stage
    stage_parallelism = [map_instances,
                         map_instances]  # One sink per map instance

    placement = {}  # oparator name -> node ids
    if simulate_cluster:
        # Simulate a cluster with the given configuration in a single node
        utils.start_virtual_cluster(num_nodes, num_redis_shards,
                                    plasma_memory, redis_max_memory,
                                    stage_parallelism, num_sources,
                                    pin_processes)
        # Set actor placement for the virtual cluster
        num_stages = 2  # We have a source and a map stage (sinks omitted)
        stages_per_node = math.trunc(math.ceil(num_stages / num_nodes))
        source_node_id  = utils.CLUSTER_NODE_PREFIX + "0"
        placement["Bids Source"] = [source_node_id] * num_sources
        id = 1 // stages_per_node
        map_node_id = utils.CLUSTER_NODE_PREFIX + str(id)
        placement["Dollars to Euros"] = [map_node_id] * map_instances
        placement["sink"] = [map_node_id] * map_instances
    else:  # Connect to an existing cluster
        if pin_processes:
            utils.pin_processes()
        ray.init(redis_address="localhost:6379")
        if not placement_file:
            raise Exception("No actor placement specified.")
        node_ids = utils.get_cluster_node_ids()
        logger.info("Found {} cluster nodes.".format(len(node_ids)))
        # Parse user-defined actor placement for the existing cluster
        placement = utils.parse_placement(placement_file, node_ids)

    # Create streaming environment, construct and run dataflow
    env = Environment()
    # Virtual queue configuration
    queue_config = QueueConfig(max_queue_size, max_batch_size, batch_timeout)
    env.set_queue_config(queue_config)
    if logging:
        env.enable_logging()

    # Construct the custom source objects (all read from the same file)
    source_objects = [dg.NexmarkEventGenerator(bids_file, "Bid",
                                               source_rate,
                                               sample_period=sample_period,
                                               max_records=max_records,
                                               omit_extra=omit_extra)
                      for _ in range(num_sources)]
    # Add sources to the dataflow
    bid_source = env.source(source_objects,
                    name="Bids Source",
                    batch_size=max_batch_size,
                    placement=placement["Bids Source"]).set_parallelism(
                                                                  num_sources)
    # Add mapper
    output = bid_source.map(map_function,
                        name="Dollars to Euros",
                        placement=placement[
                        "Dollars to Euros"]).set_parallelism(map_instances)

    # Add a final custom sink to measure latency (if logging is enabled)
    output.sink(dg.EventLatencySink(),
                name="sink",
                placement=placement["sink"]).set_parallelism(map_instances)

    start = time.time()
    dataflow = env.execute()
    # Wait until execution finishes
    ray.get(dataflow.termination_status())

    # Write log files
    input_rate = source_rate if source_rate > 0 else "inf"
    all = "-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}-{}".format(
        num_redis_shards, redis_max_memory, plasma_memory,
        max_queue_size, max_batch_size, batch_timeout,
        simulate_cluster, pin_processes, num_nodes, max_records,
        input_rate, num_sources, omit_extra,
        sample_period, logging, map_instances
    )
    utils.write_log_files(all, latency_filename, throughput_filename,
                          dump_filename, dataflow)

    logger.info("Elapsed time: {}".format(time.time() - start))

    utils.shutdown_ray(sleep=2)
