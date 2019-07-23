from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import logging
import math
import multiprocessing
import numpy as np
import subprocess
import time

try:  # Python 3
    from itertools import zip_longest as zip_longest
except ImportError:  # Python 2
    from itertools import izip_longest as zip_longest

import ray
from ray.tests.cluster_utils import Cluster

logger = logging.getLogger(__name__)
logger.setLevel("INFO")

CLUSTER_NODE_PREFIX = "Node_"
LOGGING_PERIOD = 100000  # Log throughput every 100K records


# Uses Linux taskset command to pin each Python process to a CPU core
# All Ray processes must be up and running before calling this
def pin_processes():
    # Pins each python process to a specific core
    num_cpus = multiprocessing.cpu_count()
    cmd_pids = ["pgrep", "python"]
    result = subprocess.check_output(cmd_pids)
    pids = [pid for pid in str(result.decode("ascii").strip()).split("\n")]
    logger.info("Found {} python processes with PIDs: {}".format(
        len(pids), pids))
    if num_cpus < len(pids):
        raise Exception("CPUs are less than python processes.")

    cmd_pin = ["taskset", "-p", None, None]
    for i, pid in enumerate(pids):
        cmd_pin[2] = str(hex(i + 1))  # Affinity mask
        cmd_pin[3] = pid
        subprocess.call(cmd_pin)


# Returns all node ids in a Ray cluster
def get_cluster_node_ids():
    node_ids = []
    for node in ray.global_state.client_table():
        for node_id in node["Resources"].keys():
            if "CPU" not in node_id and "GPU" not in node_id:
                node_ids.append(node_id)
    return node_ids

# Generates Ray configuration object in JSON format
def generate_configuration(kv_pairs):
    config = {}
    for (key, value) in kv_pairs:
        config[key] = value
    json_config = json.dumps(config)
    logger.info("Generated configuration object: {}".format(json_config))
    return json_config

# Simulates a Ray cluster with the given number of nodes in a single node.
# Actor placement is done based on a N:1 mapping from dataflow stages to
# virtual nodes, i.e. a node might host more than one stages, but all operator
# instances of a particular stage will run at the same virtual node
def start_virtual_cluster(num_nodes, num_redis_shards, plasma_memory,
                          redis_max_memory, stage_parallelism, num_sources,
                          pin, ray_config=None):

    cluster = Cluster()  # Simulate a cluster on a single machine
    num_actors = num_sources + 1  # +1 for the progress monitor
    num_actors += sum(p for p in stage_parallelism)
    logger.info("Total number of required actors: {}".format(num_actors))
    num_cpus = multiprocessing.cpu_count()
    if num_cpus < num_actors:
        part_1 = "Dataflow contains {} actors".format(num_actors)
        part_2 = "but only {} available CPUs were found.".format(num_cpus)
        logger.error(part_1 + " " + part_2)
    # Sinks and the progress monitoring actor are excluded from
    # 'actors_per_stage'. Sinks are always placed at the last node whereas
    # the progress monitoring actor is always placed at the first node
    actors_per_stage = [num_sources]
    actors_per_stage.extend([
        stage_parallelism[n] for n in range(len(stage_parallelism) - 1)
    ])  # -1 to exclude sink actors
    stages_per_node = math.trunc(math.ceil(len(actors_per_stage) / num_nodes))
    message = "Number of stages per node: {} (incl. source stage)"
    logger.info(message.format(stages_per_node))
    assigned_actors = 0
    node_actors = 1  # The monitoring actor runs at the first node
    for i in range(num_nodes):
        remaining_actors = num_actors - assigned_actors
        if remaining_actors == 0:  # No more nodes are needed
            break
        low = i * stages_per_node
        high = (i + 1) * stages_per_node
        if high >= len(actors_per_stage):  # Last node
            node_actors += stage_parallelism[-1]  # Sinks run at the last node
            high = len(actors_per_stage)
        node_actors += sum(n for n in actors_per_stage[low:high])
        # Add cluster node
        cluster.add_node(
            # Start only one Redis instance
            num_redis_shards=num_redis_shards if i == 0 else None,
            num_cpus=node_actors,  # One CPU per actor
            num_gpus=0,
            # Specify a custom resource to allow explicit actor placement
            resources={CLUSTER_NODE_PREFIX + str(i): 100},
            object_store_memory=plasma_memory,
            redis_max_memory=redis_max_memory,
            # Overwrite internal configuration if ray_config is not None
            _internal_config=ray_config)
        assigned_actors += node_actors
        logger.info("Added node {} with {} CPUs".format(i, node_actors))
        node_actors = 0

    # Start ray
    ray.init(redis_address=cluster.redis_address)

    if pin:  # Pin python processes to CPU cores (Linux only)
        logger.info("Waiting for python processes to come up...")
        time.sleep(5)  # Wait a bit for Ray to start
        pin_processes()


# Shuts down Ray and (optionally) sleeps for a given number of seconds
def shutdown_ray(sleep=0):
    ray.shutdown()
    time.sleep(sleep)


# Parses and returns the user-defined actor placement as a mapping of the
# form 'operator_name -> cluster node ids' (one node for each instance)
def parse_placement(placement_file, cluster_node_ids):
    ids = {}
    for i, node_id in enumerate(cluster_node_ids):
        # In case the user is not aware of the actual node ids in the cluster
        # and just uses ids in [0,N), where N is the total number of nodes
        ids[str(i)] = node_id
    placement = {}  # operator name -> cluster node ids
    try:
        with open(placement_file, "r") as pf:
            for line in pf:
                name_placement = line.split(":")
                name = name_placement[0].strip()
                node_ids = name_placement[1].split(",")
                node_ids = [n.strip() for n in node_ids]
                operator_placement = []
                for node_id in node_ids:
                    new_id = ids.setdefault(node_id, node_id)
                    operator_placement.append(new_id)
                existing_lacement = placement.setdefault(
                    name, operator_placement)
                if existing_lacement != operator_placement:
                    error_message = "Looks like there are two dataflow"
                    error_message += "  operators with the same name."
                    raise Exception(error_message)
    except Exception as e:
        raise Exception(e)
    logger.info("Found explicit placement: {}".format(placement))
    return placement


# Collects sampled latencies and throughputs from
# actors in the dataflow and writes the log files
def write_log_files(all_parameters, latency_filename, throughput_filename,
                    dump_filename, dataflow):
    time.sleep(2)  # Wait a bit for everything to finish
    # Dump timeline
    if dump_filename:
        dump_filename = dump_filename + all_parameters
        ray.global_state.chrome_tracing_dump(dump_filename)

    # Collect sampled per-record latencies from all sink instances
    sink_id = dataflow.operator_id("sink")
    local_states = ray.get(dataflow.state_of(sink_id))
    latencies = [
        latency for state in local_states for latency in state
        if state is not None
    ][1:]  # Omit first measurement
    latencies = [latency for l in latencies for latency in l]
    latency_filename = latency_filename + all_parameters
    with open(latency_filename, "w") as tf:
        for value in latencies:
            tf.write(str(value) + "\n")
        if latencies:
            logger.info("Mean latency: {}".format(np.mean(latencies)))
            logger.info("Max latency: {}".format(np.max(latencies)))
            tf.write("Mean latency:{}\n".format(np.mean(latencies)))
        else:
            message = "Maybe sample period is too large?"
            logger.info("No latencies found. Is logging enabled? " + message)

    # Collect throughputs from all actors
    ids = dataflow.operator_ids()
    rates = []
    for id in ids:
        logs = ray.get(dataflow.logs_of(id))
        rates.extend(logs)
    throughput_filename = throughput_filename + all_parameters
    all_rates = []
    with open(throughput_filename, "w") as tf:
        for actor_id, in_rate, out_rate in rates:
            operator_id, instance_id = actor_id
            operator_name = dataflow.name_of(operator_id)
            for i, o in zip_longest(in_rate, out_rate, fillvalue=0):
                tf.write(
                    str("(" + str(operator_id) + ", " + str(operator_name) +
                        ", " + str(instance_id)) + ")" + " | " + str(i) +
                    " | " + str(o) + "\n")
                all_rates.append(o)
