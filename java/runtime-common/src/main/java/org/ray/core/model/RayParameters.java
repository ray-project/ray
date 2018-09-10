package org.ray.core.model;

import org.ray.api.id.UniqueId;
import org.ray.util.NetworkUtil;
import org.ray.util.config.AConfig;
import org.ray.util.config.ConfigReader;

/**
 * Runtime parameters of Ray process.
 */
public class RayParameters {

  @AConfig(comment = "worker mode for this process DRIVER | WORKER | NONE")
  public WorkerMode worker_mode = WorkerMode.DRIVER;

  @AConfig(comment = "run mode for this app SINGLE_PROCESS | SINGLE_BOX | CLUSTER")
  public RunMode run_mode = RunMode.SINGLE_PROCESS;

  @AConfig(comment = "local node ip")
  public String node_ip_address = NetworkUtil.getIpAddress(null);

  @AConfig(comment = "primary redis address (e.g., 127.0.0.1:34222")
  public String redis_address = "";

  @AConfig(comment = "object store name (e.g., /tmp/store1111")
  public String object_store_name = "";

  @AConfig(comment = "object store rpc listen port")
  public int object_store_rpc_port = 32567;

  @AConfig(comment = "driver ID when the worker is served as a driver")
  public UniqueId driver_id = UniqueId.NIL;

  @AConfig(comment = "logging directory")
  public String log_dir = "/tmp/raylogs";

  @AConfig(comment = "primary redis port")
  public int redis_port = 34222;

  @AConfig(comment = "number of workers started initially")
  public int num_workers = 1;

  @AConfig(comment = "redirect err and stdout to files for newly created processes")
  public boolean redirect = true;

  @AConfig(comment = "whether to start redis shard server in addition to the primary server")
  public boolean start_redis_shards = false;

  @AConfig(comment = "whether to clean up the processes when there is a process start failure")
  public boolean cleanup = false;

  @AConfig(comment = "number of redis shard servers to be started")
  public int num_redis_shards = 0;

  @AConfig(comment = "whether this is a deployment in cluster")
  public boolean deploy = false;

  @AConfig(comment = "whether this is for python deployment")
  public boolean py = false;

  @AConfig(comment = "the max bytes of the buffer for task submit")
  public int max_submit_task_buffer_size_bytes = 2 * 1024 * 1024;

  @AConfig(comment = "default first check timeout(ms)")
  public int default_first_check_timeout_ms = 1000;

  @AConfig(comment = "default get check rate(ms)")
  public int default_get_check_interval_ms = 5000;

  @AConfig(comment = "add the jvm parameters for java worker")
  public String jvm_parameters = "";

  @AConfig(comment = "set the occupied memory(MB) size of object store")
  public int object_store_occupied_memory_MB = 1000;

  @AConfig(comment = "whether to use supreme failover strategy")
  public boolean supremeFO = false;

  @AConfig(comment = "whether to disable process failover")
  public boolean disable_process_failover = false;

  @AConfig(comment = "delay seconds under onebox before app logic for debugging")
  public int onebox_delay_seconds_before_run_app_logic = 0;

  @AConfig(comment = "raylet socket name (e.g., /tmp/raylet1111")
  public String raylet_socket_name = "";

  @AConfig(comment = "raylet rpc listen port")
  public int raylet_port = 35567;

  @AConfig(comment = "worker fetch request size")
  public int worker_fetch_request_size = 10000;

  @AConfig(comment = "static resource list of this node")
  public String static_resources = "";

  public RayParameters(ConfigReader config) {
    if (null != config) {
      String networkInterface = config.getStringValue("ray.java", "network_interface", null,
          "Network interface to be specified for host ip address(e.g., en0, eth0), may use "
              + "ifconfig to get options");
      node_ip_address = NetworkUtil.getIpAddress(networkInterface);
      config.readObject("ray.java.start", this, this);
    }
  }
}
