package org.ray.cli;

import com.beust.jcommander.JCommander;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.ray.api.id.UniqueId;
import org.ray.runtime.config.PathConfig;
import org.ray.runtime.config.RayParameters;
import org.ray.runtime.config.RunMode;
import org.ray.runtime.gcs.KeyValueStoreLink;
import org.ray.runtime.gcs.RedisClient;
import org.ray.runtime.gcs.StateStoreProxy;
import org.ray.runtime.gcs.StateStoreProxyImpl;
import org.ray.runtime.runner.RunManager;
import org.ray.runtime.runner.worker.DefaultDriver;
import org.ray.runtime.util.config.ConfigReader;
import org.ray.runtime.util.logger.RayLog;


/**
 * Ray command line interface.
 */
public class RayCli {

  private static RayCliArgs rayArgs = new RayCliArgs();

  private static RunManager startRayHead(RayParameters params, PathConfig paths,
      ConfigReader configReader) {
    RunManager manager = new RunManager(params, paths, configReader);

    try {
      manager.startRayHead();
    } catch (Exception e) {
      e.printStackTrace();
      RayLog.core.error("error at RayCli startRayHead", e);
      throw new RuntimeException("Ray head node start failed", e);
    }

    RayLog.core.info("Started Ray head node. Redis address: {}", manager.info().redisAddress);
    return manager;
  }

  private static RunManager startRayNode(RayParameters params, PathConfig paths,
      ConfigReader configReader) {
    RunManager manager = new RunManager(params, paths, configReader);

    try {
      manager.startRayNode();
    } catch (Exception e) {
      e.printStackTrace();
      RayLog.core.error("error at RayCli startRayNode", e);
      throw new RuntimeException("Ray work node start failed, err = " + e.getMessage());
    }

    RayLog.core.info("Started Ray work node.");
    return manager;
  }

  private static RunManager startProcess(CommandStart cmdStart, ConfigReader config) {
    PathConfig paths = new PathConfig(config);
    RayParameters params = new RayParameters(config);

    // Init RayLog before using it.
    RayLog.init(params.log_dir);

    RayLog.core.info("Using IP address {} for this node.", params.node_ip_address);
    RunManager manager;
    if (cmdStart.head) {
      manager = startRayHead(params, paths, config);
    } else {
      manager = startRayNode(params, paths, config);
    }
    return manager;
  }

  private static void start(CommandStart cmdStart, ConfigReader reader) {
    startProcess(cmdStart, reader);
  }

  private static void stop(CommandStop cmdStop) {
    String[] cmd = {"/bin/sh", "-c", ""};

    cmd[2] = "killall global_scheduler local_scheduler plasma_store plasma_manager";
    try {
      Runtime.getRuntime().exec(cmd);
    } catch (IOException e) {
      RayLog.core.warn("exception in killing ray processes");
    }

    cmd[2] = "kill $(ps aux | grep redis-server | grep -v grep | "
        + "awk \'{ print $2 }\') 2> /dev/null";
    try {
      Runtime.getRuntime().exec(cmd);
    } catch (IOException e) {
      RayLog.core.warn("exception in killing ray processes");
    }

    cmd[2] = "kill -9 $(ps aux | grep DefaultWorker | grep -v grep | "
        + "awk \'{ print $2 }\') 2> /dev/null";
    try {
      Runtime.getRuntime().exec(cmd);
    } catch (IOException e) {
      RayLog.core.warn("exception in killing ray processes");
    }
  }

  private static String[] buildRayRuntimeArgs(CommandSubmit cmdSubmit) {

    if (cmdSubmit.redisAddress == null) {
      throw new RuntimeException(
          "--redis-address must be specified to submit a job");      
    }

    List<String> argList = new ArrayList<String>();
    String section = "ray.java.start.";
    String overwrite = "--overwrite="
        + section + "redis_address=" + cmdSubmit.redisAddress + ";"
        + section + "run_mode=" + "CLUSTER";

    argList.add(overwrite);

    if (cmdSubmit.config != null) {
      String config = "--config=" + cmdSubmit.config;
      argList.add(config);
    }

    String[] args = new String[argList.size()];
    argList.toArray(args);

    return args;
  }
 
  private static void submit(CommandSubmit cmdSubmit, String configPath) throws Exception {
    ConfigReader config = new ConfigReader(configPath, "ray.java.start.deploy=true");
    PathConfig paths = new PathConfig(config);
    RayParameters params = new RayParameters(config);
    params.redis_address = cmdSubmit.redisAddress;
    params.run_mode = RunMode.CLUSTER;

    KeyValueStoreLink kvStore = new RedisClient();
    kvStore.setAddr(cmdSubmit.redisAddress);
    StateStoreProxy stateStoreProxy = new StateStoreProxyImpl(kvStore);
    stateStoreProxy.initializeGlobalState();

    // Init RayLog before using it.
    RayLog.init(params.log_dir);
    UniqueId appId = params.driver_id;
    String appDir = "/tmp/" + cmdSubmit.className;

    // Start driver process.
    RunManager runManager = new RunManager(params, paths, config);
    Process proc = runManager.startDriver(
        DefaultDriver.class.getName(),
        cmdSubmit.redisAddress,
        appId,
        appDir,
        params.node_ip_address,
        cmdSubmit.className,
        cmdSubmit.classArgs,
        "",
        null);

    if (null == proc) {
      RayLog.rapp.error("Failed to start driver.");
      return;
    }

    RayLog.rapp.info("Driver started.");
  }

  private static String getConfigPath(String config) {
    String configPath;

    if (config != null && !config.equals("")) {
      configPath = config;
    } else {
      configPath = System.getenv("RAY_CONFIG");
      if (configPath == null) {
        configPath = System.getProperty("ray.config");
      }
      if (configPath == null) {
        throw new RuntimeException(
            "Please set config file path in env RAY_CONFIG or property ray.config");
      }
    }
    return configPath;
  }

  public static void main(String[] args) throws Exception {

    CommandStart cmdStart = new CommandStart();
    CommandStop cmdStop = new CommandStop();
    CommandSubmit cmdSubmit = new CommandSubmit();
    JCommander rayCommander = JCommander.newBuilder().addObject(rayArgs)
        .addCommand("start", cmdStart)
        .addCommand("stop", cmdStop)
        .addCommand("submit", cmdSubmit)
        .build();
    rayCommander.parse(args);

    if (rayArgs.help) {
      rayCommander.usage();
      System.exit(0);
    }

    String cmd = rayCommander.getParsedCommand();
    if (cmd == null) {
      rayCommander.usage();
      System.exit(0);
    }

    String configPath;
    switch (cmd) {
      case "start": {
        configPath = getConfigPath(cmdStart.config);
        ConfigReader config = new ConfigReader(configPath, cmdStart.overwrite);
        start(cmdStart, config);
      }
      break;
      case "stop":
        stop(cmdStop);
        break;
      case "submit":
        configPath = getConfigPath(cmdSubmit.config);
        submit(cmdSubmit, configPath);
        break;
      default:
        rayCommander.usage();
    }
  }

}
