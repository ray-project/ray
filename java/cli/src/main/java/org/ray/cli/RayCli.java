package org.ray.cli;

import com.beust.jcommander.JCommander;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import net.lingala.zip4j.core.ZipFile;
import org.ray.api.id.UniqueId;
import org.ray.core.model.RayParameters;
import org.ray.core.model.RunMode;
import org.ray.runner.RunManager;
import org.ray.runner.worker.DefaultDriver;
import org.ray.spi.KeyValueStoreLink;
import org.ray.spi.PathConfig;
import org.ray.spi.RemoteFunctionManager;
import org.ray.spi.StateStoreProxy;
import org.ray.spi.impl.NativeRemoteFunctionManager;
import org.ray.spi.impl.RedisClient;
import org.ray.spi.impl.StateStoreProxyImpl;
import org.ray.util.FileUtil;
import org.ray.util.config.ConfigReader;
import org.ray.util.logger.RayLog;


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

    RemoteFunctionManager functionManager = new NativeRemoteFunctionManager(kvStore);

    // Register app to Redis. 
    byte[] zip = FileUtil.fileToBytes(cmdSubmit.packageZip);

    String packageName = cmdSubmit.packageZip.substring(
        cmdSubmit.packageZip.lastIndexOf('/') + 1,
        cmdSubmit.packageZip.lastIndexOf('.'));

    UniqueId resourceId = functionManager.registerResource(zip);

    // Init RayLog before using it.
    RayLog.init(params.log_dir);

    RayLog.rapp.debug(
        "registerResource " + resourceId + " for package " + packageName + " done");

    UniqueId appId = params.driver_id;
    functionManager.registerApp(appId, resourceId);
    RayLog.rapp.debug("registerApp " + appId + " for resouorce " + resourceId + " done");
  
    // Unzip the package file.
    String appDir = "/tmp/" + cmdSubmit.className;
    String extPath = appDir + "/" + packageName;
    if (!FileUtil.createDir(extPath, false)) {
      throw new RuntimeException("create dir " + extPath + " failed ");
    }

    ZipFile zipFile = new ZipFile(cmdSubmit.packageZip);
    zipFile.extractAll(extPath);

    // Build the args for driver process.
    File originDirFile = new File(extPath);
    File[] topFiles = originDirFile.listFiles();
    String topDir = null;
    for (File file : topFiles) {
      if (file.isDirectory()) {
        topDir = file.getName();
      }
    }
    RayLog.rapp.debug("topDir of app classes: " + topDir);
    if (topDir == null) {
      RayLog.rapp.error("Can't find topDir of app classes, the app directory " + appDir);
      return;
    }

    String additionalClassPath = appDir + "/" + packageName  + "/" + topDir + "/*";
    RayLog.rapp.debug("Find app class path  " + additionalClassPath);

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
        additionalClassPath,
        null);

    if (null == proc) { 
      RayLog.rapp.error(
          "Create process for app " + packageName + " in local directory " + appDir
          + " failed");
      return;
    }

    RayLog.rapp
    .info("Create app " + appDir + " for package " + packageName + " succeeded");
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
