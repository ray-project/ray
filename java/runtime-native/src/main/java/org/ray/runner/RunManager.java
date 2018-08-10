package org.ray.runner;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.ray.api.UniqueID;
import org.ray.core.model.RayParameters;
import org.ray.core.model.RunMode;
import org.ray.runner.RunInfo.ProcessType;
import org.ray.spi.PathConfig;
import org.ray.spi.model.AddressInfo;
import org.ray.util.ResourceUtil;
import org.ray.util.StringUtil;
import org.ray.util.config.ConfigReader;
import org.ray.util.logger.RayLog;
import redis.clients.jedis.Jedis;

/**
 * Ray service management on one box.
 */
public class RunManager {

  public static final int INT16_MAX = 32767;

  private RayParameters params;

  private PathConfig paths;

  private ConfigReader configReader;

  private String procStdoutFileName = "";

  private String procStderrFileName = "";

  private RunInfo runInfo = new RunInfo();

  public RunManager(RayParameters params, PathConfig paths, ConfigReader configReader) {
    this.params = params;
    this.paths = paths;
    this.configReader = configReader;
  }

  private static boolean killProcess(Process p) {
    if (p.isAlive()) {
      p.destroy();
      return true;
    } else {
      return false;
    }
  }

  public RunInfo info() {
    return runInfo;
  }

  public PathConfig getPathManager() {
    return paths;
  }

  public String getProcStdoutFileName() {
    return procStdoutFileName;
  }

  public String getProcStderrFileName() {
    return procStderrFileName;
  }

  public void startRayHead() throws Exception {
    if (params.redis_address.length() != 0) {
      throw new Exception("Redis address must be empty in head node.");
    }
    if (params.num_redis_shards <= 0) {
      params.num_redis_shards = 1;
    }
    if (params.num_local_schedulers <= 0) {
      params.num_local_schedulers = 1;
    }

    params.start_workers_from_local_scheduler = params.run_mode != RunMode.SINGLE_BOX;

    params.include_global_scheduler = true;
    params.start_redis_shards = true;

    startRayProcesses();
  }

  public void startRayNode() throws Exception {
    if (params.redis_address.length() == 0) {
      throw new Exception("Redis address cannot be empty in non-head node.");
    }
    if (params.num_redis_shards != 0) {
      throw new Exception("Number of redis shards should be zero in non-head node.");
    }
    if (params.num_local_schedulers <= 0) {
      params.num_local_schedulers = 1;
    }

    //params.start_workers_from_local_scheduler = true;
    params.include_global_scheduler = false;
    params.start_redis_shards = false;

    startRayProcesses();
  }

  public Process startDriver(String mainClass, String redisAddress, UniqueID driverId,
                             String workDir, String ip,
                             String driverClass, String driverArgs, String additonalClassPaths, 
                             String additionalConfigs) {
    String driverConfigs =
        "ray.java.start.driver_id=" + driverId + ";ray.java.start.driver_class=" + driverClass;
    if (driverArgs != null) {
      driverConfigs += ";ray.java.start.driver_args=" + driverArgs;
    }

    if (null != additionalConfigs) {
      additionalConfigs += ";" + driverConfigs;
    } else {
      additionalConfigs = driverConfigs;
    }

    return startJavaProcess(
        RunInfo.ProcessType.PT_DRIVER,
        mainClass,
        additonalClassPaths,
        additionalConfigs,
        "",
        workDir,
        ip,
        redisAddress,
        true,
        false,
        null
    );
  }

  private Process startJavaProcess(RunInfo.ProcessType pt, String mainClass,
                                   String additonalClassPaths, String additionalConfigs,
                                   String additionalJvmArgs, String workDir, String ip, String
                                       redisAddr, boolean redirect,
                                   boolean cleanup, String agentlibAddr) {

    String cmd = buildJavaProcessCommand(pt, mainClass, additonalClassPaths, additionalConfigs,
        additionalJvmArgs, workDir, ip, redisAddr, agentlibAddr);
    return startProcess(cmd.split(" "), null, pt, workDir, redisAddr, ip, redirect, cleanup);
  }

  private String buildJavaProcessCommand(
      RunInfo.ProcessType pt, String mainClass, String additionalClassPaths,
      String additionalConfigs,
      String additionalJvmArgs, String workDir, String ip, String redisAddr, String agentlibAddr) {
    String cmd = "java -ea -noverify " + params.jvm_parameters + " ";
    if (agentlibAddr != null && !agentlibAddr.equals("")) {
      cmd += " -agentlib:jdwp=transport=dt_socket,address=" + agentlibAddr + ",server=y,suspend=n";
    }

    cmd += " -Djava.library.path=" + StringUtil.mergeArray(paths.java_jnilib_paths, ":");
    cmd += " -classpath " + StringUtil.mergeArray(paths.java_class_paths, ":");

    if (additionalClassPaths.length() > 0) {
      cmd += ":" + additionalClassPaths;
    }

    if (additionalJvmArgs.length() > 0) {
      cmd += " " + additionalJvmArgs;
    }

    cmd += " " + mainClass;

    String section = "ray.java.start.";
    cmd += " --config=" + configReader.filePath();
    cmd += " --overwrite="
        + section + "node_ip_address=" + ip + ";"
        + section + "redis_address=" + redisAddr + ";"
        + section + "working_directory=" + workDir + ";"
        + section + "run_mode=" + params.run_mode;

    if (additionalConfigs.length() > 0) {
      cmd += ";" + additionalConfigs;
    }

    return cmd;
  }

  private Process startProcess(String[] cmd, Map<String, String> env, RunInfo.ProcessType type,
                               String workDir,
                               String redisAddress, String ip, boolean redirect,
                               boolean cleanup) {
    File wdir = new File(workDir);
    if (!wdir.exists()) {
      wdir.mkdirs();
    }

    int processIndex = runInfo.allProcesses.get(type.ordinal()).size();
    ProcessBuilder builder;
    List<String> newCmd = Arrays.stream(cmd).filter(s -> s.length() > 0)
        .collect(Collectors.toList());
    builder = new ProcessBuilder(newCmd);
    builder.directory(new File(workDir));
    if (redirect) {
      String stdoutFile;
      String stderrFile;
      stdoutFile = workDir + "/" + processIndex + ".out.txt";
      stderrFile = workDir + "/" + processIndex + ".err.txt";
      builder.redirectOutput(new File(stdoutFile));
      builder.redirectError(new File(stderrFile));
      List<String> stdFileList = new ArrayList<>();
      stdFileList.add(stdoutFile);
      stdFileList.add(stderrFile);
      record_log_files_in_redis(redisAddress, ip, stdFileList);
      procStdoutFileName = stdoutFile;
      procStderrFileName = stderrFile;
    }

    if (env != null && !env.isEmpty()) {
      builder.environment().putAll(env);
    }

    Process p = null;
    try {
      p = builder.start();
    } catch (IOException e) {
      RayLog.core
          .error("Start process " + Arrays.toString(cmd).replace(',', ' ') + " in working dir '"
                  + workDir + "' failed",
              e);
      return null;
    }

    RayLog.core.info(
        "Start process " + p.hashCode() + " OK, cmd = " + Arrays.toString(cmd).replace(',', ' ')
            + ", working dir = '" + workDir + "'" + (redirect ? ", redirect" : ", no redirect"));

    if (cleanup) {
      runInfo.toBeCleanedProcesses.get(type.ordinal()).add(p);
    }

    ProcessInfo processInfo = new ProcessInfo();
    processInfo.cmd = cmd;
    processInfo.type = type;
    processInfo.workDir = workDir;
    processInfo.redisAddress = redisAddress;
    processInfo.ip = ip;
    processInfo.redirect = redirect;
    processInfo.cleanup = cleanup;
    processInfo.process = p;
    runInfo.allProcesses.get(type.ordinal()).add(processInfo);

    return p;
  }

  private void record_log_files_in_redis(String redisAddress, String nodeIpAddress,
                                         List<String> logfiles) {
    if (redisAddress != null && !redisAddress.isEmpty() && nodeIpAddress != null
        && !nodeIpAddress.isEmpty() && logfiles.size() > 0) {
      String[] ipPort = redisAddress.split(":");
      Jedis jedisClient = new Jedis(ipPort[0], Integer.parseInt(ipPort[1]));
      String logFileListKey = String.format("LOG_FILENAMES:{%s}", nodeIpAddress);
      for (String logfile : logfiles) {
        jedisClient.rpush(logFileListKey, logfile);
      }
      jedisClient.close();
    }
  }

  public void startRayProcesses() {
    Jedis redisClient = null;

    RayLog.core.info("start ray processes @ " + params.node_ip_address + " ...");

    // start primary redis
    if (params.redis_address.length() == 0) {
      List<String> primaryShards = startRedis(params.working_directory + "/redis",
          params.node_ip_address, params.redis_port, 1, params.redirect, params.cleanup);
      params.redis_address = primaryShards.get(0);

      String[] args = params.redis_address.split(":");
      redisClient = new Jedis(args[0], Integer.parseInt(args[1]));

      // Register the number of Redis shards in the primary shard, so that clients
      // know how many redis shards to expect under RedisShards.
      redisClient.set("NumRedisShards", Integer.toString(params.num_redis_shards));
    } else {
      String[] args = params.redis_address.split(":");
      redisClient = new Jedis(args[0], Integer.parseInt(args[1]));
    }
    runInfo.redisAddress = params.redis_address;

    // start redis shards
    if (params.start_redis_shards) {
      runInfo.redisShards = startRedis(params.working_directory + "/redis/shards",
          params.node_ip_address, params.redis_port + 1, params.num_redis_shards,
          params.redirect,
          params.cleanup);

      // Store redis shard information in the primary redis shard.
      for (int i = 0; i < runInfo.redisShards.size(); i++) {
        String addr = runInfo.redisShards.get(i);
        redisClient.rpush("RedisShards", addr);
      }
    }
    redisClient.close();

    // start global scheduler
    if (params.include_global_scheduler && !params.use_raylet) {
      startGlobalScheduler(params.working_directory + "/globalScheduler",
          params.redis_address, params.node_ip_address, params.redirect, params.cleanup);
    }

    // prepare parameters for node processes
    if (params.num_cpus.length == 0) {
      params.num_cpus = new int[params.num_local_schedulers];
      for (int i = 0; i < params.num_local_schedulers; i++) {
        params.num_cpus[i] = 1;
      }
    } else {
      assert (params.num_cpus.length == params.num_local_schedulers);
    }

    if (params.num_gpus.length == 0) {
      params.num_gpus = new int[params.num_local_schedulers];
      for (int i = 0; i < params.num_local_schedulers; i++) {
        params.num_gpus[i] = 0;
      }
    } else {
      assert (params.num_gpus.length == params.num_local_schedulers);
    }

    int[] localNumWorkers = new int[params.num_local_schedulers];
    if (params.num_workers == 0) {
      System.arraycopy(params.num_cpus, 0, localNumWorkers, 0, params.num_local_schedulers);
    } else {
      for (int i = 0; i < params.num_local_schedulers; i++) {
        localNumWorkers[i] = params.num_workers;
      }
    }

    AddressInfo info = new AddressInfo();

    if (params.use_raylet) {
      // Start object store
      int rpcPort = params.object_store_rpc_port;
      String storeName = "/tmp/plasma_store" + rpcPort;

      startObjectStore(0, info, params.working_directory + "/store",
          params.redis_address, params.node_ip_address, params.redirect, params.cleanup);

      Map<String, Double> staticResources =
          ResourceUtil.getResourcesMapFromString(params.static_resources);

      //Start raylet
      startRaylet(storeName, info, params.num_workers,
          params.working_directory + "/raylet", params.redis_address,
          params.node_ip_address, params.redirect, staticResources, params.cleanup);

      runInfo.localStores.add(info);
    } else {
      for (int i = 0; i < params.num_local_schedulers; i++) {
        // Start object stores
        startObjectStore(i, info, params.working_directory + "/store",
            params.redis_address, params.node_ip_address, params.redirect, params.cleanup);

        startObjectManager(i, info,
            params.working_directory + "/storeManager", params.redis_address,
            params.node_ip_address, params.redirect, params.cleanup);

        // Start local scheduler
        int workerCount = 0;

        if (params.start_workers_from_local_scheduler) {
          workerCount = localNumWorkers[i];
          localNumWorkers[i] = 0;
        }

        startLocalScheduler(i, info,
            params.num_cpus[i], params.num_gpus[i], workerCount,
            params.working_directory + "/localsc", params.redis_address,
            params.node_ip_address, params.redirect, params.cleanup);

        runInfo.localStores.add(info);
      }
    }

    // start local workers
    if (!params.use_raylet) {
      for (int i = 0; i < params.num_local_schedulers; i++) {
        AddressInfo localStores = runInfo.localStores.get(i);
        localStores.workerCount = localNumWorkers[i];
        for (int j = 0; j < localNumWorkers[i]; j++) {
          startWorker(localStores.storeName, localStores.managerName, localStores.schedulerName,
                  params.working_directory + "/worker" + i + "." + j, params.redis_address,
                  params.node_ip_address, UniqueID.nil, "", params.redirect, params.cleanup);
        }
      }
    }

    HashSet<RunInfo.ProcessType> excludeTypes = new HashSet<>();
    if (!params.use_raylet) {
      excludeTypes.add(RunInfo.ProcessType.PT_RAYLET);
    } else {
      excludeTypes.add(RunInfo.ProcessType.PT_LOCAL_SCHEDULER);
      excludeTypes.add(RunInfo.ProcessType.PT_GLOBAL_SCHEDULER);
      excludeTypes.add(RunInfo.ProcessType.PT_PLASMA_MANAGER);
    }
    if (!checkAlive(excludeTypes)) {
      cleanup(true);
      throw new RuntimeException("Start Ray processes failed");
    }
  }

  public boolean checkAlive(HashSet<RunInfo.ProcessType> excludeTypes) {
    RunInfo.ProcessType[] types = RunInfo.ProcessType.values();
    for (int i = 0; i < types.length; i++) {
      if (excludeTypes.contains(types[i])) {
        continue;
      }

      ProcessInfo p;
      for (int j = 0; j < runInfo.allProcesses.get(i).size(); ) {
        p = runInfo.allProcesses.get(i).get(j);
        if (!p.process.isAlive()) {
          RayLog.core.error("Process " + p.process.hashCode() + " is not alive!" + " Process Type "
              + types[i].name());
          runInfo.deadProcess.add(p);
          runInfo.allProcesses.get(i).remove(j);
        } else {
          j++;
        }
      }
    }

    return runInfo.deadProcess.isEmpty();
  }

  public boolean tryRecoverDeadProcess() {

    if (runInfo.deadProcess.isEmpty()) {
      return true;
    }

    /* check the dead process */
    for (ProcessInfo info : runInfo.deadProcess) {
      if (info.type == RunInfo.ProcessType.PT_LOCAL_SCHEDULER
          || info.type == RunInfo.ProcessType.PT_PLASMA_STORE
          || info.type == RunInfo.ProcessType.PT_PLASMA_MANAGER) {
        /* When local scheduler or plasma store or plasma manager process dead, we can not
         * recover this node simply by restarting the dead process. Instead, We need to restart
         * all the node processes
         *  */
        RayLog.core
            .error(info.type.name() + "process dead, we can not simply restart this process");
        return false;
      }
    }

    /* try to recover */
    ProcessInfo info;
    for (int i = 0; i < runInfo.deadProcess.size(); i++) {
      info = runInfo.deadProcess.get(i);
      if (info.type == RunInfo.ProcessType.PT_GLOBAL_SCHEDULER) {
        RayLog.core.error(info.type.name() + "process dead, restart this process");
        startProcess(info.cmd, null, info.type, info.workDir, info.redisAddress, info.ip,
            info.redirect, info.cleanup);
      } else {
        RayLog.core.error(info.type.name() + "process dead, we don't deal with it");
      }
    }
    runInfo.deadProcess.clear();
    return true;
  }

  // kill all processes started by startRayHead
  public void cleanup(boolean killAll) {
    // clean up the process in reverse order
    for (int i = ProcessType.values().length - 1; i >= 0; i--) {
      if (killAll) {
        runInfo.allProcesses.get(i).forEach(p -> {
          if (killProcess(p.process)) {
            RayLog.core.info("Kill process " + p.process.hashCode() + " forcely");
          }
        });
      } else {
        runInfo.toBeCleanedProcesses.get(i).forEach(p -> {
          if (killProcess(p)) {
            RayLog.core.info("Kill process " + p.hashCode() + " forcely");
          }
        });
      }

      runInfo.toBeCleanedProcesses.get(i).clear();
      runInfo.allProcesses.get(i).clear();
      runInfo.deadProcess.clear();
    }
  }

  //
  // start a redis server
  //
  // @param ip the IP address of the local node
  // @param port port to be opended for redis traffic
  // @param numOfShards the number of redis shards to start
  // @param redirect whether to redirect the output/err to the log files
  // @param cleanup true if using ray in local mode. If cleanup is true, when
  // all Redis processes started by this method will be killed by @cleanup
  // when the worker exits
  // @return primary redis shard address
  //
  private List<String> startRedis(String workDir, String ip, int port, int numOfShards,
                                  boolean redirect, boolean cleanup) {
    ArrayList<String> shards = new ArrayList<>();
    String addr;
    for (int i = 0; i < numOfShards; i++) {
      addr = startRedisInstance(workDir, ip, port + i, redirect, cleanup);

      if (addr.length() == 0) {
        cleanup(cleanup);
        shards.clear();
        return shards;
      } else {
        shards.add(addr);
      }
    }

    for (String shard : shards) {
      // TODO: wait for redis server to start
    }

    return shards;
  }

  //
  // @param ip local node ip, only used for logging purpose
  // @param port given port for this redis instance, 0 for auto-selected port
  // @return redis server address
  //
  private String startRedisInstance(String workDir, String ip, int port,
                                    boolean redirect, boolean cleanup) {
    String redisFilePath = paths.redis_server;
    String redisModule = paths.redis_module;

    assert (new File(redisFilePath).exists()) : "file don't exsits : " + redisFilePath;
    assert (new File(redisModule).exists()) : "file don't exsits : " + redisModule;


    String cmd = redisFilePath + " --protected-mode no --port " + port + " --loglevel warning"
        + " --loadmodule " + redisModule;

    Map<String, String> env = null;
    Process p = startProcess(cmd.split(" "), env, RunInfo.ProcessType.PT_REDIS_SERVER,
        workDir + port, "", ip, redirect, cleanup);

    if (p == null || !p.isAlive()) {
      return "";
    }

    try {
      TimeUnit.MILLISECONDS.sleep(300);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    Jedis client = new Jedis(params.node_ip_address, port);

    // Configure Redis to only generate notifications for the export keys.
    client.configSet("notify-keyspace-events", "Kl");

    // Put a time stamp in Redis to indicate when it was started.
    client.set("redis_start_time", LocalDateTime.now().toString());

    client.close();
    return ip + ":" + port;
  }

  private void startGlobalScheduler(String workDir, String redisAddress, String ip,
                                    boolean redirect, boolean cleanup) {
    String filePath = paths.global_scheduler;
    String cmd = filePath + " -r " + redisAddress + " -h " + ip;

    Map<String, String> env = null;
    startProcess(cmd.split(" "), env, RunInfo.ProcessType.PT_GLOBAL_SCHEDULER, workDir,
        redisAddress,
        ip, redirect, cleanup);
  }

  private Map<String, String> retrieveEnv(String conf, Map<String, String> env) {
    String[] splits = conf.split(" ");
    for (String item : splits) {
      int idx = item.trim().indexOf('=');
      if (idx == -1) {
        continue;
      }
      String key = item.substring(0, idx);
      String val = item.substring(idx + 1);
      env.put(key, val);
    }
    return env;
  }

  /*
   * @param storeName The name of the plasma store socket to connect to
   *
   * @param storeManagerName The name of the plasma manager socket to connect
   * to
   *
   * @param storeManagerAddress the address of the plasma manager to connect
   * to
   *
   * @param workerPath The path of the script to use when the local scheduler
   * starts up new workers
   *
   * @param numCpus The number of CPUs the local scheduler should be
   * configured with
   *
   * @param numGpus The number of GPUs the local scheduler should be
   * configured with
   *
   * @param numWorkers The number of workers that the local scheduler should
   * start
   */
  private void startLocalScheduler(int index, AddressInfo info, int numCpus,
                                   int numGpus, int numWorkers, String workDir,
                                   String redisAddress, String ip, boolean redirect,
                                   boolean cleanup) {
    //if (numCpus <= 0)
    //    numCpus = Runtime.getRuntime().availableProcessors();
    if (numGpus <= 0) {
      numGpus = 0;
    }

    String filePath = paths.local_scheduler;
    int rpcPort = params.local_scheduler_rpc_port + index;
    String name = "/tmp/scheduler" + rpcPort;
    String rpcAddr = "";
    String cmd = filePath + " -s " + name + " -p " + info.storeName + " -h " + ip + " -n "
        + numWorkers + " -c " + "CPU," + INT16_MAX + ",GPU,0";

    assert (info.managerName.length() > 0);
    assert (info.storeName.length() > 0);
    assert (redisAddress.length() > 0);

    cmd += " -m " + info.managerName;

    String workerCmd = null;
    workerCmd = buildWorkerCommand(true, info.storeName, info.managerName, name,
        UniqueID.nil, "", workDir + rpcPort, ip, redisAddress);
    cmd += " -w \"" + workerCmd + "\"";

    if (redisAddress.length() > 0) {
      cmd += " -r " + redisAddress;
    }
    if (info.managerPort > 0) {
      cmd += " -a " + params.node_ip_address + ":" + info.managerPort;
    }

    Map<String, String> env = null;
    String[] cmds = StringUtil.split(cmd, " ", "\"", "\"").toArray(new String[0]);
    Process p = startProcess(cmds, env, RunInfo.ProcessType.PT_LOCAL_SCHEDULER,
        workDir + rpcPort, redisAddress, ip, redirect, cleanup);

    if (p != null && p.isAlive()) {
      try {
        TimeUnit.MILLISECONDS.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    if (p == null || !p.isAlive()) {
      info.schedulerName = "";
      info.schedulerRpcAddr = "";
      throw new RuntimeException("Start local scheduler failed ...");
    } else {
      info.schedulerName = name;
      info.schedulerRpcAddr = rpcAddr;
    }
  }

  private void startRaylet(String storeName, AddressInfo info, int numWorkers,
                           String workDir, String redisAddress, String ip, boolean redirect,
                           Map<String, Double> staticResources, boolean cleanup) {

    int rpcPort = params.raylet_port;
    String rayletSocketName = "/tmp/raylet" + rpcPort;

    String filePath = paths.raylet;
    
    String workerCmd = null;
    workerCmd = buildWorkerCommandRaylet(info.storeName, rayletSocketName, UniqueID.nil,
            "", workDir + rpcPort, ip, redisAddress);

    int sep = redisAddress.indexOf(':');
    assert (sep != -1);
    String gcsIp = redisAddress.substring(0, sep);
    String gcsPort = redisAddress.substring(sep + 1);

    String resourceArgument = ResourceUtil.getResourcesStringFromMap(staticResources);

    String[] cmds = new String[]{filePath, rayletSocketName, storeName, ip, gcsIp,
                                 gcsPort, "" + numWorkers, workerCmd, resourceArgument};

    Process p = startProcess(cmds, null, RunInfo.ProcessType.PT_RAYLET,
        workDir + rpcPort, redisAddress, ip, redirect, cleanup);

    if (p != null && p.isAlive()) {
      try {
        TimeUnit.MILLISECONDS.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    if (p == null || !p.isAlive()) {
      info.rayletSocketName = "";
      info.rayletRpcAddr = "";
      throw new RuntimeException("Failed to start raylet process.");
    } else {
      info.rayletSocketName = rayletSocketName;
      info.rayletRpcAddr = ip + ":" + rpcPort;
    }
  }

  private String buildWorkerCommandRaylet(String storeName, String rayletSocketName,
                                          UniqueID actorId, String actorClass, String workDir,
                                          String ip, String redisAddress) {
    String workerConfigs = "ray.java.start.object_store_name=" + storeName
        + ";ray.java.start.raylet_socket_name=" + rayletSocketName
        + ";ray.java.start.worker_mode=WORKER;ray.java.start.use_raylet=true";
    workerConfigs += ";ray.java.start.deploy=" + params.deploy;
    if (!actorId.equals(UniqueID.nil)) {
      workerConfigs += ";ray.java.start.actor_id=" + actorId;
    }
    if (!actorClass.equals("")) {
      workerConfigs += ";ray.java.start.driver_class=" + actorClass;
    }

    String jvmArgs = "";
    jvmArgs += " -Dlogging.path=" + params.working_directory + "/logs/workers";
    jvmArgs += " -Dlogging.file.name=core-*pid_suffix*";

    return buildJavaProcessCommand(
        RunInfo.ProcessType.PT_WORKER,
        "org.ray.runner.worker.DefaultWorker",
        "",
        workerConfigs,
        jvmArgs,
        workDir,
        ip,
        redisAddress,
        null
    );
  }

  private String buildWorkerCommand(boolean isFromLocalScheduler, String storeName,
                                    String storeManagerName, String localSchedulerName,
                                    UniqueID actorId, String actorClass, String workDir, String
                                        ip, String redisAddress) {
    String workerConfigs = "ray.java.start.object_store_name=" + storeName
        + ";ray.java.start.object_store_manager_name=" + storeManagerName
        + ";ray.java.start.worker_mode=WORKER"
        + ";ray.java.start.local_scheduler_name=" + localSchedulerName;
    workerConfigs += ";ray.java.start.deploy=" + params.deploy;
    if (!actorId.equals(UniqueID.nil)) {
      workerConfigs += ";ray.java.start.actor_id=" + actorId;
    }
    if (!actorClass.equals("")) {
      workerConfigs += ";ray.java.start.driver_class=" + actorClass;
    }

    String jvmArgs = "";
    jvmArgs += " -Dlogging.path=" + params.working_directory + "/logs/workers";
    jvmArgs += " -Dlogging.file.name=core-*pid_suffix*";

    return buildJavaProcessCommand(
        RunInfo.ProcessType.PT_WORKER,
        "org.ray.runner.worker.DefaultWorker",
        "",
        workerConfigs,
        jvmArgs,
        workDir,
        ip,
        redisAddress,
        null
    );
  }

  private void startObjectStore(int index, AddressInfo info, String workDir, String redisAddress,
                                String ip, boolean redirect, boolean cleanup) {
    int occupiedMemoryMb = params.object_store_occupied_memory_MB;
    long memoryBytes = occupiedMemoryMb * 1000000;
    String filePath = paths.store;
    int rpcPort = params.object_store_rpc_port + index;
    String name = "/tmp/plasma_store" + rpcPort;
    String rpcAddr = "";
    String cmd = filePath + " -s " + name + " -m " + memoryBytes;

    Map<String, String> env = null;
    Process p = startProcess(cmd.split(" "), env, RunInfo.ProcessType.PT_PLASMA_STORE,
        workDir + rpcPort, redisAddress, ip, redirect, cleanup);

    if (p != null && p.isAlive()) {
      try {
        TimeUnit.MILLISECONDS.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    if (p == null || !p.isAlive()) {
      info.storeName = "";
      info.storeRpcAddr = "";
      throw new RuntimeException("Start object store failed ...");
    } else {
      info.storeName = name;
      info.storeRpcAddr = rpcAddr;
    }
  }

  private AddressInfo startObjectManager(int index, AddressInfo info, String workDir,
                                         String redisAddress, String ip, boolean redirect,
                                         boolean cleanup) {
    String filePath = paths.store_manager;
    int rpcPort = params.object_store_manager_rpc_port + index;
    String name = "/tmp/plasma_manager" + rpcPort;
    String rpcAddr = "";

    String cmd = filePath + " -s " + info.storeName + " -m " + name + " -h " + ip + " -p "
        + (params.object_store_manager_ray_listen_port + index)
        + " -r " + redisAddress;

    Map<String, String> env = null;
    Process p = startProcess(cmd.split(" "), env, RunInfo.ProcessType.PT_PLASMA_MANAGER,
        workDir + rpcPort, redisAddress, ip, redirect, cleanup);

    if (p != null && p.isAlive()) {
      try {
        TimeUnit.MILLISECONDS.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    if (p == null || !p.isAlive()) {
      throw new RuntimeException("Start object manager failed ...");
    } else {
      info.managerName = name;
      info.managerPort = params.object_store_manager_ray_listen_port + index;
      info.managerRpcAddr = rpcAddr;
      return info;
    }
  }

  public void startWorker(String storeName, String storeManagerName,
                          String localSchedulerName, String workDir, String redisAddress,
                          String ip, UniqueID actorId, String actorClass,
                          boolean redirect, boolean cleanup) {
    String cmd = buildWorkerCommand(false, storeName, storeManagerName, localSchedulerName, actorId,
        actorClass, workDir, ip, redisAddress);
    startProcess(cmd.split(" "), null, RunInfo.ProcessType.PT_WORKER, workDir, redisAddress, ip,
        redirect, cleanup);
  }
}
