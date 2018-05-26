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
import org.ray.util.StringUtil;
import org.ray.util.config.ConfigReader;
import org.ray.util.logger.RayLog;
import redis.clients.jedis.Jedis;

/**
 * Ray service management on one box
 */
public class RunManager {

  public static final int INT16_MAX = 32767;

  private RayParameters params;

  private PathConfig paths;

  private ConfigReader configReader;

  private String procStdoutFileName = "";

  private String procStderrFileName = "";

  private RunInfo runInfo = new RunInfo();

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

  public RunManager(RayParameters params, PathConfig paths, ConfigReader configReader) {
    this.params = params;
    this.paths = paths;
    this.configReader = configReader;
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
        + section + "logging_directory=" + params.logging_directory + ";"
        + section + "working_directory=" + workDir;

    if (additionalConfigs.length() > 0) {
      cmd += ";" + additionalConfigs;
    }

    return cmd;
  }

  private Process startJavaProcess(RunInfo.ProcessType pt, String mainClass,
      String additonalClassPaths, String additionalConfigs,
      String additionalJvmArgs, String workDir, String ip, String redisAddr, boolean redirect,
      boolean cleanup, String agentlibAddr) {

    String cmd = buildJavaProcessCommand(pt, mainClass, additonalClassPaths, additionalConfigs,
        additionalJvmArgs, workDir, ip, redisAddr, agentlibAddr);
    return startProcess(cmd.split(" "), null, pt, workDir, redisAddr, ip, redirect, cleanup);
  }

  public Process startDriver(String mainClass, String redisAddress, UniqueID driverId,
      String workDir, String ip,
      String driverClass, String additonalClassPaths, String additionalConfigs) {
    String driverConfigs =
        "ray.java.start.driver_id=" + driverId + ";ray.java.start.driver_class=" + driverClass;
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

  public void startRayProcesses() {
    Jedis _redis_client = null;

    RayLog.core.info("start ray processes @ " + params.node_ip_address + " ...");

    // start primary redis
    if (params.redis_address.length() == 0) {
      List<String> primaryShards = startRedis(params.working_directory + "/redis",
          params.node_ip_address, params.redis_port, 1, params.redirect, params.cleanup);
      params.redis_address = primaryShards.get(0);

      String[] args = params.redis_address.split(":");
      _redis_client = new Jedis(args[0], Integer.parseInt(args[1]));

      // Register the number of Redis shards in the primary shard, so that clients
      // know how many redis shards to expect under RedisShards.
      _redis_client.set("NumRedisShards", Integer.toString(params.num_redis_shards));
    } else {
      String[] args = params.redis_address.split(":");
      _redis_client = new Jedis(args[0], Integer.parseInt(args[1]));
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
        _redis_client.rpush("RedisShards", addr);
      }
    }
    _redis_client.close();

    // start global scheduler
    if (params.include_global_scheduler) {
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

    int[] local_num_workers = new int[params.num_local_schedulers];
    if (params.num_workers == 0) {
      System.arraycopy(params.num_cpus, 0, local_num_workers, 0, params.num_local_schedulers);
    } else {
      for (int i = 0; i < params.num_local_schedulers; i++) {
        local_num_workers[i] = params.num_workers;
      }
    }

    // start object stores
    for (int i = 0; i < params.num_local_schedulers; i++) {
      AddressInfo info = new AddressInfo();
      // store
      startObjectStore(i, info, params.working_directory + "/store",
          params.redis_address, params.node_ip_address, params.redirect, params.cleanup);

      // store manager
      startObjectManager(i, info,
          params.working_directory + "/storeManager", params.redis_address,
          params.node_ip_address, params.redirect, params.cleanup);

      runInfo.local_stores.add(info);
    }

    // start local scheduler
    for (int i = 0; i < params.num_local_schedulers; i++) {
      int workerCount = 0;

      if (params.start_workers_from_local_scheduler) {
        workerCount = local_num_workers[i];
        local_num_workers[i] = 0;
      }

      startLocalScheduler(i, runInfo.local_stores.get(i),
          params.num_cpus[i], params.num_gpus[i], workerCount,
          params.working_directory + "/localScheduler", params.redis_address,
          params.node_ip_address, params.redirect, params.cleanup);
    }

    // start local workers
    for (int i = 0; i < params.num_local_schedulers; i++) {
      runInfo.local_stores.get(i).workerCount = local_num_workers[i];
      for (int j = 0; j < local_num_workers[i]; j++) {
        startWorker(runInfo.local_stores.get(i).storeName,
                runInfo.local_stores.get(i).managerName, runInfo.local_stores.get(i).schedulerName,
                params.working_directory + "/worker" + i + "." + j, params.redis_address,
                params.node_ip_address, UniqueID.nil, "",
                params.redirect, params.cleanup);
      }
    }

    HashSet<RunInfo.ProcessType> excludeTypes = new HashSet<>();
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
      for (int j = 0; j < runInfo.allProcesses.get(i).size();) {
        p = runInfo.allProcesses.get(i).get(j);
        if (!p.process.isAlive()) {
          RayLog.core.error("Process " + p.hashCode() + " is not alive!" + " Process Type " + types[i].name());
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
            RayLog.core.info("Kill process " + p.hashCode() + " forcely");
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

    if (killAll) {
      // kill all workers that are forked by local scheduler
      // ps aux | grep DefaultWorker | awk '{system("kill "$2);}'
      String[] cmd = {"/bin/sh", "-c", ""};
      cmd[2] = "ps aux | grep DefaultWorker | grep -v grep | awk \"{print \\$2}\" | xargs kill";
      try {
        Runtime.getRuntime().exec(cmd);
      } catch (IOException e) {
      }
    }
  }

  private void record_log_files_in_redis(String redis_address, String node_ip_address,
      List<String> logfiles) {
    if (redis_address != null && !redis_address.isEmpty() && node_ip_address != null
        && !node_ip_address.isEmpty() && logfiles.size() > 0) {
      String[] ip_port = redis_address.split(":");
      Jedis jedis_client = new Jedis(ip_port[0], Integer.parseInt(ip_port[1]));
      String log_file_list_key = String.format("LOG_FILENAMES:{%s}", node_ip_address);
      for (String logfile : logfiles) {
        jedis_client.rpush(log_file_list_key, logfile);
      }
      jedis_client.close();
    }
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
      String stdout_file;
      String stderr_file;
      stdout_file = workDir + "/" + processIndex + ".out.txt";
      stderr_file = workDir + "/" + processIndex + ".err.txt";
      builder.redirectOutput(new File(stdout_file));
      builder.redirectError(new File(stderr_file));
      List<String> std_file_list = new ArrayList<>();
      std_file_list.add(stdout_file);
      std_file_list.add(stderr_file);
      record_log_files_in_redis(redisAddress, ip, std_file_list);
      procStdoutFileName = stdout_file;
      procStderrFileName = stderr_file;
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

  private static boolean killProcess(Process p) {
    if (p.isAlive()) {
      p.destroyForcibly();
      return true;
    } else {
      return false;
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
        + numWorkers + " -c " + "CPU," + INT16_MAX +",GPU,0";

    assert (info.managerName.length() > 0);
    assert (info.storeName.length() > 0);
    assert (redisAddress.length() > 0);

    cmd += " -m " + info.managerName;

    String workerCmd = null;
    workerCmd = buildWorkerCommand(true, info.storeName, info.managerName, name, UniqueID.nil,
            "", workDir + rpcPort, ip, redisAddress);
    cmd += " -w \"" + workerCmd + "\"";

    if (redisAddress.length() > 0) {
      cmd += " -r " + redisAddress;
    }
    if (info.managerPort > 0) {
      cmd += " -a " + params.node_ip_address + ":" + info.managerPort;
    }

    Map<String, String> env = null;
    String[] cmds = StringUtil.Split(cmd, " ", "\"", "\"").toArray(new String[0]);
    Process p = startProcess(cmds, env, RunInfo.ProcessType.PT_LOCAL_SCHEDULER,
        workDir + rpcPort, redisAddress, ip, redirect, cleanup);

    if (p != null && p.isAlive()) {
      try {
        TimeUnit.MILLISECONDS.sleep(100);
      } catch (InterruptedException e) {
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

  private String buildWorkerCommand(boolean isFromLocalScheduler, String storeName,
      String storeManagerName, String localSchedulerName,
      UniqueID actorId, String actorClass, String workDir, String ip, String redisAddress) {
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
    jvmArgs += " -DlogOutput=" + params.logging_directory + "/workers/*pid_suffix*";

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
    int occupiedMemoryMB = params.object_store_occupied_memory_MB;
    long memoryBytes = occupiedMemoryMB * 1000000;
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
