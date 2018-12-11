package org.ray.runtime.runner;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.util.FileUtil;
import org.ray.runtime.util.ResourceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

/**
 * Ray service management on one box.
 */
public class RunManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(RunManager.class);

  private static final DateTimeFormatter DATE_TIME_FORMATTER =
      DateTimeFormatter.ofPattern("Y-M-d_H-m-s");

  private static final String WORKER_CLASS = "org.ray.runtime.runner.worker.DefaultWorker";

  private RayConfig rayConfig;

  private Random random;

  private List<Process> processes;

  public RunManager(RayConfig rayConfig) {
    this.rayConfig = rayConfig;
    processes = new ArrayList<>();
    random = new Random();
  }

  public void cleanup() {
    for (Process p : processes) {
      p.destroy();
    }
  }

  private void createTempDirs() {
    FileUtil.mkDir(new File(rayConfig.logDir));
    FileUtil.mkDir(new File(rayConfig.rayletSocketName).getParentFile());
    FileUtil.mkDir(new File(rayConfig.objectStoreSocketName).getParentFile());
  }

  /**
   * Start a process.
   * @param command The command to start the process with.
   * @param env Environment variables.
   * @param name Process name.
   */
  private void startProcess(List<String> command, Map<String, String> env, String name) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Starting process {} with command: {}", name, command,
          Joiner.on(" ").join(command));
    }

    ProcessBuilder builder = new ProcessBuilder(command);

    if (rayConfig.redirectOutput) {
      // Set stdout and stderr paths.
      int logId = random.nextInt(10000);
      String date = DATE_TIME_FORMATTER.format(LocalDateTime.now());
      String stdout = String.format("%s/%s-%s-%05d.out", rayConfig.logDir, name, date, logId);
      String stderr = String.format("%s/%s-%s-%05d.err", rayConfig.logDir, name, date, logId);
      builder.redirectOutput(new File(stdout));
      builder.redirectError(new File(stderr));
    }
    // Set environment variables.
    if (env != null && !env.isEmpty()) {
      builder.environment().putAll(env);
    }

    Process p;
    try {
      p = builder.start();
    } catch (IOException e) {
      LOGGER.error("Failed to start process " + name, e);
      throw new RuntimeException("Failed to start process " + name, e);
    }
    // Wait 200ms and check whether the process is alive.
    try {
      TimeUnit.MILLISECONDS.sleep(200);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    if (!p.isAlive()) {
      throw new RuntimeException("Failed to start " + name);
    }
    processes.add(p);
    LOGGER.info("{} process started", name);
  }

  /**
   * Start all Ray processes on this node.
   * @param isHead Whether this node is the head node. If true, redis server will be started.
   */
  public void startRayProcesses(boolean isHead) {
    LOGGER.info("Starting ray processes @ {}.", rayConfig.nodeIp);
    try {
      createTempDirs();
      if (isHead) {
        startRedisServer();
      }
      startObjectStore();
      startRaylet();
      LOGGER.info("All processes started @ {}.", rayConfig.nodeIp);
    } catch (Exception e) {
      // Clean up started processes.
      cleanup();
      LOGGER.error("Failed to start ray processes.", e);
      throw new RuntimeException("Failed to start ray processes.", e);
    }
  }

  private void startRedisServer() {
    // start primary redis
    String primary = startRedisInstance(rayConfig.nodeIp, rayConfig.headRedisPort, null);
    rayConfig.setRedisAddress(primary);
    try (Jedis client = new Jedis("127.0.0.1", rayConfig.headRedisPort)) {
      client.set("UseRaylet", "1");
      // Register the number of Redis shards in the primary shard, so that clients
      // know how many redis shards to expect under RedisShards.
      client.set("NumRedisShards", Integer.toString(rayConfig.numberRedisShards));

      // start redis shards
      for (int i = 0; i < rayConfig.numberRedisShards; i++) {
        String shard = startRedisInstance(rayConfig.nodeIp, rayConfig.headRedisPort + i + 1, i);
        client.rpush("RedisShards", shard);
      }
    }
  }

  private String startRedisInstance(String ip, int port, Integer shard) {
    List<String> command = ImmutableList.of(
        rayConfig.redisServerExecutablePath,
        "--protected-mode",
        "no",
        "--port",
        String.valueOf(port),
        "--loglevel",
        "warning",
        "--loadmodule",
        rayConfig.redisModulePath
    );
    String name = shard == null ? "redis" : "redis-" + shard;
    startProcess(command, null, name);

    try (Jedis client = new Jedis("127.0.0.1", port)) {
      // Configure Redis to only generate notifications for the export keys.
      client.configSet("notify-keyspace-events", "Kl");
      // Put a time stamp in Redis to indicate when it was started.
      client.set("redis_start_time", LocalDateTime.now().toString());
    }

    return ip + ":" + port;
  }

  private void startRaylet() {
    int hardwareConcurrency = Runtime.getRuntime().availableProcessors();
    int maximumStartupConcurrency = Math.max(1,
        Math.min(rayConfig.resources.getOrDefault("CPU", 0.0).intValue(), hardwareConcurrency));

    // See `src/ray/raylet/main.cc` for the meaning of each parameter.
    List<String> command = ImmutableList.of(
        rayConfig.rayletExecutablePath,
        rayConfig.rayletSocketName,
        rayConfig.objectStoreSocketName,
        "0",  // The object manager port.
        "0",  // The node manager port.
        rayConfig.nodeIp,
        rayConfig.getRedisIp(),
        rayConfig.getRedisPort().toString(),
        "0", // number of initial workers
        String.valueOf(maximumStartupConcurrency),
        ResourceUtil.getResourcesStringFromMap(rayConfig.resources),
        "",  // The internal config list.
        buildPythonWorkerCommand(), // python worker command
        buildWorkerCommandRaylet() // java worker command
    );

    startProcess(command, null, "raylet");
  }

  private String concatPath(Stream<String> stream) {
    // TODO (hchen): Right now, raylet backend doesn't support worker command with spaces.
    // Thus, we have to drop some some paths until that is fixed.
    return stream.filter(s -> !s.contains(" ")).collect(Collectors.joining(":"));
  }

  private String buildWorkerCommandRaylet() {
    List<String> cmd = new ArrayList<>();
    cmd.add("java");
    cmd.add("-classpath");

    // Generate classpath based on current classpath + user-defined classpath.
    String classpath = concatPath(Stream.concat(
        rayConfig.classpath.stream(),
        Stream.of(System.getProperty("java.class.path").split(":"))
    ));
    cmd.add(classpath);

    // library path
    String libraryPath = concatPath(rayConfig.libraryPath.stream());
    cmd.add("-Djava.library.path=" + libraryPath);

    // logging path
    if (rayConfig.redirectOutput) {
      cmd.add("-Dray.logging.stdout=org.apache.log4j.varia.NullAppender");
      cmd.add("-Dray.logging.file=org.apache.log4j.FileAppender");
      int logId = random.nextInt(10000);
      String date = DATE_TIME_FORMATTER.format(LocalDateTime.now());
      String logFile = String.format("%s/worker-%s-%05d.out", rayConfig.logDir, date, logId);
      cmd.add("-Dray.logging.file.path=" + logFile);
    }

    // Config overwrite
    cmd.add("-Dray.redis.address=" + rayConfig.getRedisAddress());

    cmd.addAll(rayConfig.jvmParameters);

    // Main class
    cmd.add(WORKER_CLASS);
    String command = Joiner.on(" ").join(cmd);
    LOGGER.debug("Worker command is: {}", command);
    return command;
  }

  private void startObjectStore() {
    List<String> command = ImmutableList.of(
        rayConfig.plasmaStoreExecutablePath,
        "-s",
        rayConfig.objectStoreSocketName,
        "-m",
        rayConfig.objectStoreSize.toString()
    );
    startProcess(command, null, "plasma_store");
  }

  private String buildPythonWorkerCommand() {
    // disable python worker start from raylet, which starts from java
    if (rayConfig.pythonWorkerCommand == null) {
      return "";
    }

    List<String> cmd = new ArrayList<>();
    cmd.add(rayConfig.pythonWorkerCommand);
    cmd.add("--node-ip-address=" + rayConfig.nodeIp);
    cmd.add("--object-store-name=" + rayConfig.objectStoreSocketName);
    cmd.add("--raylet-name=" + rayConfig.rayletSocketName);
    cmd.add("--redis-address=" + rayConfig.getRedisAddress());

    String command = cmd.stream().collect(Collectors.joining(" "));
    LOGGER.debug("python worker command: {}", command);
    return command;
  }

}
