package io.ray.runtime.runner;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.ray.runtime.config.RayConfig;
import io.ray.runtime.util.BinaryFileUtil;
import io.ray.runtime.util.ResourceUtil;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

/**
 * Ray service management on one box.
 */
public class RunManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(RunManager.class);

  private static final DateTimeFormatter DATE_TIME_FORMATTER =
      DateTimeFormatter.ofPattern("YYYY-MM-dd_HH-mm-ss");

  private static final String WORKER_CLASS = "io.ray.runtime.runner.worker.DefaultWorker";

  private static final String SESSION_LATEST = "session_latest";

  private RayConfig rayConfig;

  private Random random = new Random();

  private List<Pair<String, Process>> processes;

  private static final int KILL_PROCESS_WAIT_TIMEOUT_SECONDS = 1;

  public RunManager(RayConfig rayConfig) {
    this.rayConfig = rayConfig;
    processes = new ArrayList<>();
    createTempDirs();
  }

  public void cleanup() {
    // Terminate the processes in the reversed order of creating them.
    // Because raylet needs to exit before object store, otherwise it
    // cannot exit gracefully.

    for (int i = processes.size() - 1; i >= 0; --i) {
      Pair<String, Process> pair = processes.get(i);
      terminateProcess(pair.getLeft(), pair.getRight());
    }
  }

  public void terminateProcess(String name, Process p) {
    int numAttempts = 0;
    while (p.isAlive()) {
      if (numAttempts == 0) {
        LOGGER.debug("Terminating process {}.", name);
        p.destroy();
      } else {
        LOGGER.debug("Terminating process {} forcibly.", name);
        p.destroyForcibly();
      }
      try {
        p.waitFor(KILL_PROCESS_WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        LOGGER.warn("Got InterruptedException while waiting for process {}" +
            " to be terminated.", name);
      }
      numAttempts++;
    }
    LOGGER.info("Process {} is now terminated.", name);
  }

  /**
   * Get processes by name. For test purposes only.
   */
  public List<Process> getProcesses(String name) {
    return processes.stream().filter(pair -> pair.getLeft().equals(name)).map(Pair::getRight)
        .collect(Collectors.toList());
  }

  private void createTempDirs() {
    try {
      FileUtils.forceMkdir(new File(rayConfig.logDir));
      FileUtils.forceMkdir(new File(rayConfig.rayletSocketName).getParentFile());
      FileUtils.forceMkdir(new File(rayConfig.objectStoreSocketName).getParentFile());

      // Remove session_latest first, and then create a new symbolic link for session_latest.
      final String parentOfSessionDir = new File(rayConfig.sessionDir).getParent();
      final File sessionLatest = new File(
          String.format("%s/%s", parentOfSessionDir, SESSION_LATEST));
      if (sessionLatest.exists()) {
        sessionLatest.delete();
      }
      Files.createSymbolicLink(
          Paths.get(sessionLatest.getAbsolutePath()),
          Paths.get(rayConfig.sessionDir));
    } catch (IOException e) {
      LOGGER.error("Couldn't create temp directories.", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Start a process.
   *
   * @param command The command to start the process with.
   * @param env Environment variables.
   * @param name Process name.
   */
  private void startProcess(List<String> command, Map<String, String> env, String name) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Starting process {} with command: {}", name,
          Joiner.on(" ").join(command));
    }

    ProcessBuilder builder = new ProcessBuilder(command);

    String stdout = "";
    String stderr = "";
    if (rayConfig.redirectOutput) {
      // Set stdout and stderr paths.
      int logId = random.nextInt(10000);
      String date = DATE_TIME_FORMATTER.format(LocalDateTime.now());
      stdout = String.format("%s/%s-%s-%05d.out", rayConfig.logDir, name, date, logId);
      stderr = String.format("%s/%s-%s-%05d.err", rayConfig.logDir, name, date, logId);
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
    // Wait 1000 ms and check whether the process is alive.
    try {
      TimeUnit.MILLISECONDS.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    if (!p.isAlive()) {
      String message = String.format("Failed to start %s. Exit code: %d.",
          name, p.exitValue());
      if (rayConfig.redirectOutput) {
        message += String.format(" Logs are redirected to %s and %s.", stdout, stderr);
      }
      throw new RuntimeException(message);
    }
    processes.add(Pair.of(name, p));
    if (LOGGER.isInfoEnabled()) {
      String message = String.format("%s process started.", name);
      if (rayConfig.redirectOutput) {
        message += String.format(" Logs are redirected to %s and %s.", stdout, stderr);
      }
      LOGGER.info(message);
    }
  }

  /**
   * Start all Ray processes on this node.
   *
   * @param isHead Whether this node is the head node. If true, redis server will be started.
   */
  public void startRayProcesses(boolean isHead) {
    LOGGER.info("Starting ray processes @ {}.", rayConfig.nodeIp);
    try {
      if (isHead) {
        startGcs();
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

  private void startGcs() {
    // start primary redis
    String primary = startRedisInstance(rayConfig.nodeIp,
        rayConfig.headRedisPort, rayConfig.headRedisPassword, null);
    rayConfig.setRedisAddress(primary);
    try (Jedis client = new Jedis("127.0.0.1", rayConfig.headRedisPort)) {
      if (!Strings.isNullOrEmpty(rayConfig.headRedisPassword)) {
        client.auth(rayConfig.headRedisPassword);
      }
      client.set("UseRaylet", "1");
      // Set job counter to compute job id.
      client.set("JobCounter", "0");
      // Register the number of Redis shards in the primary shard, so that clients
      // know how many redis shards to expect under RedisShards.
      client.set("NumRedisShards", Integer.toString(rayConfig.numberRedisShards));

      // start redis shards
      for (int i = 0; i < rayConfig.numberRedisShards; i++) {
        String shard = startRedisInstance(rayConfig.nodeIp,
            rayConfig.redisShardPorts[i], rayConfig.headRedisPassword, i);
        client.rpush("RedisShards", shard);
      }
    }

    // start gcs server
    if (rayConfig.gcsServiceEnabled) {
      String redisPasswordOption = "";
      if (!Strings.isNullOrEmpty(rayConfig.headRedisPassword)) {
        redisPasswordOption = rayConfig.headRedisPassword;
      }

      // See `src/ray/gcs/gcs_server/gcs_server_main.cc` for the meaning of each parameter.
      final File gcsServerFile = BinaryFileUtil.getFile(
          rayConfig.sessionDir, BinaryFileUtil.GCS_SERVER_BINARY_NAME);
      Preconditions.checkState(gcsServerFile.setExecutable(true));
      List<String> command = ImmutableList.of(
          gcsServerFile.getAbsolutePath(),
          String.format("--redis_address=%s", rayConfig.getRedisIp()),
          String.format("--redis_port=%d", rayConfig.getRedisPort()),
          String.format("--config_list=%s",
              rayConfig.rayletConfigParameters.entrySet().stream()
                  .map(entry -> entry.getKey() + "," + entry.getValue()).collect(Collectors
                      .joining(","))),
          String.format("--redis_password=%s", redisPasswordOption)
      );
      startProcess(command, null, "gcs_server");
    }
  }

  private String startRedisInstance(String ip, int port, String password, Integer shard) {
    final File redisServerFile = BinaryFileUtil.getFile(
        rayConfig.sessionDir, BinaryFileUtil.REDIS_SERVER_BINARY_NAME);
    Preconditions.checkState(redisServerFile.setExecutable(true));
    List<String> command = Lists.newArrayList(
        // The redis-server executable file.
        redisServerFile.getAbsolutePath(),
        "--protected-mode",
        "no",
        "--port",
        String.valueOf(port),
        "--loglevel",
        "warning",
        "--loadmodule",
        // The redis module file.
        BinaryFileUtil.getFile(
            rayConfig.sessionDir, BinaryFileUtil.REDIS_MODULE_LIBRARY_NAME).getAbsolutePath()
    );

    if (!Strings.isNullOrEmpty(password)) {
      command.add("--requirepass ");
      command.add(password);
    }

    String name = shard == null ? "redis" : "redis-" + shard;
    startProcess(command, null, name);

    try (Jedis client = new Jedis("127.0.0.1", port)) {
      if (!Strings.isNullOrEmpty(password)) {
        client.auth(password);
      }

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

    String redisPasswordOption = "";
    if (!Strings.isNullOrEmpty(rayConfig.headRedisPassword)) {
      redisPasswordOption = rayConfig.headRedisPassword;
    }

    // See `src/ray/raylet/main.cc` for the meaning of each parameter.
    final File rayletFile = BinaryFileUtil.getFile(
        rayConfig.sessionDir, BinaryFileUtil.RAYLET_BINARY_NAME);
    Preconditions.checkState(rayletFile.setExecutable(true));
    List<String> command = ImmutableList.of(
        rayletFile.getAbsolutePath(),
        String.format("--raylet_socket_name=%s", rayConfig.rayletSocketName),
        String.format("--store_socket_name=%s", rayConfig.objectStoreSocketName),
        String.format("--object_manager_port=%d", 0), // The object manager port.
        // The node manager port.
        String.format("--node_manager_port=%d", rayConfig.getNodeManagerPort()),
        String.format("--node_ip_address=%s", rayConfig.nodeIp),
        String.format("--redis_address=%s", rayConfig.getRedisIp()),
        String.format("--redis_port=%d", rayConfig.getRedisPort()),
        String.format("--num_initial_workers=%d", 0),  // number of initial workers
        String.format("--maximum_startup_concurrency=%d", maximumStartupConcurrency),
        String.format("--static_resource_list=%s",
            ResourceUtil.getResourcesStringFromMap(rayConfig.resources)),
        String.format("--config_list=%s", rayConfig.rayletConfigParameters.entrySet().stream()
            .map(entry -> entry.getKey() + "," + entry.getValue())
            .collect(Collectors.joining(","))),
        String.format("--python_worker_command=%s", buildPythonWorkerCommand()),
        String.format("--java_worker_command=%s", buildWorkerCommand()),
        String.format("--redis_password=%s", redisPasswordOption)
    );

    startProcess(command, null, "raylet");
  }

  private String concatPath(Stream<String> stream) {
    // TODO (hchen): Right now, raylet backend doesn't support worker command with spaces.
    // Thus, we have to drop some some paths until that is fixed.
    return stream.filter(s -> !s.contains(" ")).collect(Collectors.joining(":"));
  }

  private String buildWorkerCommand() {
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

    // session path
    cmd.add("-Dray.session-dir=" + rayConfig.sessionDir);

    // logging path
    if (rayConfig.redirectOutput) {
      cmd.add("-Dray.logging.stdout=org.apache.log4j.varia.NullAppender");
      cmd.add("-Dray.logging.file=org.apache.log4j.FileAppender");
      int logId = random.nextInt(10000);
      String date = DATE_TIME_FORMATTER.format(LocalDateTime.now());
      String logFile = String.format("%s/worker-%s-%05d.out", rayConfig.logDir, date, logId);
      cmd.add("-Dray.logging.file.path=" + logFile);
    }

    if (!Strings.isNullOrEmpty(rayConfig.jobResourcePath)) {
      cmd.add("-Dray.job.resource-path=" + rayConfig.jobResourcePath);
    }

    // socket names
    cmd.add("-Dray.raylet.socket-name=" + rayConfig.rayletSocketName);
    cmd.add("-Dray.object-store.socket-name=" + rayConfig.objectStoreSocketName);

    cmd.add("-Dray.raylet.node-manager-port=" + rayConfig.getNodeManagerPort());

    // Config overwrite
    cmd.add("-Dray.redis.address=" + rayConfig.getRedisAddress());

    // redis password
    if (!Strings.isNullOrEmpty(rayConfig.headRedisPassword)) {
      cmd.add("-Dray.redis.password=" + rayConfig.headRedisPassword);
    }


    cmd.add("RAY_WORKER_RAYLET_CONFIG_PLACEHOLDER");

    cmd.addAll(rayConfig.jvmParameters);

    // jvm options
    cmd.add("RAY_WORKER_DYNAMIC_OPTION_PLACEHOLDER_0");

    // Main class
    cmd.add(WORKER_CLASS);
    String command = Joiner.on(" ").join(cmd);
    LOGGER.debug("Worker command is: {}", command);
    return command;
  }

  private void startObjectStore() {
    final File objectStoreFile = BinaryFileUtil.getFile(
        rayConfig.sessionDir, BinaryFileUtil.PLASMA_STORE_SERVER_BINARY_NAME);
    Preconditions.checkState(objectStoreFile.setExecutable(true));
    List<String> command = ImmutableList.of(
        // The plasma store executable file.
        objectStoreFile.getAbsolutePath(),
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
