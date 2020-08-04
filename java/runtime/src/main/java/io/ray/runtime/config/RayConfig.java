package io.ray.runtime.config;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import io.ray.api.id.JobId;
import io.ray.runtime.generated.Common.WorkerType;
import io.ray.runtime.util.NetworkUtil;
import io.ray.runtime.util.ResourceUtil;
import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Configurations of Ray runtime.
 * See `ray.default.conf` for the meaning of each field.
 */
public class RayConfig {

  public static final String DEFAULT_CONFIG_FILE = "ray.default.conf";
  public static final String CUSTOM_CONFIG_FILE = "ray.conf";

  private static final Random RANDOM = new Random();

  private static final DateTimeFormatter DATE_TIME_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");

  private static final String DEFAULT_TEMP_DIR = "/tmp/ray";

  private Config config;

  public final String nodeIp;
  public final WorkerType workerMode;
  public final RunMode runMode;
  public final Map<String, Double> resources;
  private JobId jobId;
  public String sessionDir;
  public String logDir;
  public final List<String> libraryPath;
  public final List<String> classpath;
  public final List<String> jvmParameters;

  private String redisAddress;
  private String redisIp;
  private Integer redisPort;
  public final int headRedisPort;
  public final int[] redisShardPorts;
  public final int numberRedisShards;
  public final String headRedisPassword;
  public final String redisPassword;

  public String objectStoreSocketName;
  public final Long objectStoreSize;

  public String rayletSocketName;
  private int nodeManagerPort;
  public final Map<String, String> rayletConfigParameters;

  public final String jobResourcePath;
  public final String pythonWorkerCommand;

  private static volatile RayConfig instance = null;

  public static RayConfig getInstance() {
    if (instance == null) {
      synchronized (RayConfig.class) {
        if (instance == null) {
          instance = RayConfig.create();
        }
      }
    }
    return instance;
  }

  public static void reset() {
    synchronized (RayConfig.class) {
      instance = null;
    }
  }


  public final int numWorkersPerProcess;

  private void validate() {
    if (workerMode == WorkerType.WORKER) {
      Preconditions.checkArgument(redisAddress != null,
          "Redis address must be set in worker mode.");
    }
  }

  private String removeTrailingSlash(String path) {
    if (path.endsWith("/")) {
      return path.substring(0, path.length() - 1);
    } else {
      return path;
    }
  }

  public RayConfig(Config config) {
    this.config = config;
    // Worker mode.
    WorkerType localWorkerMode;
    try {
      localWorkerMode = config.getEnum(WorkerType.class, "ray.worker.mode");
    } catch (ConfigException.Missing e) {
      localWorkerMode = WorkerType.DRIVER;
    }
    workerMode = localWorkerMode;
    boolean isDriver = workerMode == WorkerType.DRIVER;
    // Run mode.
    runMode = config.getEnum(RunMode.class, "ray.run-mode");
    // Node ip.
    String nodeIp = config.getString("ray.node-ip");
    if (nodeIp.isEmpty()) {
      nodeIp = NetworkUtil.getIpAddress(null);
    }
    this.nodeIp = nodeIp;
    // Resources.
    resources = ResourceUtil.getResourcesMapFromString(
        config.getString("ray.resources"));
    if (isDriver) {
      if (!resources.containsKey("CPU")) {
        int numCpu = Runtime.getRuntime().availableProcessors();
        resources.put("CPU", numCpu * 1.0);
      }
    }
    // Job id.
    String jobId = config.getString("ray.job.id");
    if (!jobId.isEmpty()) {
      this.jobId = JobId.fromHexString(jobId);
    } else {
      this.jobId = JobId.NIL;
    }

    updateSessionDir();
    // Object store configurations.
    objectStoreSize = config.getBytes("ray.object-store.size");

    // Library path.
    libraryPath = config.getStringList("ray.library.path");
    // Custom classpath.
    classpath = config.getStringList("ray.classpath");
    // Custom worker jvm parameters.
    if (config.hasPath("ray.worker.jvm-parameters")) {
      jvmParameters = config.getStringList("ray.worker.jvm-parameters");
    } else {
      jvmParameters = ImmutableList.of();
    }

    if (config.hasPath("ray.worker.python-command")) {
      pythonWorkerCommand = config.getString("ray.worker.python-command");
    } else {
      pythonWorkerCommand = null;
    }

    // Redis configurations.
    String redisAddress = config.getString("ray.redis.address");
    if (!redisAddress.isEmpty()) {
      setRedisAddress(redisAddress);
    } else {
      this.redisAddress = null;
    }

    if (config.hasPath("ray.redis.head-port")) {
      headRedisPort = config.getInt("ray.redis.head-port");
    } else {
      headRedisPort = NetworkUtil.getUnusedPort();
    }
    numberRedisShards = config.getInt("ray.redis.shard-number");
    redisShardPorts = new int[numberRedisShards];
    for (int i = 0; i < numberRedisShards; i++) {
      redisShardPorts[i] = NetworkUtil.getUnusedPort();
    }
    headRedisPassword = config.getString("ray.redis.head-password");
    redisPassword = config.getString("ray.redis.password");

    // Raylet node manager port.
    nodeManagerPort = config.getInt("ray.raylet.node-manager-port");
    if (nodeManagerPort == 0) {
      Preconditions.checkState(this.redisAddress == null,
          "Java worker started by raylet should accept the node manager port from raylet.");
      nodeManagerPort = NetworkUtil.getUnusedPort();
    }

    // Raylet parameters.
    rayletConfigParameters = new HashMap<>();
    Config rayletConfig = config.getConfig("ray.raylet.config");
    for (Map.Entry<String, ConfigValue> entry : rayletConfig.entrySet()) {
      Object value = entry.getValue().unwrapped();
      rayletConfigParameters.put(entry.getKey(), value == null ? "" : value.toString());
    }

    // Job resource path.
    if (config.hasPath("ray.job.resource-path")) {
      jobResourcePath = config.getString("ray.job.resource-path");
    } else {
      jobResourcePath = null;
    }

    numWorkersPerProcess = config.getInt("ray.raylet.config.num_workers_per_process_java");

    // Validate config.
    validate();
  }

  public void setRedisAddress(String redisAddress) {
    Preconditions.checkNotNull(redisAddress);
    Preconditions.checkState(this.redisAddress == null, "Redis address was already set");

    this.redisAddress = redisAddress;
    String[] ipAndPort = redisAddress.split(":");
    Preconditions.checkArgument(ipAndPort.length == 2, "Invalid redis address.");
    this.redisIp = ipAndPort[0];
    this.redisPort = Integer.parseInt(ipAndPort[1]);
  }

  public String getRedisAddress() {
    return redisAddress;
  }

  public String getRedisIp() {
    return redisIp;
  }

  public Integer getRedisPort() {
    return redisPort;
  }

  public void setJobId(JobId jobId) {
    this.jobId = jobId;
  }

  public JobId getJobId() {
    return this.jobId;
  }

  public int getNodeManagerPort() {
    return nodeManagerPort;
  }

  public void setSessionDir(String sessionDir) {
    this.sessionDir = sessionDir;
  }

  public String getSessionDir() {
    return sessionDir;
  }

  public Config getInternalConfig() {
    return config;
  }

  /**
   * Renders the config value as a HOCON string.
   */
  public String render() {
    // These items might be dynamically generated or mutated at runtime.
    // Explicitly include them.
    Map<String, Object> dynamic = new HashMap<>();
    dynamic.put("ray.session-dir", sessionDir);
    dynamic.put("ray.raylet.socket-name", rayletSocketName);
    dynamic.put("ray.object-store.socket-name", objectStoreSocketName);
    dynamic.put("ray.raylet.node-manager-port", nodeManagerPort);
    dynamic.put("ray.redis.address", redisAddress);
    dynamic.put("ray.job.resource-path", jobResourcePath);
    Config toRender = ConfigFactory.parseMap(dynamic).withFallback(config);
    return toRender.root().render(ConfigRenderOptions.concise());
  }

  private void updateSessionDir() {
    // session dir
    if (workerMode == WorkerType.DRIVER) {
      final int minBound = 100000;
      final int maxBound = 999999;
      final String sessionName = String.format("session_%s_%d", DATE_TIME_FORMATTER.format(
          LocalDateTime.now()), RANDOM.nextInt(maxBound - minBound) + minBound);
      sessionDir = String.format("%s/%s", DEFAULT_TEMP_DIR, sessionName);
    } else if (workerMode == WorkerType.WORKER) {
      sessionDir = removeTrailingSlash(config.getString("ray.session-dir"));
    } else {
      throw new RuntimeException("Unknown worker type.");
    }

    // Log dir.
    String localLogDir = null;
    if (config.hasPath("ray.logging.dir")) {
      localLogDir = removeTrailingSlash(config.getString("ray.logging.dir"));
    }
    if (Strings.isNullOrEmpty(localLogDir)) {
      logDir = String.format("%s/logs", sessionDir);
    } else {
      logDir = localLogDir;
    }

    // Object store socket name.
    String localObjectStoreSocketName = null;
    if (config.hasPath("ray.object-store.socket-name")) {
      localObjectStoreSocketName = config.getString("ray.object-store.socket-name");
    }
    if (Strings.isNullOrEmpty(localObjectStoreSocketName)) {
      objectStoreSocketName = String.format("%s/sockets/object_store", sessionDir);
    } else {
      objectStoreSocketName = localObjectStoreSocketName;
    }

    // Raylet socket name.
    String localRayletSocketName = null;
    if (config.hasPath("ray.raylet.socket-name")) {
      localRayletSocketName = config.getString("ray.raylet.socket-name");
    }
    if (Strings.isNullOrEmpty(localRayletSocketName)) {
      rayletSocketName = String.format("%s/sockets/raylet", sessionDir);
    } else {
      rayletSocketName = localRayletSocketName;
    }

  }

  @Override
  public String toString() {
    return render();
  }

  /**
   * Create a RayConfig by reading configuration in the following order:
   * 1. System properties.
   * 2. `ray.conf` file.
   * 3. `ray.default.conf` file.
   */
  public static RayConfig create() {
    ConfigFactory.invalidateCaches();
    Config config = ConfigFactory.systemProperties();
    String configPath = System.getProperty("ray.config-file");
    if (Strings.isNullOrEmpty(configPath)) {
      config = config.withFallback(ConfigFactory.load(CUSTOM_CONFIG_FILE));
    } else {
      config = config.withFallback(ConfigFactory.parseFile(new File(configPath)));
    }
    config = config.withFallback(ConfigFactory.load(DEFAULT_CONFIG_FILE));
    return new RayConfig(config.withOnlyPath("ray"));
  }

}
