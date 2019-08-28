package org.ray.runtime.config;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.ray.api.id.JobId;
import org.ray.runtime.generated.Common.WorkerType;
import org.ray.runtime.util.NetworkUtil;
import org.ray.runtime.util.ResourceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configurations of Ray runtime.
 * See `ray.default.conf` for the meaning of each field.
 */
public class RayConfig {

  private static final Logger LOGGER = LoggerFactory.getLogger(RayConfig.class);

  public static final String DEFAULT_CONFIG_FILE = "ray.default.conf";
  public static final String CUSTOM_CONFIG_FILE = "ray.conf";

  public final String nodeIp;
  public final WorkerType workerMode;
  public final RunMode runMode;
  public final Map<String, Double> resources;
  private JobId jobId;
  public final String logDir;
  public final boolean redirectOutput;
  public final List<String> libraryPath;
  public final List<String> classpath;
  public final List<String> jvmParameters;

  private String redisAddress;
  private String redisIp;
  private Integer redisPort;
  public final int headRedisPort;
  public final int numberRedisShards;
  public final String headRedisPassword;
  public final String redisPassword;

  public final String objectStoreSocketName;
  public final Long objectStoreSize;

  public final String rayletSocketName;
  public final List<String> rayletConfigParameters;

  public final String jobResourcePath;
  public final String pythonWorkerCommand;

  /**
   * Number of threads that execute tasks.
   */
  public final int numberExecThreadsForDevRuntime;

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
        LOGGER.warn("No CPU resource is set in configuration, "
            + "setting it to the number of CPU cores: {}", numCpu);
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
    // Log dir.
    logDir = removeTrailingSlash(config.getString("ray.log-dir"));
    // Redirect output.
    redirectOutput = config.getBoolean("ray.redirect-output");
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
    headRedisPort = config.getInt("ray.redis.head-port");
    numberRedisShards = config.getInt("ray.redis.shard-number");
    headRedisPassword = config.getString("ray.redis.head-password");
    redisPassword = config.getString("ray.redis.password");

    // Object store configurations.
    objectStoreSocketName = config.getString("ray.object-store.socket-name");
    objectStoreSize = config.getBytes("ray.object-store.size");

    // Raylet socket name.
    rayletSocketName = config.getString("ray.raylet.socket-name");

    // Raylet parameters.
    rayletConfigParameters = new ArrayList<>();
    Config rayletConfig = config.getConfig("ray.raylet.config");
    for (Map.Entry<String,ConfigValue> entry : rayletConfig.entrySet()) {
      String parameter = entry.getKey() + "," + entry.getValue().unwrapped();
      rayletConfigParameters.add(parameter);
    }

    // Job resource path.
    if (config.hasPath("ray.job.resource-path")) {
      jobResourcePath = config.getString("ray.job.resource-path");
    } else {
      jobResourcePath = null;
    }

    // Number of threads that execute tasks.
    numberExecThreadsForDevRuntime = config.getInt("ray.dev-runtime.execution-parallelism");

    // Validate config.
    validate();
    LOGGER.debug("Created config: {}", this);
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

  @Override
  public String toString() {
    return "RayConfig{"
        + ", nodeIp='" + nodeIp + '\''
        + ", workerMode=" + workerMode
        + ", runMode=" + runMode
        + ", resources=" + resources
        + ", jobId=" + jobId
        + ", logDir='" + logDir + '\''
        + ", redirectOutput=" + redirectOutput
        + ", libraryPath=" + libraryPath
        + ", classpath=" + classpath
        + ", jvmParameters=" + jvmParameters
        + ", redisAddress='" + redisAddress + '\''
        + ", redisIp='" + redisIp + '\''
        + ", redisPort=" + redisPort
        + ", headRedisPort=" + headRedisPort
        + ", numberRedisShards=" + numberRedisShards
        + ", objectStoreSocketName='" + objectStoreSocketName + '\''
        + ", objectStoreSize=" + objectStoreSize
        + ", rayletSocketName='" + rayletSocketName + '\''
        + ", rayletConfigParameters=" + rayletConfigParameters
        + ", jobResourcePath='" + jobResourcePath + '\''
        + ", pythonWorkerCommand='" + pythonWorkerCommand + '\''
        + '}';
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
    String configPath = System.getProperty("ray.config");
    if (Strings.isNullOrEmpty(configPath)) {
      LOGGER.info("Loading config from \"ray.conf\" file in classpath.");
      config = config.withFallback(ConfigFactory.load(CUSTOM_CONFIG_FILE));
    } else {
      LOGGER.info("Loading config from " + configPath + ".");
      config = config.withFallback(ConfigFactory.parseFile(new File(configPath)));
    }
    config = config.withFallback(ConfigFactory.load(DEFAULT_CONFIG_FILE));
    return new RayConfig(config);
  }

}
