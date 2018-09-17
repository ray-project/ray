package org.ray.runtime.config;


import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Map;
import org.ray.api.id.UniqueId;
import org.ray.runtime.util.NetworkUtil;
import org.ray.runtime.util.ResourceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configurations of Ray runtime.
 */
public class RayConfig {

  private static final Logger LOGGER = LoggerFactory.getLogger(RayConfig.class);

  public static final String DEFAULT_CONFIG_FILE = "ray.default.conf";
  public static final String CUSTOM_CONFIG_FILE = "ray.conf";

  public final String rayHome;
  public final String nodeIp;
  public final WorkerMode workerMode;
  public final RunMode runMode;
  public final Map<String, Double> resources;
  public final UniqueId driverId;
  public final String logDir;
  public final boolean redirectOutput;
  public final List<String> libraryPath;
  public final List<String> classpath;

  private String redisAddress;
  private String redisIp;
  private Integer redisPort;
  public final int headRedisPort;
  public final int numberRedisShards;

  public final String objectStoreSocketName;
  public final Long objectStoreSize;

  public final String rayletSocketName;

  public final int defaultFirstCheckTimeoutMs;
  public final int defaultGetCheckIntervalMs;
  public final int fetchBatchSize;

  public final String redisServerExecutablePath;
  public final String redisModulePath;
  public final String plasmaStoreExecutablePath;
  public final String rayletExecutablePath;

  private void validate() {
    if (workerMode == WorkerMode.WORKER) {
      Preconditions.checkArgument(redisAddress != null,
          "Redis address must be set in worker mode.");
    } else {
      Preconditions.checkArgument(!rayHome.isEmpty(),
          "'ray.home' must be set in driver mode");
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
    // worker mode
    WorkerMode localWorkerMode;
    try {
      localWorkerMode = config.getEnum(WorkerMode.class, "ray.worker.mode");
    } catch (ConfigException.Missing e) {
      localWorkerMode = WorkerMode.DRIVER;
    }

    workerMode = localWorkerMode;
    boolean isDriver = workerMode == WorkerMode.DRIVER;
    // run mode
    runMode = config.getEnum(RunMode.class, "ray.run-mode");
    // ray home
    String rayHome = config.getString("ray.home");
    if (!rayHome.startsWith("/")) {
      // If ray.home isn't an absolute path, prepend it with current work dir.
      rayHome = System.getProperty("user.dir") + "/" + rayHome;
    }
    this.rayHome = removeTrailingSlash(rayHome);
    // node ip
    String nodeIp = config.getString("ray.node-ip");
    if (nodeIp.isEmpty()) {
      nodeIp = NetworkUtil.getIpAddress(null);
    }
    this.nodeIp = nodeIp;
    // resources
    resources = ResourceUtil.getResourcesMapFromString(
        config.getString("ray.resources"));
    if (isDriver) {
      if (!resources.containsKey("CPU")) {
        int numCpu = Runtime.getRuntime().availableProcessors();
        LOGGER.warn("No CPU resource is set in configuration, "
            + "setting it to the number of CPU cores: {}", numCpu);
        resources.put("CPU", numCpu * 1.0);
      }
      if (!resources.containsKey("GPU")) {
        LOGGER.warn("No GPU resource is set in configuration, setting it to 0");
        resources.put("GPU", 0.0);
      }
    }
    // driver id
    String driverId = config.getString("ray.driver.id");
    if (!driverId.isEmpty()) {
      this.driverId = UniqueId.fromHexString(driverId);
    } else {
      this.driverId = UniqueId.randomId();
    }
    // log dir
    logDir = removeTrailingSlash(config.getString("ray.log-dir"));
    // redirect output
    redirectOutput = config.getBoolean("ray.redirect-output");
    // custom library path
    List<String> customLibraryPath = config.getStringList("ray.library.path");
    // custom classpath
    classpath = config.getStringList("ray.classpath");

    // redis configurations
    String redisAddress = config.getString("ray.redis.address");
    if (!redisAddress.isEmpty()) {
      setRedisAddress(redisAddress);
    } else {
      this.redisAddress = null;
    }
    headRedisPort = config.getInt("ray.redis.head-port");
    numberRedisShards = config.getInt("ray.redis.shard-number");

    // object store configurations
    objectStoreSocketName = config.getString("ray.object-store.socket-name");
    objectStoreSize = config.getBytes("ray.object-store.size");

    // raylet socket name
    rayletSocketName = config.getString("ray.raylet.socket-name");

    // runtime configurations
    defaultFirstCheckTimeoutMs = config.getInt("ray.runtime.default-first-check-timeout-ms");
    defaultGetCheckIntervalMs = config.getInt("ray.runtime.default-get-check-interval-ms");
    fetchBatchSize = config.getInt("ray.runtime.fetch-batch-size");

    // library path
    this.libraryPath = new ImmutableList.Builder<String>().add(
        rayHome + "/build/src/plasma",
        rayHome + "/build/src/local_scheduler"
    ).addAll(customLibraryPath).build();

    redisServerExecutablePath = rayHome + "/build/src/common/thirdparty/redis/src/redis-server";
    redisModulePath = rayHome + "/build/src/common/redis_module/libray_redis_module.so";
    plasmaStoreExecutablePath = rayHome + "/build/src/plasma/plasma_store_server";
    rayletExecutablePath = rayHome + "/build/src/ray/raylet/raylet";

    // validate config
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

  @Override
  public String toString() {
    return "RayConfig{"
        + "rayHome='" + rayHome + '\''
        + ", nodeIp='" + nodeIp + '\''
        + ", workerMode=" + workerMode
        + ", runMode=" + runMode
        + ", resources=" + resources
        + ", driverId=" + driverId
        + ", logDir='" + logDir + '\''
        + ", redirectOutput=" + redirectOutput
        + ", libraryPath=" + libraryPath
        + ", classpath=" + classpath
        + ", redisAddress='" + redisAddress + '\''
        + ", redisIp='" + redisIp + '\''
        + ", redisPort=" + redisPort
        + ", headRedisPort=" + headRedisPort
        + ", numberRedisShards=" + numberRedisShards
        + ", objectStoreSocketName='" + objectStoreSocketName + '\''
        + ", objectStoreSize=" + objectStoreSize
        + ", rayletSocketName='" + rayletSocketName + '\''
        + ", defaultFirstCheckTimeoutMs=" + defaultFirstCheckTimeoutMs
        + ", defaultGetCheckIntervalMs=" + defaultGetCheckIntervalMs
        + ", fetchBatchSize=" + fetchBatchSize
        + ", redisServerExecutablePath='" + redisServerExecutablePath + '\''
        + ", plasmaStoreExecutablePath='" + plasmaStoreExecutablePath + '\''
        + ", rayletExecutablePath='" + rayletExecutablePath + '\''
        + '}';
  }

  /**
   * Create a RayConfig by reading configuration in the following order:
   * 1. System properties.
   * 2. `ray.conf` file.
   * 3. `ray.default.conf` file.
  */
  public static RayConfig create() {
    Config config = ConfigFactory.systemProperties()
        .withFallback(ConfigFactory.load(CUSTOM_CONFIG_FILE))
        .withFallback(ConfigFactory.load(DEFAULT_CONFIG_FILE));
    return new RayConfig(config);
  }

}
