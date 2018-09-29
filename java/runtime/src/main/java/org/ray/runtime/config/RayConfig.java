package org.ray.runtime.config;


import com.google.common.base.Preconditions;
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
 * See `ray.default.conf` for the meaning of each field.
 */
public class RayConfig {

  private Logger logger = LoggerFactory.getLogger(RayConfig.class);

  private static final String DEFAULT_CONFIG_FILE = "ray.default.conf";

  public final String rayHome;
  public final String nodeIp;
  public final WorkerMode workerMode;
  public final RunMode runMode;
  public final Map<String, Double> resources;
  public final UniqueId driverId;
  public final String logDir;
  public final List<String> libraryPath;
  public final List<String> classpath;
  public final List<String> jvmParameters;

  private String redisAddress;
  private String redisIp;
  private Integer redisPort;
  public final int headRedisPort;
  public final int numberRedisShards;

  public final String objectStoreSocketName;
  public final Long objectStoreSize;

  public final String rayletSocketName;

  public final String redisServerExecutablePath;
  public final String redisModulePath;
  public final String plasmaStoreExecutablePath;
  public final String rayletExecutablePath;
  public final String driverResourcePath;

  private Config config;

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
    // keep the config, so the user could set his own properties
    // and get the key-value in RayConfig
    this.config = config;

    // worker mode
    WorkerMode localWorkerMode;
    try {
      localWorkerMode = config.getEnum(WorkerMode.class, "ray.mode");
    } catch (ConfigException.Missing e) {
      localWorkerMode = WorkerMode.DRIVER;
    }

    workerMode = localWorkerMode;
    boolean isDriver = workerMode == WorkerMode.DRIVER;
    // run mode
    runMode = config.getEnum(RunMode.class, "ray.run-mode");
    // ray home
    String localRayHome = config.getString("ray.home");
    if (!localRayHome.startsWith("/")) {
      // If ray.home isn't an absolute path, prepend it with current work dir.
      localRayHome = System.getProperty("user.dir") + "/" + localRayHome;
    }
    rayHome = removeTrailingSlash(localRayHome);
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
        logger.warn("No CPU resource is set in configuration, "
            + "setting it to the number of CPU cores: {}", numCpu);
        resources.put("CPU", numCpu * 1.0);
      }
      if (!resources.containsKey("GPU")) {
        logger.warn("No GPU resource is set in configuration, setting it to 0");
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

    // worker
    // native library path
    this.libraryPath = config.getStringList("ray.worker.library.path");
    // class path for worker
    this.classpath = config.getStringList("ray.worker.classpath");
    // worker custom jvm properties
    this.jvmParameters = config.getStringList("ray.worker.jvm-parameters");

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

    // redis server binary file
    this.redisServerExecutablePath = config.getString("ray.redis.redis-server");

    // redis module file
    this.redisModulePath = config.getString("ray.redis.redis-module");

    // plasma store binary file
    this.plasmaStoreExecutablePath = config.getString("ray.object-store.plasma-store");

    // raylet binary file
    this.rayletExecutablePath = config.getString("ray.raylet.raylet-bin");

    // driver resource path
    String localDriverResourcePath;
    if (config.hasPath("ray.driver.resource-path")) {
      localDriverResourcePath = config.getString("ray.driver.resource-path");
    } else {
      localDriverResourcePath = rayHome + "/driver/resource";
      LOGGER.warn("Didn't configure ray.driver.resource-path, set it to default value: {}",
          localDriverResourcePath);
    }

    driverResourcePath = localDriverResourcePath;

    // validate config
    validate();
    logger.debug("Created config: {}", this);
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
        + ", redisServerExecutablePath='" + redisServerExecutablePath + '\''
        + ", plasmaStoreExecutablePath='" + plasmaStoreExecutablePath + '\''
        + ", rayletExecutablePath='" + rayletExecutablePath + '\''
        + '}';
  }

  public Config getConfig() {
    return config;
  }

  /**
   * Create a RayConfig by reading configuration in the following order:
   * 1. System properties.
   * 2. `@{config}.conf` file.
   * 3. `ray.default.conf` file.
  */
  public static RayConfig create(String config) {
    ConfigFactory.invalidateCaches();
    Config defaultConfig = ConfigFactory.parseResources(DEFAULT_CONFIG_FILE);
    if (config == null) {
      return new RayConfig(ConfigFactory.systemProperties()
          .withFallback(defaultConfig).resolve());
    } else {
      return new RayConfig(ConfigFactory.load(config)
          .withFallback(defaultConfig).resolve());
    }
  }

}
