package io.ray.runtime.config;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import io.ray.api.id.JobId;
import io.ray.api.options.ActorLifetime;
import io.ray.runtime.generated.Common.WorkerType;
import io.ray.runtime.runtimeenv.RuntimeEnvImpl;
import io.ray.runtime.util.NetworkUtil;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;

/** Configurations of Ray runtime. See `ray.default.conf` for the meaning of each field. */
public class RayConfig {

  public static final String DEFAULT_CONFIG_FILE = "ray.default.conf";
  public static final String CUSTOM_CONFIG_FILE = "ray.conf";

  private Config config;

  /** IP of this node. if not provided, IP will be automatically detected. */
  public final String nodeIp;

  public final WorkerType workerMode;
  public final RunMode runMode;
  private JobId jobId;
  public String sessionDir;
  public String logDir;

  private String bootstrapAddress;
  public final String redisPassword;

  // RPC socket name of object store.
  public String objectStoreSocketName;

  // RPC socket name of Raylet.
  public String rayletSocketName;
  // Listening port for node manager.
  public int nodeManagerPort;

  public int startupToken;

  public int runtimeEnvHash;

  public RuntimeEnvImpl runtimeEnvImpl = null;

  public final ActorLifetime defaultActorLifetime;

  public static class LoggerConf {
    public final String loggerName;
    public final String fileName;
    public final String pattern;

    public LoggerConf(String loggerName, String fileName, String pattern) {
      this.loggerName = loggerName;
      this.fileName = fileName;
      this.pattern = pattern;
    }
  }

  public final List<LoggerConf> loggers;

  public final List<String> codeSearchPath;

  public final List<String> headArgs;

  public final int numWorkersPerProcess;

  public final String namespace;

  public final List<String> jvmOptionsForJavaWorker;

  private void validate() {
    if (workerMode == WorkerType.WORKER) {
      Preconditions.checkArgument(
          bootstrapAddress != null, "Bootstrap address must be set in worker mode.");
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
    if (config.hasPath("ray.local-mode")) {
      runMode = config.getBoolean("ray.local-mode") ? RunMode.SINGLE_PROCESS : RunMode.CLUSTER;
    } else {
      runMode = config.getEnum(RunMode.class, "ray.run-mode");
    }
    // Node ip.
    if (config.hasPath("ray.node-ip")) {
      nodeIp = config.getString("ray.node-ip");
    } else {
      if (SystemUtils.IS_OS_LINUX) {
        nodeIp = NetworkUtil.getIpAddress(null);
      } else {
        /// We use a localhost on MacOS or Windows to avid security popups.
        /// See the related issue https://github.com/ray-project/ray/issues/18730
        nodeIp = NetworkUtil.localhostIp();
      }
    }

    // Job id.
    String jobId = config.getString("ray.job.id");
    if (!jobId.isEmpty()) {
      this.jobId = JobId.fromHexString(jobId);
    } else {
      this.jobId = JobId.NIL;
    }

    // Namespace of this job.
    String localNamespace = config.getString("ray.job.namespace");
    if (workerMode == WorkerType.DRIVER) {
      namespace =
          StringUtils.isEmpty(localNamespace) ? UUID.randomUUID().toString() : localNamespace;
    } else {
      /// We shouldn't set it for worker.
      namespace = null;
    }

    defaultActorLifetime = config.getEnum(ActorLifetime.class, "ray.job.default-actor-lifetime");
    Preconditions.checkState(defaultActorLifetime != null);

    // jvm options for java workers of this job.
    jvmOptionsForJavaWorker = config.getStringList("ray.job.jvm-options");
    updateSessionDir(null);

    // Object store socket name.
    if (config.hasPath("ray.object-store.socket-name")) {
      objectStoreSocketName = config.getString("ray.object-store.socket-name");
    }

    // Raylet socket name.
    if (config.hasPath("ray.raylet.socket-name")) {
      rayletSocketName = config.getString("ray.raylet.socket-name");
    }

    // Bootstrap configurations.
    String bootstrapAddress = config.getString("ray.address");
    if (StringUtils.isNotBlank(bootstrapAddress)) {
      setBootstrapAddress(bootstrapAddress);
    } else {
      // We need to start gcs using `RunManager` for local cluster
      this.bootstrapAddress = null;
    }

    redisPassword = config.getString("ray.redis.password");
    // Raylet node manager port.
    if (config.hasPath("ray.raylet.node-manager-port")) {
      nodeManagerPort = config.getInt("ray.raylet.node-manager-port");
    } else {
      Preconditions.checkState(
          workerMode != WorkerType.WORKER,
          "Worker started by raylet should accept the node manager port from raylet.");
    }

    // Job code search path.
    String codeSearchPathString = null;
    if (config.hasPath("ray.job.code-search-path")) {
      codeSearchPathString = config.getString("ray.job.code-search-path");
    }
    if (StringUtils.isEmpty(codeSearchPathString)) {
      codeSearchPathString = System.getProperty("java.class.path");
    }
    codeSearchPath = Arrays.asList(codeSearchPathString.split(":"));

    numWorkersPerProcess = config.getInt("ray.job.num-java-workers-per-process");

    startupToken = config.getInt("ray.raylet.startup-token");

    /// Driver needn't this config item.
    if (workerMode == WorkerType.WORKER && config.hasPath("ray.internal.runtime-env-hash")) {
      runtimeEnvHash = config.getInt("ray.internal.runtime-env-hash");
    }

    {
      /// Runtime Env env-vars
      final String envVarsPath = "ray.job.runtime-env.env-vars";
      if (config.hasPath(envVarsPath)) {
        Map<String, String> envVars = new HashMap<>();
        Config envVarsConfig = config.getConfig(envVarsPath);
        envVarsConfig
            .entrySet()
            .forEach(
                (entry) -> {
                  envVars.put(entry.getKey(), ((String) entry.getValue().unwrapped()));
                });
        runtimeEnvImpl = new RuntimeEnvImpl(envVars, null);
      }
    }

    {
      loggers = new ArrayList<>();
      List<Config> loggerConfigs = (List<Config>) config.getConfigList("ray.logging.loggers");
      for (Config loggerConfig : loggerConfigs) {
        Preconditions.checkState(loggerConfig.hasPath("name"));
        Preconditions.checkState(loggerConfig.hasPath("file-name"));
        final String name = loggerConfig.getString("name");
        final String fileName = loggerConfig.getString("file-name");
        final String pattern =
            loggerConfig.hasPath("pattern") ? loggerConfig.getString("pattern") : "";
        loggers.add(new LoggerConf(name, fileName, pattern));
      }
    }

    headArgs = config.getStringList("ray.head-args");

    // Validate config.
    validate();
  }

  public void setBootstrapAddress(String bootstrapAddress) {
    Preconditions.checkNotNull(bootstrapAddress);
    Preconditions.checkState(this.bootstrapAddress == null, "Bootstrap address was already set");
    this.bootstrapAddress = bootstrapAddress;
  }

  public String getBootstrapAddress() {
    return this.bootstrapAddress;
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

  public int getStartupToken() {
    return startupToken;
  }

  public void setSessionDir(String sessionDir) {
    updateSessionDir(sessionDir);
  }

  public Config getInternalConfig() {
    return config;
  }

  /** Renders the config value as a HOCON string. */
  @Override
  public String toString() {
    // These items might be dynamically generated or mutated at runtime.
    // Explicitly include them.
    Map<String, Object> dynamic = new HashMap<>();
    dynamic.put("ray.session-dir", sessionDir);
    dynamic.put("ray.raylet.socket-name", rayletSocketName);
    dynamic.put("ray.object-store.socket-name", objectStoreSocketName);
    dynamic.put("ray.raylet.node-manager-port", nodeManagerPort);
    dynamic.put("ray.address", bootstrapAddress);
    dynamic.put("ray.raylet.startup-token", startupToken);
    Config toRender = ConfigFactory.parseMap(dynamic).withFallback(config);
    return toRender.root().render(ConfigRenderOptions.concise());
  }

  private void updateSessionDir(String sessionDir) {
    // session dir
    if (config.hasPath("ray.session-dir")) {
      sessionDir = config.getString("ray.session-dir");
    }
    if (sessionDir != null) {
      sessionDir = removeTrailingSlash(sessionDir);
    }
    this.sessionDir = sessionDir;

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
  }

  /**
   * Create a RayConfig by reading configuration in the following order: 1. System properties. 2.
   * `ray.conf` file. 3. `ray.default.conf` file.
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
