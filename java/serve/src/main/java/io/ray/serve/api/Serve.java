package io.ray.serve.api;

import com.google.common.base.Preconditions;
import io.ray.api.ActorHandle;
import io.ray.api.BaseActorHandle;
import io.ray.api.PyActorHandle;
import io.ray.api.Ray;
import io.ray.api.function.PyActorClass;
import io.ray.api.function.PyActorMethod;
import io.ray.api.options.ActorLifetime;
import io.ray.runtime.exception.RayTimeoutException;
import io.ray.serve.AutoscalingConfig;
import io.ray.serve.Constants;
import io.ray.serve.DeploymentConfig;
import io.ray.serve.DeploymentInfo;
import io.ray.serve.ProxyActor;
import io.ray.serve.RayServeException;
import io.ray.serve.ReplicaContext;
import io.ray.serve.generated.ActorNameList;
import io.ray.serve.util.CommonUtil;
import io.ray.serve.util.LogUtil;
import io.ray.serve.util.ServeProtoUtil;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Ray Serve global API. TODO: will be riched in the Java SDK/API PR. */
public class Serve {

  private static final Logger LOGGER = LoggerFactory.getLogger(Serve.class);

  private static ReplicaContext INTERNAL_REPLICA_CONTEXT;

  private static Client GLOBAL_CLIENT;

  private static Pattern UUID_RE =
      Pattern.compile("[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89aAbB][a-f0-9]{3}-[a-f0-9]{12}");

  /**
   * Initialize a serve instance.
   *
   * <p>By default, the instance will be scoped to the lifetime of the returned Client object (or
   * when the script exits). If detached is set to True, the instance will instead persist until
   * Serve.shutdown() is called. This is only relevant if connecting to a long-running Ray cluster.
   *
   * @param detached Whether not the instance should be detached from this script. If set, the
   *     instance will live on the Ray cluster until it is explicitly stopped with Serve.shutdown().
   * @param dedicatedCpu Whether to reserve a CPU core for the internal Serve controller actor.
   *     Defaults to False.
   * @param checkpointPath
   * @param config Configuration options for Serve.
   * @return
   */
  @SuppressWarnings("unchecked")
  public static synchronized Client start(
      boolean detached, boolean dedicatedCpu, String checkpointPath, Map<String, String> config) {

    // Initialize ray if needed.
    if (!Ray.isInitialized()) {
      System.setProperty("ray.job.namespace", "serve");
      Ray.init();
    }

    String controllerNamespace = getControllerNamespace(detached);

    try {
      Client client = getGlobalClient();
      LOGGER.info("Connecting to existing Serve instance in namespace {}", controllerNamespace);
      checkCheckpointPath(client, checkpointPath);
      return client;
    } catch (RayServeException e) {
      LOGGER.info("There is no instance running on this Ray cluster. A new one will be started.");
    }

    String controllerName = null;
    if (detached) {
      controllerName = Constants.SERVE_CONTROLLER_NAME;
    } else {
      controllerName =
          CommonUtil.formatActorName(
              Constants.SERVE_CONTROLLER_NAME, RandomStringUtils.randomAlphabetic(6));
    }

    // TODO The namespace, max_task_retries and dispatching on head node is not supported in Java
    // now.
    PyActorHandle controller =
        Ray.actor(
                PyActorClass.of("ray.serve.controller", "ServeController"),
                controllerName,
                null, // http_config TODO change it nullable or define protobuf.
                checkpointPath,
                detached,
                null)
            .setName(controllerName)
            .setLifetime(detached ? ActorLifetime.DETACHED : ActorLifetime.NON_DETACHED)
            .setMaxRestarts(-1)
            .setResource("CPU", dedicatedCpu ? 1.0 : 0.0)
            .setMaxConcurrency(Constants.CONTROLLER_MAX_CONCURRENCY)
            .remote();

    List<String> proxyNames = null;
    byte[] actorNameListProtoBytes =
        (byte[]) controller.task(PyActorMethod.of("get_http_proxy_names")).remote().get();
    ActorNameList actorNameList =
        ServeProtoUtil.bytesToProto(
            actorNameListProtoBytes, bytes -> ActorNameList.parseFrom(bytes));
    if (actorNameList != null) {
      proxyNames = actorNameList.getNamesList();
    }

    if (proxyNames != null && proxyNames.size() > 0) {
      try {
        for (String name : proxyNames) {
          ActorHandle<ProxyActor> proxyActorHandle =
              (ActorHandle<ProxyActor>) Ray.getActor(name).get();
          proxyActorHandle.task(ProxyActor::ready).remote().get(Constants.PROXY_TIMEOUT * 1000);
        }
      } catch (RayTimeoutException e) {
        String errMsg = LogUtil.format("Proxies not available after {}s.", Constants.PROXY_TIMEOUT);
        LOGGER.error(errMsg, e);
        throw new RayServeException(errMsg, e);
      }
    }

    Client client = new Client(controller, controllerName, detached);
    setGlobalClient(client);
    LOGGER.info(
        "Started{}Serve instance in namespace {}",
        detached ? " detached " : " ",
        controllerNamespace);
    return client;
  }

  private static void checkCheckpointPath(Client client, String checkpointPath) {
    if (StringUtils.isNotBlank(checkpointPath)
        && !StringUtils.equals(checkpointPath, client.getCheckpointPath())) {
      LOGGER.warn(
          "The new client checkpoint path '{}' is different from the existing one '{}'. The new checkpoint path is ignored.",
          checkpointPath,
          client.getCheckpointPath());
    }
  }

  public static String getControllerNamespace(boolean detached) {
    String controllerNamespace = Ray.getRuntimeContext().getNamespace();

    if (!detached) {
      return controllerNamespace;
    }

    // Start controller in "serve" namespace if detached and currently in anonymous namespace.
    if (UUID_RE.matcher(controllerNamespace).matches()) {
      controllerNamespace = "serve";
    }
    return controllerNamespace;
  }

  /**
   * Completely shut down the connected Serve instance.
   * <p>Shuts down all processes and deletes all state associated with the instance.
   */
  public static void shutdown() {
    if (GLOBAL_CLIENT == null) {
      return;
    }

    getGlobalClient().shutdown();
    setGlobalClient(null);
  }

  /**
   * Define a Serve deployment.
   *
   * @param deploymentDef
   * @param name Globally-unique name identifying this deployment. If not provided, the name of the
   *     class or function will be used.
   * @param version Version of the deployment. This is used to indicate a code change for the
   *     deployment; when it is re-deployed with a version change, a rolling update of the replicas
   *     will be performed. If not provided, every deployment will be treated as a new version.
   * @param prevVersion Version of the existing deployment which is used as a precondition for the
   *     next deployment. If prev_version does not match with the existing deployment's version, the
   *     deployment will fail. If not provided, deployment procedure will not check the existing
   *     deployment's version.
   * @param numReplicas The number of processes to start up that will handle requests to this
   *     deployment. Defaults to 1.
   * @param initArgs Positional args to be passed to the class constructor when starting up
   *     deployment replicas. These can also be passed when you call `.deploy()` on the returned
   *     Deployment.
   * @param routePrefix Requests to paths under this HTTP path prefix will be routed to this
   *     deployment. Defaults to '/{name}'. When set to 'None', no HTTP endpoint will be created.
   *     Routing is done based on longest-prefix match, so if you have deployment A with a prefix of
   *     '/a' and deployment B with a prefix of '/a/b', requests to '/a', '/a/', and '/a/c' go to A
   *     and requests to '/a/b', '/a/b/', and '/a/b/c' go to B. Routes must not end with a '/'
   *     unless they're the root (just '/'), which acts as a catch-all.
   * @param rayActorOptions Options to be passed to the Ray actor constructor such as resource
   *     requirements.
   * @param userConfig [experimental] Config to pass to the reconfigure method of the deployment.
   *     This can be updated dynamically without changing the version of the deployment and
   *     restarting its replicas. The user_config needs to be hashable to keep track of updates, so
   *     it must only contain hashable types, or hashable types nested in lists and dictionaries.
   * @param maxConcurrentQueries The maximum number of queries that will be sent to a replica of
   *     this deployment without receiving a response. Defaults to 100.
   * @param autoscalingConfig
   * @param gracefulShutdownWaitLoopS
   * @param gracefulShutdownTimeoutS
   * @param healthCheckPeriodS
   * @param healthCheckTimeoutS
   * @return
   */
  public static Deployment deployment(
      String deploymentDef,
      String name,
      String version,
      String prevVersion,
      Integer numReplicas,
      Object[] initArgs,
      String routePrefix,
      Map<String, Object> rayActorOptions,
      Object userConfig,
      Integer maxConcurrentQueries,
      AutoscalingConfig autoscalingConfig,
      Double gracefulShutdownWaitLoopS,
      Double gracefulShutdownTimeoutS,
      Double healthCheckPeriodS,
      Double healthCheckTimeoutS) {

    Preconditions.checkArgument(
        numReplicas != null && autoscalingConfig != null,
        "Manually setting num_replicas is not allowed when autoscalingConfig is provided.");

    DeploymentConfig deploymentConfig = new DeploymentConfig();
    if (numReplicas != null) {
      deploymentConfig.setNumReplicas(numReplicas);
    }
    if (userConfig != null) {
      deploymentConfig.setUserConfig(userConfig);
    }
    if (maxConcurrentQueries != null) {
      deploymentConfig.setMaxConcurrentQueries(maxConcurrentQueries);
    }
    if (autoscalingConfig != null) {
      deploymentConfig.setAutoscalingConfig(autoscalingConfig);
    }
    if (gracefulShutdownWaitLoopS != null) {
      deploymentConfig.setGracefulShutdownWaitLoopS(gracefulShutdownWaitLoopS);
    }
    if (gracefulShutdownTimeoutS != null) {
      deploymentConfig.setGracefulShutdownTimeoutS(gracefulShutdownTimeoutS);
    }
    if (healthCheckPeriodS != null) {
      deploymentConfig.setHealthCheckPeriodS(healthCheckPeriodS);
    }
    if (healthCheckTimeoutS != null) {
      deploymentConfig.setHealthCheckTimeoutS(healthCheckTimeoutS);
    }

    return new Deployment(
        deploymentDef,
        name,
        deploymentConfig,
        version,
        prevVersion,
        initArgs,
        routePrefix,
        rayActorOptions);
  }

  public static Deployment deployment() {
    return new Deployment();
  }

  /**
   * Set replica information to global context.
   *
   * @param deploymentName deployment name
   * @param replicaTag replica tag
   * @param controllerName the controller actor's name
   * @param controllerNamespace
   * @param servableObject the servable object of the specified replica.
   */
  public static void setInternalReplicaContext(
      String deploymentName,
      String replicaTag,
      String controllerName,
      String controllerNamespace,
      Object servableObject) {
    INTERNAL_REPLICA_CONTEXT =
        new ReplicaContext(
            deploymentName, replicaTag, controllerName, controllerNamespace, servableObject);
  }

  public static void setInternalReplicaContext(ReplicaContext replicaContext) {
    INTERNAL_REPLICA_CONTEXT = replicaContext;
  }

  /**
   * Get the global replica context.
   *
   * @return the replica context if it exists, or throw RayServeException.
   */
  public static ReplicaContext getReplicaContext() {
    if (INTERNAL_REPLICA_CONTEXT == null) {
      throw new RayServeException(
          "`Serve.getReplicaContext()` may only be called from within a Ray Serve deployment.");
    }
    return INTERNAL_REPLICA_CONTEXT;
  }

  protected static Client getGlobalClient() {
    if (GLOBAL_CLIENT != null) {
      return GLOBAL_CLIENT;
    }
    synchronized (Client.class) {
      if (GLOBAL_CLIENT != null) {
        return GLOBAL_CLIENT;
      }
      return connect();
    }
  }

  public static void setGlobalClient(Client client) {
    GLOBAL_CLIENT = client;
  }

  /**
   * Connect to an existing Serve instance on this Ray cluster.
   * <p>If calling from the driver program, the Serve instance on this Ray cluster must first have
   * been initialized using `serve.start(detached=True)`.
   * <p>If called from within a replica, this will connect to the same Serve instance that the
   * replica is running in.
   *
   * @return
   */
  public static Client connect() {

    // Initialize ray if needed.
    if (!Ray.isInitialized()) {
      System.setProperty("ray.job.namespace", "serve");
      Ray.init();
    }

    String controllerName = null;
    String controllerNamespace = null;

    // When running inside of a replica, _INTERNAL_REPLICA_CONTEXT is set to ensure that the correct
    // instance is connected to.
    if (INTERNAL_REPLICA_CONTEXT == null) {
      controllerName = Constants.SERVE_CONTROLLER_NAME;
      controllerNamespace = getControllerNamespace(true);
    } else {
      controllerName = INTERNAL_REPLICA_CONTEXT.getInternalControllerName();
      controllerNamespace = INTERNAL_REPLICA_CONTEXT.getInternalControllerNamespace();
    }

    // Try to get serve controller if it exists
    Optional<BaseActorHandle> controller = Ray.getActor(controllerName, controllerNamespace);
    if (!controller.isPresent()) {
      throw new RayServeException(
          LogUtil.format(
              "There is no instance running on this Ray cluster. "
                  + "Please call `Serve.start(...) to start one."));
    }

    Client client = new Client(controller.get(), controllerName, true);
    setGlobalClient(client);
    return client;
  }

  /**
   * Dynamically fetch a handle to a Deployment object.
   *
   * <p>This can be used to update and redeploy a deployment without access to the original
   * definition.
   *
   * @param name name of the deployment. This must have already been deployed.
   * @return Deployment
   */
  public static Deployment getDeployment(String name) {
    DeploymentInfo deploymentInfo = getGlobalClient().getDeploymentInfo(name);
    if (deploymentInfo == null) {
      throw new RayServeException(
          LogUtil.format(
              "Deployment {} was not found. Did you call Deployment.deploy(...)?", name));
    }

    return new Deployment(
        deploymentInfo.getDeploymentDef(),
        name,
        deploymentInfo.getDeploymentConfig(),
        deploymentInfo.getDeploymentVersion().getCodeVersion(), // TODO version
        null,
        deploymentInfo.getReplicaConfig().getInitArgs(),
        null, // TODO route
        deploymentInfo.getReplicaConfig().getRayActorOptions());
  }

  /**
   * Returns a dictionary of all active deployments.
   *
   * <p>Dictionary maps deployment name to Deployment objects.
   *
   * @return
   */
  public static Map<String, Deployment> listDeployments() {
    Map<String, DeploymentInfo> infos = getGlobalClient().listDeployments();
    Map<String, Deployment> deployments = new HashMap<>();
    if (infos == null || infos.size() == 0) {
      return deployments;
    }
    for (Map.Entry<String, DeploymentInfo> entry : infos.entrySet()) {
      deployments.put(
          entry.getKey(),
          new Deployment(
              entry.getValue().getDeploymentDef(),
              entry.getKey(),
              entry.getValue().getDeploymentConfig(),
              entry.getValue().getDeploymentVersion().getCodeVersion(), // TODO version
              null,
              entry.getValue().getReplicaConfig().getInitArgs(),
              null, // TODO route
              entry.getValue().getReplicaConfig().getRayActorOptions()));
    }
    return deployments;
  }
}
