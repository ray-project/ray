package io.ray.serve.api;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.ray.api.ActorHandle;
import io.ray.api.BaseActorHandle;
import io.ray.api.PyActorHandle;
import io.ray.api.Ray;
import io.ray.api.function.PyActorClass;
import io.ray.api.function.PyActorMethod;
import io.ray.api.options.ActorLifetime;
import io.ray.runtime.exception.RayActorException;
import io.ray.runtime.exception.RayTimeoutException;
import io.ray.serve.common.Constants;
import io.ray.serve.context.ReplicaContext;
import io.ray.serve.deployment.Deployment;
import io.ray.serve.deployment.DeploymentCreator;
import io.ray.serve.deployment.DeploymentRoute;
import io.ray.serve.exception.RayServeException;
import io.ray.serve.generated.ActorNameList;
import io.ray.serve.proxy.ProxyActor;
import io.ray.serve.util.CollectionUtil;
import io.ray.serve.util.CommonUtil;
import io.ray.serve.util.LogUtil;
import io.ray.serve.util.ServeProtoUtil;

/** Ray Serve global API. TODO: will be riched in the Java SDK/API PR. */
public class Serve {

  private static final Logger LOGGER = LoggerFactory.getLogger(Serve.class);

  private static ReplicaContext INTERNAL_REPLICA_CONTEXT;

  private static ServeControllerClient GLOBAL_CLIENT;

  private static Pattern ANONYMOUS_NAMESPACE_PATTERN =
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
   * @param overrideControllerNamespace
   * @param config Configuration options for Serve.
   * @return
   */
  @SuppressWarnings("unchecked")
  public static synchronized ServeControllerClient start(
      boolean detached,
      boolean dedicatedCpu,
      String checkpointPath,
      String overrideControllerNamespace,
      Map<String, String> config) {

    // Initialize ray if needed.
    if (!Ray.isInitialized()) {
      System.setProperty("ray.job.namespace", "serve");
      Ray.init();
    }

    String controllerNamespace = getControllerNamespace(detached, overrideControllerNamespace);

    try {
      ServeControllerClient client = getGlobalClient(overrideControllerNamespace, true);
      LOGGER.info("Connecting to existing Serve instance in namespace {}", controllerNamespace);
      checkCheckpointPath(client, checkpointPath);
      return client;
    } catch (RayServeException e) {
      LOGGER.info("There is no instance running on this Ray cluster. A new one will be started.");
    }

    String controllerName =
        detached
            ? Constants.SERVE_CONTROLLER_NAME
            : CommonUtil.formatActorName(
                Constants.SERVE_CONTROLLER_NAME, RandomStringUtils.randomAlphabetic(6));

    // TODO The namespace, max_task_retries and dispatching on head node is not supported in Java
    // now.
    PyActorHandle controller =
        Ray.actor(
                PyActorClass.of("ray.serve.controller", "ServeController"),
                controllerName,
                null, // http_config TODO change it nullable or define protobuf.
                checkpointPath,
                detached,
                overrideControllerNamespace)
            .setResource("CPU", dedicatedCpu ? 1.0 : 0.0)
            .setName(controllerName)
            .setLifetime(detached ? ActorLifetime.DETACHED : ActorLifetime.NON_DETACHED)
            .setMaxRestarts(-1)
            .setMaxConcurrency(Constants.CONTROLLER_MAX_CONCURRENCY)
            .remote();

    ActorNameList actorNameList =
        ServeProtoUtil.bytesToProto(
            (byte[]) controller.task(PyActorMethod.of("get_http_proxy_names")).remote().get(),
            bytes -> ActorNameList.parseFrom(bytes));
    if (actorNameList != null && !CollectionUtil.isEmpty(actorNameList.getNamesList())) {
      try {
        for (String name : actorNameList.getNamesList()) {
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

    ServeControllerClient client =
        new ServeControllerClient(
            controller, controllerName, detached, overrideControllerNamespace);
    setGlobalClient(client);
    LOGGER.info(
        "Started{}Serve instance in namespace {}",
        detached ? " detached " : " ",
        controllerNamespace);
    return client;
  }
  
  private static void checkCheckpointPath(ServeControllerClient client, String checkpointPath) {
    if (StringUtils.isNotBlank(checkpointPath)
        && !StringUtils.equals(checkpointPath, client.getCheckpointPath())) {
      LOGGER.warn(
          "The new client checkpoint path '{}' is different from the existing one '{}'. The new checkpoint path is ignored.",
          checkpointPath,
          client.getCheckpointPath());
    }
  }

  /**
   * Gets the controller's namespace.
   *
   * @param detached Whether serve.start() was called with detached=True
   * @param overrideControllerNamespace When set, this is the controller's namespace
   * @return
   */
  public static String getControllerNamespace(
      boolean detached, String overrideControllerNamespace) {

    if (StringUtils.isNotBlank(overrideControllerNamespace)) {
      return overrideControllerNamespace;
    }
    
    String controllerNamespace = Ray.getRuntimeContext().getNamespace();

    if (!detached) {
      return controllerNamespace;
    }

    // Start controller in "serve" namespace if detached and currently in anonymous namespace.
    if (ANONYMOUS_NAMESPACE_PATTERN.matcher(controllerNamespace).matches()) {
      controllerNamespace = "serve";
    }
    return controllerNamespace;
  }

  /**
   * Completely shut down the connected Serve instance.
   *
   * <p>Shuts down all processes and deletes all state associated with the instance.
   */
  public static void shutdown() {

    ServeControllerClient client = null;
    try {
      client = getGlobalClient();
    } catch (RayServeException e) {
      LOGGER.info(
          "Nothing to shut down. There's no Serve application running on this Ray cluster.");
      return;
    }

    client.shutdown();
    setGlobalClient(null);
  }

  /**
   * Define a Serve deployment.
   *
   * @return DeploymentCreator
   */
  public static DeploymentCreator deployment() {
    return new DeploymentCreator();
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
   * If called from a deployment, returns the deployment and replica tag.
   *
   * <p>A replica tag uniquely identifies a single replica for a Ray Serve deployment at runtime.
   * Replica tags are of the form `<deployment_name>#<random letters>`.
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

  /**
   * Gets the global client, which stores the controller's handle.
   *
   * @param overrideControllerNamespace If None and there's no cached client, searches for the
   *     controller in this namespace.
   * @param healthCheckController If True, run a health check on the cached controller if it exists.
   *     If the check fails, try reconnecting to the controller.
   * @return
   */
  public static ServeControllerClient getGlobalClient(
      String overrideControllerNamespace, boolean healthCheckController) {
    try {
      if (GLOBAL_CLIENT != null) {
        if (healthCheckController) {
          // TODO _controller.check_alive
        }
        return GLOBAL_CLIENT;
      }
    } catch (RayActorException e) {
      LOGGER.info("The cached controller has died. Reconnecting.");
      setGlobalClient(null);
    }
    synchronized (ServeControllerClient.class) {
      if (GLOBAL_CLIENT != null) {
        return GLOBAL_CLIENT;
      }
      return connect(
          overrideControllerNamespace); // TODO throw RayServeException if there is no Serve
                                        // controller actor in the expected namespace.
    }
  }

  public static ServeControllerClient getGlobalClient() {
    return getGlobalClient(null, false);
  }

  public static void setGlobalClient(ServeControllerClient client) {
    GLOBAL_CLIENT = client;
  }

  /**
   * Connect to an existing Serve instance on this Ray cluster.
   *
   * <p>If calling from the driver program, the Serve instance on this Ray cluster must first have
   * been initialized using `serve.start(detached=True)`.
   *
   * <p>If called from within a replica, this will connect to the same Serve instance that the
   * replica is running in.
   *
   * @param overrideControllerNamespace The namespace to use when looking for the controller. If
   *     None, Serve recalculates the controller's namespace using get_controller_namespace().
   * @return
   */
  public static ServeControllerClient connect(String overrideControllerNamespace) {

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
      controllerNamespace = getControllerNamespace(true, overrideControllerNamespace);
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
                  + "Please call `Serve.start(...) to start one.")); // TODO change
                                                                     // RayServeException to checked
                                                                     // exception?
    }

    ServeControllerClient client = new ServeControllerClient(controller.get(), controllerName, true, overrideControllerNamespace);
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
    DeploymentRoute deploymentRoute = getGlobalClient().getDeploymentInfo(name);
    if (deploymentRoute == null) {
      throw new RayServeException(
          LogUtil.format("Deployment {} was not found. Did you call Deployment.deploy?", name));
    }

    // TODO use DeploymentCreator
    return new Deployment(
        deploymentRoute.getDeploymentInfo().getDeploymentDef(),
        name,
        deploymentRoute.getDeploymentInfo().getDeploymentConfig(),
        deploymentRoute.getDeploymentInfo().getVersion(),
        null,
        deploymentRoute.getDeploymentInfo().getReplicaConfig().getInitArgs(),
        deploymentRoute.getRoute(),
        deploymentRoute.getDeploymentInfo().getReplicaConfig().getRayActorOptions());
  }

  /**
   * Returns a dictionary of all active deployments.
   *
   * <p>Dictionary maps deployment name to Deployment objects.
   *
   * @return
   */
  public static Map<String, Deployment> listDeployments() {
    Map<String, DeploymentRoute> infos = getGlobalClient().listDeployments();
    if (infos == null || infos.size() == 0) {
      return Collections.emptyMap();
    }
    Map<String, Deployment> deployments = new HashMap<>(infos.size());
    for (Map.Entry<String, DeploymentRoute> entry : infos.entrySet()) {
      deployments.put(
          entry.getKey(),
          new Deployment(
              entry.getValue().getDeploymentInfo().getDeploymentDef(),
              entry.getKey(),
              entry.getValue().getDeploymentInfo().getDeploymentConfig(),
              entry.getValue().getDeploymentInfo().getVersion(),
              null,
              entry.getValue().getDeploymentInfo().getReplicaConfig().getInitArgs(),
              entry.getValue().getRoute(),
              entry.getValue().getDeploymentInfo().getReplicaConfig().getRayActorOptions()));
    }
    return deployments;
  }
}
