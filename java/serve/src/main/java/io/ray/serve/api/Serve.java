package io.ray.serve.api;

import com.google.common.base.Preconditions;
import io.ray.api.BaseActorHandle;
import io.ray.api.PyActorHandle;
import io.ray.api.Ray;
import io.ray.api.exception.RayActorException;
import io.ray.api.exception.RayTimeoutException;
import io.ray.api.function.PyActorClass;
import io.ray.api.function.PyActorMethod;
import io.ray.api.options.ActorLifetime;
import io.ray.serve.common.Constants;
import io.ray.serve.config.RayServeConfig;
import io.ray.serve.deployment.Deployment;
import io.ray.serve.deployment.DeploymentCreator;
import io.ray.serve.deployment.DeploymentRoute;
import io.ray.serve.exception.RayServeException;
import io.ray.serve.generated.ActorNameList;
import io.ray.serve.poll.LongPollClientFactory;
import io.ray.serve.replica.ReplicaContext;
import io.ray.serve.util.CollectionUtil;
import io.ray.serve.util.CommonUtil;
import io.ray.serve.util.LogUtil;
import io.ray.serve.util.ServeProtoUtil;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Ray Serve global API. TODO: will be riched in the Java SDK/API PR. */
public class Serve {
  private static final Logger LOGGER = LoggerFactory.getLogger(Serve.class);

  private static ReplicaContext INTERNAL_REPLICA_CONTEXT;

  private static ServeControllerClient GLOBAL_CLIENT;

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
   * @param config Configuration options for Serve.
   * @return
   */
  public static synchronized ServeControllerClient start(
      boolean detached, boolean dedicatedCpu, Map<String, String> config) {
    // Initialize ray if needed.
    if (!Ray.isInitialized()) {
      System.setProperty("ray.job.namespace", Constants.SERVE_NAMESPACE);
      Ray.init();
    }

    try {
      ServeControllerClient client = getGlobalClient(true);
      LOGGER.info("Connecting to existing Serve app in namespace {}", Constants.SERVE_NAMESPACE);
      return client;
    } catch (RayServeException | IllegalStateException e) {
      LOGGER.info("There is no instance running on this Ray cluster. A new one will be started.");
    }

    String controllerName =
        detached
            ? Constants.SERVE_CONTROLLER_NAME
            : CommonUtil.formatActorName(
                Constants.SERVE_CONTROLLER_NAME, RandomStringUtils.randomAlphabetic(6));

    int httpPort =
        Optional.ofNullable(config)
            .map(m -> m.get(RayServeConfig.PROXY_HTTP_PORT))
            .map(Integer::parseInt)
            .orElse(8000);
    PyActorHandle controllerAvatar =
        Ray.actor(
                PyActorClass.of("ray.serve.controller", "ServeControllerAvatar"),
                controllerName,
                detached,
                dedicatedCpu,
                httpPort)
            .setName(controllerName + "_AVATAR")
            .setLifetime(detached ? ActorLifetime.DETACHED : ActorLifetime.NON_DETACHED)
            .setMaxRestarts(-1)
            .setMaxConcurrency(1)
            .remote();

    controllerAvatar.task(PyActorMethod.of("check_alive")).remote().get();

    PyActorHandle controller =
        (PyActorHandle) Ray.getActor(controllerName, Constants.SERVE_NAMESPACE).get();

    ActorNameList actorNameList =
        ServeProtoUtil.bytesToProto(
            (byte[]) controller.task(PyActorMethod.of("get_http_proxy_names")).remote().get(),
            ActorNameList::parseFrom);
    if (actorNameList != null && !CollectionUtil.isEmpty(actorNameList.getNamesList())) {
      try {
        for (String name : actorNameList.getNamesList()) {
          PyActorHandle proxyActorHandle =
              (PyActorHandle) Ray.getActor(name, Constants.SERVE_NAMESPACE).get();
          proxyActorHandle
              .task(PyActorMethod.of("ready"))
              .remote()
              .get(Constants.PROXY_TIMEOUT_S * 1000);
        }
      } catch (RayTimeoutException e) {
        String errMsg =
            LogUtil.format("Proxies not available after {}s.", Constants.PROXY_TIMEOUT_S);
        LOGGER.error(errMsg, e);
        throw new RayServeException(errMsg, e);
      }
    }

    ServeControllerClient client = new ServeControllerClient(controller, controllerName, detached);
    setGlobalClient(client);
    LOGGER.info(
        "Started{}Serve instance in namespace {}",
        detached ? " detached " : " ",
        Constants.SERVE_NAMESPACE);
    return client;
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
    } catch (RayServeException | IllegalStateException e) {
      LOGGER.info(
          "Nothing to shut down. There's no Serve application running on this Ray cluster.");
      return;
    }

    LongPollClientFactory.stop();
    client.shutdown();
    clearContext();
  }

  public static void clearContext() {
    setGlobalClient(null);
    setInternalReplicaContext(null);
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
   * @param servableObject the servable object of the specified replica.
   * @param config
   */
  public static void setInternalReplicaContext(
      String deploymentName,
      String replicaTag,
      String controllerName,
      Object servableObject,
      Map<String, String> config) {
    INTERNAL_REPLICA_CONTEXT =
        new ReplicaContext(deploymentName, replicaTag, controllerName, servableObject, config);
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
   * @param healthCheckController If True, run a health check on the cached controller if it exists.
   *     If the check fails, try reconnecting to the controller.
   * @return
   */
  public static ServeControllerClient getGlobalClient(boolean healthCheckController) {
    try {
      if (GLOBAL_CLIENT != null) {
        if (healthCheckController) {
          ((PyActorHandle) GLOBAL_CLIENT.getController())
              .task(PyActorMethod.of("check_alive"))
              .remote();
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
      return connect();
    }
  }

  public static ServeControllerClient getGlobalClient() {
    return getGlobalClient(false);
  }

  private static void setGlobalClient(ServeControllerClient client) {
    GLOBAL_CLIENT = client;
  }

  /**
   * Connect to an existing Serve instance on this Ray cluster.
   *
   * <p>If calling from the driver program, the Serve instance on this Ray cluster must first have
   * been initialized using `Serve.start`.
   *
   * <p>If called from within a replica, this will connect to the same Serve instance that the
   * replica is running in.
   *
   * @return
   */
  public static ServeControllerClient connect() {
    // Initialize ray if needed.
    if (!Ray.isInitialized()) {
      System.setProperty("ray.job.namespace", Constants.SERVE_NAMESPACE);
      Ray.init();
    }

    String controllerName =
        INTERNAL_REPLICA_CONTEXT != null
            ? INTERNAL_REPLICA_CONTEXT.getInternalControllerName()
            : Constants.SERVE_CONTROLLER_NAME;

    Optional<BaseActorHandle> optional = Ray.getActor(controllerName, Constants.SERVE_NAMESPACE);
    Preconditions.checkState(
        optional.isPresent(),
        LogUtil.format(
            "There is no instance running on this Ray cluster. "
                + "Please call `serve.start(detached=True) to start one."));
    LOGGER.info(
        "Got controller handle with name `{}` in namespace `{}`.",
        controllerName,
        Constants.SERVE_NAMESPACE);

    ServeControllerClient client = new ServeControllerClient(optional.get(), controllerName, true);

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
        deploymentRoute.getDeploymentInfo().getReplicaConfig().getDeploymentDef(),
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
              entry.getValue().getDeploymentInfo().getReplicaConfig().getDeploymentDef(),
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
