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
import io.ray.serve.dag.Graph;
import io.ray.serve.deployment.Application;
import io.ray.serve.deployment.Deployment;
import io.ray.serve.deployment.DeploymentCreator;
import io.ray.serve.deployment.DeploymentRoute;
import io.ray.serve.exception.RayServeException;
import io.ray.serve.generated.ActorNameList;
import io.ray.serve.handle.DeploymentHandle;
import io.ray.serve.poll.LongPollClientFactory;
import io.ray.serve.replica.ReplicaContext;
import io.ray.serve.util.CollectionUtil;
import io.ray.serve.util.MessageFormatter;
import io.ray.serve.util.ServeProtoUtil;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
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
   * @param config Configuration options for Serve.
   * @return
   */
  public static synchronized ServeControllerClient start(Map<String, String> config) {
    return serveStart(config);
  }

  private static synchronized ServeControllerClient serveStart(Map<String, String> config) {

    try {
      ServeControllerClient client = getGlobalClient(true);
      LOGGER.info("Connecting to existing Serve app in namespace {}", Constants.SERVE_NAMESPACE);
      return client;
    } catch (RayServeException | IllegalStateException e) {
      LOGGER.info(
          "There is no Serve instance running on this Ray cluster. A new one will be started.");
    }

    // Initialize ray if needed.
    if (!Ray.isInitialized()) {
      init();
    }

    int httpPort =
        Optional.ofNullable(config)
            .map(m -> m.get(RayServeConfig.PROXY_HTTP_PORT))
            .map(Integer::parseInt)
            .orElse(Integer.valueOf(System.getProperty(RayServeConfig.PROXY_HTTP_PORT, "8000")));
    PyActorHandle controllerAvatar =
        Ray.actor(
                PyActorClass.of("ray.serve._private.controller", "ServeControllerAvatar"), httpPort)
            .setName(Constants.SERVE_CONTROLLER_NAME + "_AVATAR")
            .setLifetime(ActorLifetime.DETACHED)
            .setMaxRestarts(-1)
            .setMaxConcurrency(1)
            .remote();

    controllerAvatar.task(PyActorMethod.of("check_alive")).remote().get();

    PyActorHandle controller =
        (PyActorHandle)
            Ray.getActor(Constants.SERVE_CONTROLLER_NAME, Constants.SERVE_NAMESPACE).get();

    ActorNameList actorNameList =
        ServeProtoUtil.bytesToProto(
            (byte[]) controller.task(PyActorMethod.of("get_proxy_names")).remote().get(),
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
            MessageFormatter.format(
                "HTTP proxies not available after {}s.", Constants.PROXY_TIMEOUT_S);
        LOGGER.error(errMsg, e);
        throw new RayServeException(errMsg, e);
      }
    }

    ServeControllerClient client = new ServeControllerClient(controller);
    setGlobalClient(client);
    LOGGER.info("Started Serve in namespace {}", Constants.SERVE_NAMESPACE);
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
    client.shutdown(null);
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
   * @param servableObject the servable object of the specified replica.
   * @param config
   */
  public static void setInternalReplicaContext(
      String deploymentName,
      String replicaTag,
      Object servableObject,
      Map<String, String> config,
      String appName) {
    INTERNAL_REPLICA_CONTEXT =
        new ReplicaContext(deploymentName, replicaTag, servableObject, config, appName);
  }

  /**
   * Set replica information to global context.
   *
   * @param replicaContext
   */
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
   * @return ServeControllerClient to the running Serve controller. If there is no running
   *     controller and raise_if_no_controller_running is set to False, returns None.
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
    return connect();
  }

  /**
   * Gets the global client, which stores the controller's handle.
   *
   * @return ServeControllerClient to the running Serve controller. If there is no running
   *     controller and raise_if_no_controller_running is set to False, returns None.
   */
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
  private static synchronized ServeControllerClient connect() {

    if (GLOBAL_CLIENT != null) {
      return GLOBAL_CLIENT;
    }

    // Initialize ray if needed.
    if (!Ray.isInitialized()) {
      init();
    }

    Optional<BaseActorHandle> optional =
        Ray.getActor(Constants.SERVE_CONTROLLER_NAME, Constants.SERVE_NAMESPACE);
    Preconditions.checkState(
        optional.isPresent(),
        MessageFormatter.format(
            "There is no instance running on this Ray cluster. "
                + "Please call `serve.start() to start one."));
    LOGGER.info(
        "Got controller handle with name `{}` in namespace `{}`.",
        Constants.SERVE_CONTROLLER_NAME,
        Constants.SERVE_NAMESPACE);

    ServeControllerClient client = new ServeControllerClient(optional.get());

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
   * @deprecated {@value Constants#MIGRATION_MESSAGE}
   */
  @Deprecated
  public static Deployment getDeployment(String name) {
    LOGGER.warn(Constants.MIGRATION_MESSAGE);
    DeploymentRoute deploymentRoute = getGlobalClient().getDeploymentInfo(name);
    if (deploymentRoute == null) {
      throw new RayServeException(
          MessageFormatter.format(
              "Deployment {} was not found. Did you call Deployment.deploy?", name));
    }

    // TODO use DeploymentCreator
    return new Deployment(
        name,
        deploymentRoute.getDeploymentInfo().getDeploymentConfig(),
        deploymentRoute.getDeploymentInfo().getReplicaConfig(),
        deploymentRoute.getDeploymentInfo().getVersion(),
        deploymentRoute.getRoute());
  }

  /**
   * Run an application and return a handle to its ingress deployment.
   *
   * @param target A Serve application returned by `Deployment.bind()`.
   * @return A handle that can be used to call the application.
   */
  public static Optional<DeploymentHandle> run(Application target) {
    return run(target, true, Constants.SERVE_DEFAULT_APP_NAME, null, null);
  }

  /**
   * Run an application and return a handle to its ingress deployment.
   *
   * @param target A Serve application returned by `Deployment.bind()`.
   * @param blocking
   * @param name Application name. If not provided, this will be the only application running on the
   *     cluster (it will delete all others).
   * @param routePrefix Route prefix for HTTP requests. If not provided, it will use route_prefix of
   *     the ingress deployment. If specified neither as an argument nor in the ingress deployment,
   *     the route prefix will default to '/'.
   * @param config
   * @return A handle that can be used to call the application.
   */
  public static Optional<DeploymentHandle> run(
      Application target,
      boolean blocking,
      String name,
      String routePrefix,
      Map<String, String> config) {

    if (StringUtils.isBlank(name)) {
      throw new RayServeException("Application name must a non-empty string.");
    }

    ServeControllerClient client = serveStart(config);

    List<Deployment> deployments = Graph.build(target.getInternalDagNode(), name);
    Deployment ingress = Graph.getAndValidateIngressDeployment(deployments);

    for (Deployment deployment : deployments) {
      // Overwrite route prefix
      if (StringUtils.isNotBlank(deployment.getRoutePrefix())
          && StringUtils.isNotBlank(routePrefix)) {
        Preconditions.checkArgument(
            routePrefix.startsWith("/"), "The route_prefix must start with a forward slash ('/')");
        deployment.setRoutePrefix(routePrefix);
      }
      deployment
          .getDeploymentConfig()
          .setVersion(
              StringUtils.isNotBlank(deployment.getVersion())
                  ? deployment.getVersion()
                  : RandomStringUtils.randomAlphabetic(6));
    }

    client.deployApplication(name, deployments, blocking);

    return Optional.ofNullable(ingress)
        .map(
            ingressDeployment ->
                client.getDeploymentHandle(ingressDeployment.getName(), name, true));
  }

  private static void init() {
    System.setProperty("ray.job.namespace", Constants.SERVE_NAMESPACE);
    Ray.init();
  }

  /**
   * Get a handle to the application's ingress deployment by name.
   *
   * @param name application name
   * @return
   */
  public static DeploymentHandle getAppHandle(String name) {
    ServeControllerClient client = getGlobalClient();
    String ingress =
        (String)
            ((PyActorHandle) client.getController())
                .task(PyActorMethod.of("get_ingress_deployment_name"), name)
                .remote()
                .get();

    if (StringUtils.isBlank(ingress)) {
      throw new RayServeException(
          MessageFormatter.format("Application '{}' does not exist.", ingress));
    }
    return client.getDeploymentHandle(ingress, name, false);
  }

  /**
   * Delete an application by its name.
   *
   * <p>Deletes the app with all corresponding deployments.
   *
   * @param name application name
   */
  public static void delete(String name) {
    delete(name, true);
  }

  /**
   * Delete an application by its name.
   *
   * <p>Deletes the app with all corresponding deployments.
   *
   * @param name application name
   * @param blocking Wait for the application to be deleted or not.
   */
  public static void delete(String name, boolean blocking) {
    getGlobalClient().deleteApps(Arrays.asList(name), blocking);
  }
}
