package io.ray.serve.api;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import io.ray.api.ActorHandle;
import io.ray.api.BaseActorHandle;
import io.ray.api.PyActorHandle;
import io.ray.api.Ray;
import io.ray.api.exception.RayActorException;
import io.ray.api.exception.RayTimeoutException;
import io.ray.api.function.PyActorMethod;
import io.ray.serve.common.Constants;
import io.ray.serve.config.DeploymentConfig;
import io.ray.serve.config.ReplicaConfig;
import io.ray.serve.controller.ServeController;
import io.ray.serve.deployment.Deployment;
import io.ray.serve.deployment.DeploymentRoute;
import io.ray.serve.exception.RayServeException;
import io.ray.serve.generated.ApplicationStatus;
import io.ray.serve.generated.DeploymentArgs;
import io.ray.serve.generated.DeploymentArgsList;
import io.ray.serve.generated.DeploymentRouteList;
import io.ray.serve.generated.DeploymentStatus;
import io.ray.serve.generated.DeploymentStatusInfo;
import io.ray.serve.generated.EndpointInfo;
import io.ray.serve.generated.ListApplicationsResponse;
import io.ray.serve.generated.StatusOverview;
import io.ray.serve.handle.DeploymentHandle;
import io.ray.serve.util.CollectionUtil;
import io.ray.serve.util.MessageFormatter;
import io.ray.serve.util.ServeProtoUtil;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServeControllerClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServeControllerClient.class);

  private static long CLIENT_POLLING_INTERVAL_S = 1;

  private BaseActorHandle controller; // TODO change to PyActorHandle

  private String controllerName;

  private boolean shutdown;

  private Map<String, DeploymentHandle> handleCache = new ConcurrentHashMap<>();

  private String rootUrl;

  @SuppressWarnings("unchecked")
  public ServeControllerClient(BaseActorHandle controller, String controllerName) {
    this.controller = controller;
    this.controllerName = controllerName;
    this.rootUrl =
        controller instanceof PyActorHandle
            ? (String)
                ((PyActorHandle) controller).task(PyActorMethod.of("get_root_url")).remote().get()
            : ((ActorHandle<ServeController>) controller)
                .task(ServeController::getRootUrl)
                .remote()
                .get();
  }

  public DeploymentHandle getHandle(String deploymentName) {
    return getHandle(deploymentName, Constants.SERVE_DEFAULT_APP_NAME, false);
  }

  /**
   * Retrieve DeploymentHandle for service deployment to invoke it from Java.
   *
   * @param deploymentName A registered service deployment.
   * @param appName application name
   * @param missingOk If true, then Serve won't check the deployment is registered.
   * @return
   */
  @SuppressWarnings("unchecked")
  public DeploymentHandle getHandle(String deploymentName, String appName, boolean missingOk) {
    String cacheKey = StringUtils.join(new Object[] {deploymentName, appName, missingOk}, "#");
    if (handleCache.containsKey(cacheKey)) {
      return handleCache.get(cacheKey);
    }

    Map<String, EndpointInfo> endpoints = null;
    if (controller instanceof PyActorHandle) {
      endpoints =
          ServeProtoUtil.parseEndpointSet(
              (byte[])
                  ((PyActorHandle) controller)
                      .task(PyActorMethod.of(Constants.CONTROLLER_GET_ALL_ENDPOINTS_METHOD))
                      .remote()
                      .get());
    } else {
      LOGGER.warn("Client currently only supports the Python controller.");
      endpoints =
          ServeProtoUtil.parseEndpointSet(
              ((ActorHandle<? extends ServeController>) controller)
                  .task(ServeController::getAllEndpoints)
                  .remote()
                  .get());
    }

    if (!missingOk && (endpoints == null || !endpoints.containsKey(deploymentName))) {
      throw new RayServeException(
          MessageFormatter.format("Deployment {} does not exist.", deploymentName));
    }

    DeploymentHandle handle = new DeploymentHandle(deploymentName, appName, null, null);
    handleCache.put(cacheKey, handle);
    return handle;
  }

  public void deploy(
      String name,
      ReplicaConfig replicaConfig,
      DeploymentConfig deploymentConfig,
      String version,
      String routePrefix,
      String url,
      Boolean blocking) {

    if (deploymentConfig == null) {
      deploymentConfig = new DeploymentConfig();
    }

    deploymentConfig.setVersion(version);

    if (deploymentConfig.getAutoscalingConfig() != null
        && deploymentConfig.getMaxConcurrentQueries()
            < deploymentConfig.getAutoscalingConfig().getTargetNumOngoingRequestsPerReplica()) {
      LOGGER.warn(
          "Autoscaling will never happen, because 'max_concurrent_queries' is less than 'target_num_ongoing_requests_per_replica'.");
    }

    boolean updating =
        (boolean)
            ((PyActorHandle) controller)
                .task(
                    PyActorMethod.of("deploy"),
                    name,
                    deploymentConfig.toProtoBytes(),
                    replicaConfig.toProtoBytes(),
                    routePrefix,
                    Ray.getRuntimeContext().getCurrentJobId().getBytes())
                .remote()
                .get();

    String tag = "component=serve deployment=" + name;
    if (updating) {
      String msg = MessageFormatter.format("Updating deployment '{}'", name);
      if (StringUtils.isNotBlank(version)) {
        msg += MessageFormatter.format(" to version '{}'", version);
      }
      LOGGER.info("{}. {}", msg, tag);
    } else {
      LOGGER.info(
          "Deployment '{}' is already at version '{}', not updating. {}", name, version, tag);
    }

    if (blocking) {
      waitForDeploymentHealthy(name);
      String urlPart = url != null ? MessageFormatter.format(" at `{}`", url) : "";
      LOGGER.info(
          "Deployment '{}{}' is ready {}. {}",
          name,
          StringUtils.isNotBlank(version) ? "':'" + version : "",
          urlPart,
          tag);
    }
  }

  /**
   * Waits for the named deployment to enter "HEALTHY" status.
   *
   * <p>Raises RayServeException if the deployment enters the "UNHEALTHY" status instead or this
   * doesn't happen before timeoutS.
   *
   * @param name
   * @param timeoutS
   */
  private void waitForDeploymentHealthy(String name, Long timeoutS) {
    long start = System.currentTimeMillis();
    boolean isTimeout = true;
    while (timeoutS == null || System.currentTimeMillis() - start < timeoutS * 1000) {
      DeploymentStatusInfo status = getDeploymentStatus(name);
      if (status == null) {
        throw new RayServeException(
            MessageFormatter.format(
                "Waiting for deployment {} to be HEALTHY, but deployment doesn't exist.", name));
      }

      if (status.getStatus() == DeploymentStatus.DEPLOYMENT_STATUS_HEALTHY) {
        isTimeout = false;
        break;
      } else if (status.getStatus() == DeploymentStatus.DEPLOYMENT_STATUS_UNHEALTHY) {
        throw new RayServeException(
            MessageFormatter.format("Deployment {} is UNHEALTHY: {}", name, status.getMessage()));
      } else {
        Preconditions.checkState(status.getStatus() == DeploymentStatus.DEPLOYMENT_STATUS_UPDATING);
      }

      LOGGER.debug("Waiting for {} to be healthy, current status: {}.", name, status.getStatus());
      try {
        Thread.sleep(CLIENT_POLLING_INTERVAL_S * 1000);
      } catch (InterruptedException e) {
      }
    }
    if (isTimeout) {
      throw new RayServeException(
          MessageFormatter.format(
              "Deployment {} did not become HEALTHY after {}s.", name, timeoutS));
    }
  }

  private void waitForDeploymentHealthy(String name) {
    waitForDeploymentHealthy(name, null);
  }

  /**
   * Completely shut down the connected Serve instance.
   *
   * <p>Shuts down all processes and deletes all state associated with the instance.
   */
  public synchronized void shutdown() {
    if (Ray.isInitialized() && !shutdown) {

      try {
        ((PyActorHandle) controller)
            .task(PyActorMethod.of("graceful_shutdown"))
            .remote()
            .get(30 * 1000);
      } catch (RayActorException e) {
        // Controller has been shut down.
        return;
      } catch (RayTimeoutException e) {
        LOGGER.warn(
            "Controller failed to shut down within 30s. Check controller logs for more details.");
      }
      shutdown = true;
    }
  }

  public String getRootUrl() {
    return rootUrl;
  }

  public DeploymentRoute getDeploymentInfo(String name) {
    return DeploymentRoute.fromProtoBytes(
        (byte[])
            ((PyActorHandle) controller)
                .task(PyActorMethod.of("get_deployment_info"), name)
                .remote()
                .get());
  }

  public Map<String, DeploymentRoute> listDeployments() {
    DeploymentRouteList deploymentRouteList =
        ServeProtoUtil.bytesToProto(
            (byte[])
                ((PyActorHandle) controller)
                    .task(PyActorMethod.of("list_deployments_v1"))
                    .remote()
                    .get(),
            DeploymentRouteList::parseFrom);

    if (deploymentRouteList == null || deploymentRouteList.getDeploymentRoutesList() == null) {
      return Collections.emptyMap();
    }

    Map<String, DeploymentRoute> deploymentRoutes =
        new HashMap<>(deploymentRouteList.getDeploymentRoutesList().size());
    for (io.ray.serve.generated.DeploymentRoute deploymentRoute :
        deploymentRouteList.getDeploymentRoutesList()) {
      deploymentRoutes.put(
          deploymentRoute.getDeploymentInfo().getName(),
          DeploymentRoute.fromProto(deploymentRoute));
    }
    return deploymentRoutes;
  }

  public void deleteDeployment(String name, boolean blocking) { // TODO update to deleteDeployments
    ((PyActorHandle) controller).task(PyActorMethod.of("delete_deployment")).remote();
    if (blocking) {
      waitForDeploymentDeleted(name, 60);
    }
  }

  /**
   * Waits for the named deployment to be shut down and deleted.
   *
   * <p>Throw RayServeException if this doesn't happen before timeoutS. // TODO change to
   * TimeoutException
   *
   * @param name
   * @param timeoutS
   */
  private void waitForDeploymentDeleted(String name, long timeoutS) {
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < timeoutS * 1000) {
      DeploymentStatusInfo status = getDeploymentStatus(name);
      if (status == null) {
        break;
      }
      LOGGER.debug("Waiting for {} to be deleted, current status: {}.", name, status);
      try {
        Thread.sleep(CLIENT_POLLING_INTERVAL_S * 1000);
      } catch (InterruptedException e) {
      }
    }
    throw new RayServeException(
        MessageFormatter.format("Deployment {} wasn't deleted after {}s.", name, timeoutS));
  }

  private DeploymentStatusInfo getDeploymentStatus(String name) {
    return ServeProtoUtil.bytesToProto(
        (byte[])
            ((PyActorHandle) controller)
                .task(PyActorMethod.of("get_deployment_status"), name)
                .remote()
                .get(),
        DeploymentStatusInfo::parseFrom);
  }

  public BaseActorHandle getController() {
    return controller;
  }

  public void deployApplication(String name, List<Deployment> deployments, boolean blocking) {

    DeploymentArgsList.Builder deploymentArgsListBuilder = DeploymentArgsList.newBuilder();

    for (Deployment deployment : deployments) {
      DeploymentArgs deploymentArgs =
          DeploymentArgs.newBuilder()
              .setDeploymentName(deployment.getName())
              .setReplicaConfig(ByteString.copyFrom(deployment.getReplicaConfig().toProtoBytes()))
              .setDeploymentConfig(
                  ByteString.copyFrom(deployment.getDeploymentConfig().toProtoBytes()))
              .setRoutePrefix(deployment.getRoutePrefix())
              .setIngress(deployment.isIngress())
              .setDeployerJobId(
                  ByteString.copyFrom(Ray.getRuntimeContext().getCurrentJobId().getBytes()))
              .build();
      deploymentArgsListBuilder.addDeploymentArgs(deploymentArgs);
    }

    ((PyActorHandle) controller)
        .task(
            PyActorMethod.of("deploy_application_xlang"),
            name,
            deploymentArgsListBuilder.build().toByteArray())
        .remote()
        .get();

    if (blocking) {
      waitForApplicationRunning(name, null);
      for (Deployment deployment : deployments) {
        logDeploymentReady(
            deployment.getName(),
            deployment.getVersion(),
            deployment.getUrl(),
            "component=serve deployment=" + deployment.getName());
      }
    }
  }

  /**
   * Waits for the named application to enter "RUNNING" status.
   *
   * @param name application name
   * @param timeoutS unit: second
   */
  private void waitForApplicationRunning(String name, Long timeoutS) {
    long start = System.currentTimeMillis();
    while (timeoutS == null || System.currentTimeMillis() - start < timeoutS * 1000) {

      StatusOverview status = getServeStatus(name);
      if (status.getAppStatus().getStatus() == ApplicationStatus.APPLICATION_STATUS_RUNNING) {
        return;
      } else if (status.getAppStatus().getStatus()
          == ApplicationStatus.APPLICATION_STATUS_DEPLOY_FAILED) {
        throw new RayServeException(
            MessageFormatter.format(
                "Deploying application {} is failed: {}",
                name,
                status.getAppStatus().getMessage()));
      }

      LOGGER.debug(
          "Waiting for {} to be RUNNING, current status: {}.",
          name,
          status.getAppStatus().getStatus());
      try {
        Thread.sleep(CLIENT_POLLING_INTERVAL_S * 1000);
      } catch (InterruptedException e) {
      }
    }

    throw new RayServeException(
        MessageFormatter.format(
            "Application {} did not become RUNNING after {}s.", name, timeoutS));
  }

  private void logDeploymentReady(String name, String version, String url, String tag) {
    String urlPart = url != null ? MessageFormatter.format(" at `{}`", url) : "";
    LOGGER.info(
        "Deployment '{}{}' is ready {}. {}",
        name,
        StringUtils.isNotBlank(version) ? "':'" + version : "",
        urlPart,
        tag);
  }

  public void deleteApps(List<String> names, boolean blocking) {
    if (CollectionUtil.isEmpty(names)) {
      return;
    }

    LOGGER.info("Deleting app {}", names);

    ListApplicationsResponse apps =
        ListApplicationsResponse.newBuilder().addAllApplicationNames(names).build();
    ((PyActorHandle) controller)
        .task(PyActorMethod.of("delete_apps_xlang"), apps.toByteArray())
        .remote()
        .get();

    if (blocking) {
      long start = System.currentTimeMillis();
      List<String> undeleted = new ArrayList<>(names);

      while (System.currentTimeMillis() - start < 60 * 1000) {

        Iterator<String> iterator = undeleted.iterator();
        while (iterator.hasNext()) {

          String name = iterator.next();
          StatusOverview status = getServeStatus(name);
          if (status.getAppStatus().getStatus()
              == ApplicationStatus.APPLICATION_STATUS_NOT_STARTED) {
            iterator.remove();
          }
        }

        if (undeleted.isEmpty()) {
          return;
        }

        try {
          Thread.sleep(CLIENT_POLLING_INTERVAL_S * 1000);
        } catch (InterruptedException e) {
        }
      }
      throw new RayServeException(
          MessageFormatter.format(
              "Some of these applications weren't deleted after 60s: {}", names));
    }
  }

  public StatusOverview getServeStatus(String name) {
    byte[] statusBytes =
        (byte[])
            ((PyActorHandle) controller)
                .task(PyActorMethod.of("get_serve_status"), name)
                .remote()
                .get();
    return ServeProtoUtil.bytesToProto(statusBytes, StatusOverview::parseFrom);
  }
}
