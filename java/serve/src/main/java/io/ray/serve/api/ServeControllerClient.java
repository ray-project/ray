package io.ray.serve.api;

import com.google.protobuf.ByteString;
import io.ray.api.ActorHandle;
import io.ray.api.BaseActorHandle;
import io.ray.api.PyActorHandle;
import io.ray.api.Ray;
import io.ray.api.exception.RayActorException;
import io.ray.api.exception.RayTimeoutException;
import io.ray.api.function.PyActorMethod;
import io.ray.serve.common.Constants;
import io.ray.serve.controller.ServeController;
import io.ray.serve.deployment.Deployment;
import io.ray.serve.deployment.DeploymentRoute;
import io.ray.serve.exception.RayServeException;
import io.ray.serve.generated.ApplicationStatus;
import io.ray.serve.generated.DeploymentArgs;
import io.ray.serve.generated.EndpointInfo;
import io.ray.serve.generated.StatusOverview;
import io.ray.serve.handle.DeploymentHandle;
import io.ray.serve.util.CollectionUtil;
import io.ray.serve.util.MessageFormatter;
import io.ray.serve.util.ServeProtoUtil;
import java.util.ArrayList;
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

  private boolean shutdown;

  private Map<String, DeploymentHandle> handleCache = new ConcurrentHashMap<>();

  private String rootUrl;

  @SuppressWarnings("unchecked")
  public ServeControllerClient(BaseActorHandle controller) {
    this.controller = controller;
    this.rootUrl =
        controller instanceof PyActorHandle
            ? (String)
                ((PyActorHandle) controller).task(PyActorMethod.of("get_root_url")).remote().get()
            : ((ActorHandle<ServeController>) controller)
                .task(ServeController::getRootUrl)
                .remote()
                .get();
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
  public DeploymentHandle getDeploymentHandle(
      String deploymentName, String appName, boolean missingOk) {
    String cacheKey =
        StringUtils.join(
            new Object[] {deploymentName, appName, missingOk}, Constants.SEPARATOR_HASH);
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

    DeploymentHandle handle = new DeploymentHandle(deploymentName, appName);
    handleCache.put(cacheKey, handle);
    return handle;
  }

  /**
   * Completely shut down the connected Serve instance.
   *
   * <p>Shuts down all processes and deletes all state associated with the instance.
   *
   * @param timeoutS The unit is second.
   */
  public synchronized void shutdown(Long timeoutS) {
    if (Ray.isInitialized() && !shutdown) {

      if (timeoutS == null) {
        timeoutS = 30L;
      }

      try {
        ((PyActorHandle) controller)
            .task(PyActorMethod.of("graceful_shutdown"))
            .remote()
            .get(timeoutS * 1000);
      } catch (RayActorException e) {
        // Controller has been shut down.
        return;
      } catch (RayTimeoutException e) {
        LOGGER.warn(
            "Controller failed to shut down within {}s. Check controller logs for more details.",
            timeoutS);
      }
      shutdown = true;
    }
  }

  public String getRootUrl() {
    return rootUrl;
  }

  /**
   * @deprecated {@value Constants#MIGRATION_MESSAGE}
   * @param name
   * @return
   */
  @Deprecated
  public DeploymentRoute getDeploymentInfo(String name) {
    return DeploymentRoute.fromProtoBytes(
        (byte[])
            ((PyActorHandle) controller)
                .task(PyActorMethod.of("get_deployment_info"), name)
                .remote()
                .get());
  }

  public BaseActorHandle getController() {
    return controller;
  }

  /**
   * Deployment an application with deployment list.
   *
   * @param name application name.
   * @param routePrefix route prefix for the application.
   * @param deployments deployment list.
   * @param ingressDeploymentName name of the ingress deployment (the one that is exposed over
   *     HTTP).
   * @param blocking Wait for the applications to be deployed or not.
   */
  public void deployApplication(
      String name,
      String routePrefix,
      List<Deployment> deployments,
      String ingressDeploymentName,
      boolean blocking) {

    Object[] deploymentArgsArray = new Object[deployments.size()];

    for (int i = 0; i < deployments.size(); i++) {
      Deployment deployment = deployments.get(i);
      DeploymentArgs.Builder deploymentArgs =
          DeploymentArgs.newBuilder()
              .setDeploymentName(deployment.getName())
              .setReplicaConfig(ByteString.copyFrom(deployment.getReplicaConfig().toProtoBytes()))
              .setDeploymentConfig(
                  ByteString.copyFrom(deployment.getDeploymentConfig().toProtoBytes()))
              .setIngress(deployment.isIngress())
              .setDeployerJobId(Ray.getRuntimeContext().getCurrentJobId().toString());
      if (deployment.getName() == ingressDeploymentName) {
        deploymentArgs.setRoutePrefix(routePrefix);
      }
      deploymentArgsArray[i] = deploymentArgs.build().toByteArray();
    }

    ((PyActorHandle) controller)
        .task(PyActorMethod.of("deploy_application"), name, deploymentArgsArray)
        .remote()
        .get();

    if (blocking) {
      waitForApplicationRunning(name, null);
      for (Deployment deployment : deployments) {
        logDeploymentReady(
            deployment.getName(),
            deployment.getVersion(),
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

  private void logDeploymentReady(String name, String version, String tag) {
    LOGGER.info(
        "Deployment '{}{}' is ready. {}",
        name,
        StringUtils.isNotBlank(version) ? "':'" + version : "",
        tag);
  }

  /**
   * Delete the specified applications.
   *
   * @param names application names
   * @param blocking Wait for the applications to be deleted or not.
   */
  public void deleteApps(List<String> names, boolean blocking) {
    if (CollectionUtil.isEmpty(names)) {
      return;
    }

    LOGGER.info("Deleting app {}", names);

    ((PyActorHandle) controller)
        .task(PyActorMethod.of("delete_apps"), names.toArray())
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

  /**
   * Return the status of the specified application.
   *
   * @param name application name
   * @return
   */
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
