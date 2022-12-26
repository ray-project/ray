package io.ray.serve.replica;

import com.google.common.base.Preconditions;
import io.ray.api.BaseActorHandle;
import io.ray.api.Ray;
import io.ray.runtime.serializer.MessagePackSerializer;
import io.ray.serve.api.Serve;
import io.ray.serve.common.Constants;
import io.ray.serve.config.DeploymentConfig;
import io.ray.serve.config.RayServeConfig;
import io.ray.serve.deployment.DeploymentVersion;
import io.ray.serve.deployment.DeploymentWrapper;
import io.ray.serve.exception.RayServeException;
import io.ray.serve.generated.RequestMetadata;
import io.ray.serve.metrics.RayServeMetrics;
import io.ray.serve.util.LogUtil;
import io.ray.serve.util.ServeProtoUtil;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Replica class wrapping the provided class. Note that Java function is not supported now. */
public class RayServeWrappedReplica implements RayServeReplica {

  private static final Logger LOGGER = LoggerFactory.getLogger(RayServeReplicaImpl.class);

  private DeploymentWrapper deploymentInfo;

  private RayServeReplicaImpl replica;

  public RayServeWrappedReplica(
      String deploymentName,
      String replicaTag,
      String deploymentDef,
      byte[] initArgsbytes,
      byte[] deploymentConfigBytes,
      byte[] deploymentVersionBytes,
      String controllerName) {

    // Parse DeploymentConfig.
    DeploymentConfig deploymentConfig = DeploymentConfig.fromProtoBytes(deploymentConfigBytes);

    // Parse init args.
    Object[] initArgs = null;
    try {
      initArgs = parseInitArgs(initArgsbytes);
    } catch (IOException e) {
      String errMsg =
          LogUtil.format(
              "Failed to initialize replica {} of deployment {}",
              replicaTag,
              deploymentInfo.getName());
      LOGGER.error(errMsg, e);
      throw new RayServeException(errMsg, e);
    }

    DeploymentVersion version = DeploymentVersion.fromProtoBytes(deploymentVersionBytes);

    DeploymentWrapper deploymentWrapper = new DeploymentWrapper();
    deploymentWrapper.setName(deploymentName);
    deploymentWrapper.setDeploymentDef(deploymentDef);
    deploymentWrapper.setDeploymentConfig(deploymentConfig);
    deploymentWrapper.setInitArgs(initArgs);
    deploymentWrapper.setDeploymentVersion(version);

    // Init replica.
    init(deploymentWrapper, replicaTag, controllerName);
  }

  public RayServeWrappedReplica(
      DeploymentWrapper deploymentWrapper, String replicaTag, String controllerName) {
    init(deploymentWrapper, replicaTag, controllerName);
  }

  @SuppressWarnings("rawtypes")
  private void init(DeploymentWrapper deploymentWrapper, String replicaTag, String controllerName) {
    try {
      // Set the controller name so that Serve.connect() in the user's code will connect to the
      // instance that this deployment is running in.
      Serve.setInternalReplicaContext(
          deploymentWrapper.getName(),
          replicaTag,
          controllerName,
          null,
          deploymentWrapper.getConfig());

      // Instantiate the object defined by deploymentDef.
      Object callable = CallableUtils.makeCallable(deploymentWrapper);
      Serve.getReplicaContext().setServableObject(callable);

      // Get the controller by controllerName.
      Preconditions.checkArgument(
          StringUtils.isNotBlank(controllerName), "Must provide a valid controllerName");
      Optional<BaseActorHandle> optional = Ray.getActor(controllerName, Constants.SERVE_NAMESPACE);
      Preconditions.checkState(optional.isPresent(), "Controller does not exist");

      // Enable metrics.
      enableMetrics(deploymentWrapper.getConfig());

      // Construct worker replica.
      this.replica =
          new RayServeReplicaImpl(
              callable,
              deploymentWrapper.getDeploymentConfig(),
              deploymentWrapper.getDeploymentVersion(),
              optional.get());
      this.deploymentInfo = deploymentWrapper;
    } catch (Throwable e) {
      String errMsg =
          LogUtil.format(
              "Failed to initialize replica {} of deployment {}",
              replicaTag,
              deploymentWrapper.getName());
      LOGGER.error(errMsg, e);
      throw new RayServeException(errMsg, e);
    }
  }

  private void enableMetrics(Map<String, String> config) {
    Optional.ofNullable(config)
        .map(conf -> conf.get(RayServeConfig.METRICS_ENABLED))
        .ifPresent(
            enabled -> {
              if (Boolean.valueOf(enabled)) {
                RayServeMetrics.enable();
              } else {
                RayServeMetrics.disable();
              }
            });
  }

  private Object[] parseInitArgs(byte[] initArgsbytes) throws IOException {

    if (initArgsbytes == null || initArgsbytes.length == 0) {
      return new Object[0];
    }

    return MessagePackSerializer.decode(initArgsbytes, Object[].class);
  }

  /**
   * The entry method to process the request.
   *
   * @param requestMetadata the real type is byte[] if this invocation is cross-language. Otherwise,
   *     the real type is {@link io.ray.serve.generated.RequestMetadata}.
   * @param requestArgs The input parameters of the specified method of the object defined by
   *     deploymentDef. The real type is serialized {@link io.ray.serve.generated.RequestWrapper} if
   *     this invocation is cross-language. Otherwise, the real type is Object[].
   * @return the result of request being processed
   */
  @Override
  public Object handleRequest(Object requestMetadata, Object requestArgs) {
    boolean isCrossLanguage = requestMetadata instanceof byte[];
    return replica.handleRequest(
        isCrossLanguage
            ? ServeProtoUtil.parseRequestMetadata((byte[]) requestMetadata)
            : (RequestMetadata) requestMetadata,
        isCrossLanguage ? ServeProtoUtil.parseRequestWrapper((byte[]) requestArgs) : requestArgs);
  }

  /**
   * Check if the actor is healthy.
   *
   * @return true if the actor is health, or return false.
   */
  @Override
  public boolean checkHealth() {
    return replica.checkHealth();
  }

  /**
   * Tell the caller this replica is successfully launched.
   *
   * @return
   */
  public boolean isAllocated() {
    return true;
  }

  /**
   * Wait until there is no request in processing. It is used for stopping replica gracefully.
   *
   * @return true if it is ready for shutdown.
   */
  @Override
  public boolean prepareForShutdown() {
    return replica.prepareForShutdown();
  }

  /**
   * Reconfigure user's configuration in the callable object through its reconfigure method.
   *
   * @param userConfig new user's configuration
   * @return DeploymentVersion. If the current invocation is crossing language, the
   *     DeploymentVersion is serialized to protobuf byte[].
   */
  @Override
  public Object reconfigure(Object userConfig) {
    boolean isCrossLanguage = userConfig instanceof byte[];
    DeploymentVersion deploymentVersion =
        replica.reconfigure(
            isCrossLanguage && userConfig != null
                ? MessagePackSerializer.decode((byte[]) userConfig, Object.class)
                : userConfig);
    return deploymentVersion.toProtoBytes();
  }

  /**
   * Get the deployment version of the current replica.
   *
   * @return DeploymentVersion. If the current invocation is crossing language, the
   *     DeploymentVersion is serialized to protobuf byte[].
   */
  public Object getVersion() {
    DeploymentVersion deploymentVersion = replica.getVersion();
    return deploymentVersion.toProtoBytes();
  }

  public Object getCallable() {
    return replica.getCallable();
  }
}
