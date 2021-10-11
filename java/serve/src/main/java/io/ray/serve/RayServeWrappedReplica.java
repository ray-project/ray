package io.ray.serve;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import io.ray.api.BaseActorHandle;
import io.ray.api.Ray;
import io.ray.runtime.serializer.MessagePackSerializer;
import io.ray.serve.api.Serve;
import io.ray.serve.generated.BackendConfig;
import io.ray.serve.generated.BackendVersion;
import io.ray.serve.generated.RequestMetadata;
import io.ray.serve.util.ReflectUtil;
import io.ray.serve.util.ServeProtoUtil;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;

/** Replica class wrapping the provided class. Note that Java function is not supported now. */
public class RayServeWrappedReplica {

  private RayServeReplica backend;

  @SuppressWarnings("rawtypes")
  public RayServeWrappedReplica(
      String backendTag,
      String replicaTag,
      String backendDef,
      byte[] initArgsbytes,
      byte[] backendConfigBytes,
      byte[] backendVersionBytes,
      String controllerName)
      throws ClassNotFoundException, NoSuchMethodException, InstantiationException,
          IllegalAccessException, IllegalArgumentException, InvocationTargetException, IOException {

    // Parse BackendConfig.
    BackendConfig backendConfig = ServeProtoUtil.parseBackendConfig(backendConfigBytes);

    // Parse init args.
    Object[] initArgs = parseInitArgs(initArgsbytes, backendConfig);

    // Instantiate the object defined by backendDef.
    Class backendClass = Class.forName(backendDef);
    Object callable = ReflectUtil.getConstructor(backendClass, initArgs).newInstance(initArgs);

    // Get the controller by controllerName.
    Preconditions.checkArgument(
        StringUtils.isNotBlank(controllerName), "Must provide a valid controllerName");
    Optional<BaseActorHandle> optional = Ray.getActor(controllerName);
    Preconditions.checkState(optional.isPresent(), "Controller does not exist");

    // Set the controller name so that Serve.connect() in the user's backend code will connect to
    // the instance that this backend is running in.
    Serve.setInternalReplicaContext(backendTag, replicaTag, controllerName, callable);

    // Construct worker replica.
    backend =
        new RayServeReplica(
            callable,
            backendConfig,
            ServeProtoUtil.parseBackendVersion(backendVersionBytes),
            optional.get());
  }

  public RayServeWrappedReplica(
      String backendTag, String replicaTag, DeploymentInfo deploymentInfo, String controllerName)
      throws ClassNotFoundException, NoSuchMethodException, InstantiationException,
          IllegalAccessException, IllegalArgumentException, InvocationTargetException, IOException {
    this(
        backendTag,
        replicaTag,
        deploymentInfo.getReplicaConfig().getBackendDef(),
        deploymentInfo.getReplicaConfig().getInitArgs(),
        deploymentInfo.getBackendConfig(),
        deploymentInfo.getBackendVersion(),
        controllerName);
  }

  private Object[] parseInitArgs(byte[] initArgsbytes, BackendConfig backendConfig)
      throws IOException {

    if (initArgsbytes == null || initArgsbytes.length == 0) {
      return new Object[0];
    }

    if (!backendConfig.getIsCrossLanguage()) {
      // If the construction request is from Java API, deserialize initArgsbytes to Object[]
      // directly.
      return MessagePackSerializer.decode(initArgsbytes, Object[].class);
    } else {
      // For other language like Python API, not support Array type.
      return new Object[] {MessagePackSerializer.decode(initArgsbytes, Object.class)};
    }
  }

  /**
   * The entry method to process the request.
   *
   * @param requestMetadata the real type is byte[] if this invocation is cross-language. Otherwise,
   *     the real type is {@link io.ray.serve.generated.RequestMetadata}.
   * @param requestArgs The input parameters of the specified method of the object defined by
   *     backendDef. The real type is serialized {@link io.ray.serve.generated.RequestWrapper} if
   *     this invocation is cross-language. Otherwise, the real type is Object[].
   * @return the result of request being processed
   * @throws InvalidProtocolBufferException if the protobuf deserialization fails.
   */
  public Object handleRequest(Object requestMetadata, Object requestArgs)
      throws InvalidProtocolBufferException {
    boolean isCrossLanguage = requestMetadata instanceof byte[];
    return backend.handleRequest(
        new Query(
            isCrossLanguage
                ? ServeProtoUtil.parseRequestMetadata((byte[]) requestMetadata)
                : (RequestMetadata) requestMetadata,
            isCrossLanguage
                ? ServeProtoUtil.parseRequestWrapper((byte[]) requestArgs)
                : requestArgs));
  }

  /** Check whether this replica is ready or not. */
  public void ready() {
    return;
  }

  /**
   * Wait until there is no request in processing. It is used for stopping replica gracefully.
   *
   * @return true if it is ready for shutdown.
   */
  public boolean prepareForShutdown() {
    return backend.prepareForShutdown();
  }

  public byte[] reconfigure(Object userConfig) {
    BackendVersion backendVersion = backend.reconfigure(userConfig);
    return backendVersion.toByteArray();
  }

  public byte[] getVersion() {
    return backend.getVersion().toByteArray();
  }
}
