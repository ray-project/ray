package io.ray.serve;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import io.ray.api.BaseActorHandle;
import io.ray.api.Ray;
import io.ray.runtime.serializer.MessagePackSerializer;
import io.ray.serve.api.Serve;
import io.ray.serve.generated.BackendConfig;
import io.ray.serve.generated.ReplicaConfig;
import io.ray.serve.generated.RequestMetadata;
import io.ray.serve.util.ReflectUtil;
import io.ray.serve.util.ServeProtoUtil;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Replica class wrapping the provided class. Note that Java function is not supported now. */
public class RayServeWrappedReplica {

  private static final Logger LOGGER = LoggerFactory.getLogger(RayServeWrappedReplica.class);

  private RayServeReplica backend;

  @SuppressWarnings("rawtypes")
  public RayServeWrappedReplica(
    byte[] backendConfigProtoBytes,
    byte[] replicaConfigProtoBytes,
    byte[] initArgsbytes,
      BaseActorHandle controllerHandle)
      throws ClassNotFoundException, NoSuchMethodException, InstantiationException,
          IllegalAccessException, IllegalArgumentException, InvocationTargetException, IOException {

    // Parse BackendConfig.
    BackendConfig backendConfig = ServeProtoUtil.parseBackendConfig(backendConfigProtoBytes);

    // Parse init args.
    Object[] initArgs = parseInitArgs(initArgsbytes, backendConfig);
    LOGGER.error("init Args length "+initArgs.length);

    ReplicaConfig replicaConfig = ReplicaConfig.parseFrom(replicaConfigProtoBytes);

    // Instantiate the object defined by backendDef.
    Class backendClass = Class.forName(replicaConfig.getFuncOrClassName());
    Object callable = ReflectUtil.getConstructor(backendClass, initArgs).newInstance(initArgs);

    // Set the controller name so that Serve.connect() in the user's backend code will connect to
    // the instance that this backend is running in.
    Serve.setInternalReplicaContext(replicaConfig.getBackendTag(), replicaConfig.getReplicaTag(), "dummy controller name", callable);

    // Construct worker replica.
    backend = new RayServeReplica(callable, backendConfig, controllerHandle);
    LOGGER.error("Finish init!");
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

  /** Wait until there is no request in processing. It is used for stopping replica gracefully. */
  public void drainPendingQueries() {
    backend.drainPendingQueries();
  }
}
