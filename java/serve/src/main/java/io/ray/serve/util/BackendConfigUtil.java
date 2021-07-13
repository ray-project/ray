package io.ray.serve.util;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import io.ray.serve.generated.BackendConfig;

public class BackendConfigUtil {

  public static BackendConfig parseFrom(byte[] backendConfigBytes)
      throws InvalidProtocolBufferException {

    // Parse BackendConfig from byte[].
    BackendConfig inputBackendConfig = BackendConfig.parseFrom(backendConfigBytes);
    if (inputBackendConfig == null) {
      return null;
    }

    // Set default values.
    BackendConfig.Builder builder = BackendConfig.newBuilder();

    if (inputBackendConfig.getNumReplicas() == 0) {
      builder.setNumReplicas(1);
    } else {
      builder.setNumReplicas(inputBackendConfig.getNumReplicas());
    }

    Preconditions.checkArgument(
        inputBackendConfig.getMaxConcurrentQueries() >= 0, "max_concurrent_queries must be >= 0");
    if (inputBackendConfig.getMaxConcurrentQueries() == 0) {
      builder.setMaxConcurrentQueries(100);
    } else {
      builder.setMaxConcurrentQueries(inputBackendConfig.getMaxConcurrentQueries());
    }

    builder.setUserConfig(inputBackendConfig.getUserConfig());

    if (inputBackendConfig.getExperimentalGracefulShutdownWaitLoopS() == 0) {
      builder.setExperimentalGracefulShutdownWaitLoopS(2);
    } else {
      builder.setExperimentalGracefulShutdownWaitLoopS(
          inputBackendConfig.getExperimentalGracefulShutdownWaitLoopS());
    }

    if (inputBackendConfig.getExperimentalGracefulShutdownTimeoutS() == 0) {
      builder.setExperimentalGracefulShutdownTimeoutS(20);
    } else {
      builder.setExperimentalGracefulShutdownTimeoutS(
          inputBackendConfig.getExperimentalGracefulShutdownTimeoutS());
    }

    return builder.build();
  }
}
