package io.ray.serve.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import io.ray.runtime.serializer.MessagePackSerializer;
import io.ray.serve.RayServeException;
import io.ray.serve.generated.BackendConfig;
import io.ray.serve.generated.BackendLanguage;

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

    builder.setIsCrossLanguage(inputBackendConfig.getIsCrossLanguage());

    if (inputBackendConfig.getBackendLanguage() == BackendLanguage.UNRECOGNIZED) {
      throw new RayServeException(
          LogUtil.format(
              "Unrecognized backend language {}. Backend language must be in {}.",
              inputBackendConfig.getBackendLanguageValue(),
              Lists.newArrayList(BackendLanguage.values())));
    } else {
      builder.setBackendLanguage(inputBackendConfig.getBackendLanguage());
    }

    return builder.build();
  }

  public static Object getUserConfig(BackendConfig backendConfig) {
    if (backendConfig.getUserConfig() == null || backendConfig.getUserConfig().size() == 0) {
      return null;
    }
    return MessagePackSerializer.decode(backendConfig.getUserConfig().toByteArray(), Object.class);
  }
}
