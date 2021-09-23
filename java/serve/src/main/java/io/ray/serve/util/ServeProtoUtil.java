package io.ray.serve.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import io.ray.runtime.serializer.MessagePackSerializer;
import io.ray.serve.RayServeException;
import io.ray.serve.generated.DeploymentConfig;
import io.ray.serve.generated.BackendLanguage;
import io.ray.serve.generated.RequestMetadata;
import io.ray.serve.generated.RequestWrapper;
import org.apache.commons.lang3.StringUtils;

public class ServeProtoUtil {

  public static DeploymentConfig parseDeploymentConfig(byte[] DeploymentConfigBytes)
      throws InvalidProtocolBufferException {

    // Get a builder from DeploymentConfig(bytes) or create a new one.
    DeploymentConfig.Builder builder = null;
    if (DeploymentConfigBytes == null) {
      builder = DeploymentConfig.newBuilder();
    } else {
      DeploymentConfig DeploymentConfig = DeploymentConfig.parseFrom(DeploymentConfigBytes);
      if (DeploymentConfig == null) {
        builder = DeploymentConfig.newBuilder();
      } else {
        builder = DeploymentConfig.newBuilder(DeploymentConfig);
      }
    }

    // Set default values.
    if (builder.getNumReplicas() == 0) {
      builder.setNumReplicas(1);
    }

    Preconditions.checkArgument(
        builder.getMaxConcurrentQueries() >= 0, "max_concurrent_queries must be >= 0");
    if (builder.getMaxConcurrentQueries() == 0) {
      builder.setMaxConcurrentQueries(100);
    }

    if (builder.getExperimentalGracefulShutdownWaitLoopS() == 0) {
      builder.setExperimentalGracefulShutdownWaitLoopS(2);
    }

    if (builder.getExperimentalGracefulShutdownTimeoutS() == 0) {
      builder.setExperimentalGracefulShutdownTimeoutS(20);
    }

    if (builder.getBackendLanguage() == BackendLanguage.UNRECOGNIZED) {
      throw new RayServeException(
          LogUtil.format(
              "Unrecognized backend language {}. Backend language must be in {}.",
              builder.getBackendLanguageValue(),
              Lists.newArrayList(BackendLanguage.values())));
    }

    return builder.build();
  }

  public static Object parseUserConfig(DeploymentConfig DeploymentConfig) {
    if (DeploymentConfig.getUserConfig() == null || DeploymentConfig.getUserConfig().size() == 0) {
      return null;
    }
    return MessagePackSerializer.decode(DeploymentConfig.getUserConfig().toByteArray(), Object.class);
  }

  public static RequestMetadata parseRequestMetadata(byte[] requestMetadataBytes)
      throws InvalidProtocolBufferException {

    // Get a builder from RequestMetadata(bytes) or create a new one.
    RequestMetadata.Builder builder = null;
    if (requestMetadataBytes == null) {
      builder = RequestMetadata.newBuilder();
    } else {
      RequestMetadata requestMetadata = RequestMetadata.parseFrom(requestMetadataBytes);
      if (requestMetadata == null) {
        builder = RequestMetadata.newBuilder();
      } else {
        builder = RequestMetadata.newBuilder(requestMetadata);
      }
    }

    // Set default values.
    if (StringUtils.isBlank(builder.getCallMethod())) {
      builder.setCallMethod("call");
    }

    return builder.build();
  }

  public static RequestWrapper parseRequestWrapper(byte[] httpRequestWrapperBytes)
      throws InvalidProtocolBufferException {

    // Get a builder from HTTPRequestWrapper(bytes) or create a new one.
    RequestWrapper.Builder builder = null;
    if (httpRequestWrapperBytes == null) {
      builder = RequestWrapper.newBuilder();
    } else {
      RequestWrapper requestWrapper = RequestWrapper.parseFrom(httpRequestWrapperBytes);
      if (requestWrapper == null) {
        builder = RequestWrapper.newBuilder();
      } else {
        builder = RequestWrapper.newBuilder(requestWrapper);
      }
    }

    return builder.build();
  }
}
