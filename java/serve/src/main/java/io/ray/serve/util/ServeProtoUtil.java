package io.ray.serve.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import io.ray.runtime.serializer.MessagePackSerializer;
import io.ray.serve.RayServeException;
import io.ray.serve.generated.BackendConfig;
import io.ray.serve.generated.BackendLanguage;
import io.ray.serve.generated.RequestMetadata;
import io.ray.serve.generated.RequestWrapper;
import org.apache.commons.lang3.StringUtils;

public class ServeProtoUtil {

  public static BackendConfig parseBackendConfig(byte[] backendConfigBytes)
      throws InvalidProtocolBufferException {

    // Get a builder from BackendConfig(bytes) or create a new one.
    BackendConfig.Builder builder = null;
    if (backendConfigBytes == null) {
      builder = BackendConfig.newBuilder();
    } else {
      BackendConfig backendConfig = BackendConfig.parseFrom(backendConfigBytes);
      if (backendConfig == null) {
        builder = BackendConfig.newBuilder();
      } else {
        builder = BackendConfig.newBuilder(backendConfig);
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

    if (builder.getGracefulShutdownWaitLoopS() == 0) {
      builder.setGracefulShutdownWaitLoopS(2);
    }

    if (builder.getGracefulShutdownTimeoutS() == 0) {
      builder.setGracefulShutdownTimeoutS(20);
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

  public static Object parseUserConfig(BackendConfig backendConfig) {
    if (backendConfig.getUserConfig() == null || backendConfig.getUserConfig().size() == 0) {
      return null;
    }
    return MessagePackSerializer.decode(backendConfig.getUserConfig().toByteArray(), Object.class);
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
