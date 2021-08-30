package io.ray.serve.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import io.ray.runtime.serializer.MessagePackSerializer;
import io.ray.serve.RayServeException;
import io.ray.serve.generated.BackendConfig;
import io.ray.serve.generated.BackendLanguage;
import io.ray.serve.generated.HTTPRequestWrapper;
import io.ray.serve.generated.RequestMetadata;
import org.apache.commons.lang3.StringUtils;

public class ServeProtoUtil {

  public static BackendConfig parseBackendConfig(byte[] backendConfigBytes)
      throws InvalidProtocolBufferException {

    // Parse BackendConfig from byte[].
    BackendConfig inputBackendConfig = BackendConfig.parseFrom(backendConfigBytes);
    if (inputBackendConfig == null) {
      return null;
    }

    // Copy the input BackendConfig and set default values.
    BackendConfig.Builder builder = BackendConfig.newBuilder(inputBackendConfig);

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

  public static Object parseUserConfig(BackendConfig backendConfig) {
    if (backendConfig.getUserConfig() == null || backendConfig.getUserConfig().size() == 0) {
      return null;
    }
    return MessagePackSerializer.decode(backendConfig.getUserConfig().toByteArray(), Object.class);
  }

  public static RequestMetadata parseRequestMetadata(byte[] requestMetadataBytes)
      throws InvalidProtocolBufferException {

    // Parse RequestMetadata from byte[].
    RequestMetadata inputRequestMetadata = RequestMetadata.parseFrom(requestMetadataBytes);
    if (inputRequestMetadata == null) {
      return null;
    }

    // Copy the input RequestMetadata and set default values.
    RequestMetadata.Builder builder = RequestMetadata.newBuilder(inputRequestMetadata);
    if (StringUtils.isBlank(builder.getCallMethod())) {
      builder.setCallMethod("call");
    }

    return builder.build();
  }

  public static HTTPRequestWrapper parseHTTPRequestWrapper(byte[] httpRequestWrapperBytes)
      throws InvalidProtocolBufferException {

    // Parse RequestMetadata from byte[].
    HTTPRequestWrapper httpRequestWrapper = HTTPRequestWrapper.parseFrom(httpRequestWrapperBytes);
    return httpRequestWrapper;
  }
}
