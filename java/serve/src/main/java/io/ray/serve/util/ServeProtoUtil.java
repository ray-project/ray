package io.ray.serve.util;

import com.google.protobuf.InvalidProtocolBufferException;
import io.ray.serve.common.Constants;
import io.ray.serve.exception.RayServeException;
import io.ray.serve.generated.EndpointInfo;
import io.ray.serve.generated.EndpointSet;
import io.ray.serve.generated.RequestMetadata;
import io.ray.serve.generated.RequestWrapper;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class ServeProtoUtil {

  public static final String SERVE_PROTO_PARSE_ERROR_MSG =
      "Failed to parse {} from protobuf bytes.";

  public static RequestMetadata parseRequestMetadata(byte[] requestMetadataBytes) {

    // Get a builder from RequestMetadata(bytes) or create a new one.
    RequestMetadata.Builder builder = null;
    if (requestMetadataBytes == null) {
      builder = RequestMetadata.newBuilder();
    } else {
      RequestMetadata requestMetadata = null;
      try {
        requestMetadata = RequestMetadata.parseFrom(requestMetadataBytes);
      } catch (InvalidProtocolBufferException e) {
        throw new RayServeException("Failed to parse RequestMetadata from protobuf bytes.", e);
      }
      if (requestMetadata == null) {
        builder = RequestMetadata.newBuilder();
      } else {
        builder = RequestMetadata.newBuilder(requestMetadata);
      }
    }

    // Set default values.
    if (StringUtils.isBlank(builder.getCallMethod())) {
      builder.setCallMethod(Constants.CALL_METHOD);
    }

    return builder.build();
  }

  public static RequestWrapper parseRequestWrapper(byte[] httpRequestWrapperBytes) {

    // Get a builder from HTTPRequestWrapper(bytes) or create a new one.
    RequestWrapper.Builder builder = null;
    if (httpRequestWrapperBytes == null) {
      builder = RequestWrapper.newBuilder();
    } else {
      RequestWrapper requestWrapper = null;
      try {
        requestWrapper = RequestWrapper.parseFrom(httpRequestWrapperBytes);
      } catch (InvalidProtocolBufferException e) {
        throw new RayServeException("Failed to parse RequestWrapper from protobuf bytes.", e);
      }
      if (requestWrapper == null) {
        builder = RequestWrapper.newBuilder();
      } else {
        builder = RequestWrapper.newBuilder(requestWrapper);
      }
    }

    return builder.build();
  }

  public static Map<String, EndpointInfo> parseEndpointSet(byte[] endpointSetBytes) {
    if (endpointSetBytes == null) {
      return null;
    }
    EndpointSet endpointSet = null;
    try {
      endpointSet = EndpointSet.parseFrom(endpointSetBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new RayServeException("Failed to parse EndpointSet from protobuf bytes.", e);
    }
    if (endpointSet == null) {
      return null;
    }
    return endpointSet.getEndpointsMap();
  }

  public static <T> T bytesToProto(byte[] bytes, ProtobufBytesParser<T> protobufBytesParser) {
    if (bytes == null) {
      return null;
    }
    try {
      T proto = protobufBytesParser.parse(bytes);
      return proto;
    } catch (InvalidProtocolBufferException e) {
      throw new RayServeException("Failed to parse protobuf bytes.", e);
    }
  }
}
