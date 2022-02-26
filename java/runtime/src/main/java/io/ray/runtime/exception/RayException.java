package io.ray.runtime.exception;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.ray.runtime.generated.Common.Language;
import io.ray.runtime.serializer.Serializer;

public class RayException extends RuntimeException {

  public RayException(String message) {
    super(message);
  }

  public RayException(String message, Throwable cause) {
    super(message, cause);
  }

  public byte[] toBytes() {
    String formattedException =
        org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace(this);
    io.ray.runtime.generated.Common.RayException.Builder builder =
        io.ray.runtime.generated.Common.RayException.newBuilder();
    builder.setLanguage(Language.JAVA);
    builder.setFormattedExceptionString(formattedException);
    builder.setSerializedException(ByteString.copyFrom(Serializer.encode(this).getLeft()));
    return builder.build().toByteArray();
  }

  public static RayException fromRayExceptionPB(
      io.ray.runtime.generated.Common.RayException rayExceptionPB) {
    if (rayExceptionPB.getLanguage() == Language.JAVA) {
      return Serializer.decode(
          rayExceptionPB.getSerializedException().toByteArray(), RayException.class);
    } else {
      return new CrossLanguageException(rayExceptionPB);
    }
  }

  public static RayException fromBytes(byte[] serialized) throws InvalidProtocolBufferException {
    io.ray.runtime.generated.Common.RayException exception =
        io.ray.runtime.generated.Common.RayException.parseFrom(serialized);
    return fromRayExceptionPB(exception);
  }
}
