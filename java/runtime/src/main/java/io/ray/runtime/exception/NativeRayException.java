package io.ray.runtime.exception;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.ray.runtime.generated.Common.Language;
import io.ray.runtime.generated.Common.RayException;
import io.ray.runtime.serializer.Serializer;

public class NativeRayException extends RuntimeException {
  public NativeRayException(String message) {
    super(message);
  }

  public NativeRayException(String message, Throwable cause) {
    super(message, cause);
  }

  public byte[] toBytes() {
    String formattedException = org.apache.commons.lang3.exception.ExceptionUtils
        .getStackTrace(this);
    RayException.Builder builder = RayException.newBuilder();
    builder.setLanguage(Language.JAVA);
    builder.setFormattedExceptionString(formattedException);
    builder.setSerializedException(ByteString.copyFrom(Serializer.encode(this).getLeft()));
    return builder.build().toByteArray();
  }

  public static NativeRayException fromBytes(byte[] serialized)
      throws InvalidProtocolBufferException {
    RayException exception = RayException.parseFrom(serialized);
    if (exception.getLanguage() == Language.JAVA) {
      return Serializer
          .decode(exception.getSerializedException().toByteArray(), NativeRayException.class);
    } else {
      return new CrossLanguageException(exception);
    }
  }
}
