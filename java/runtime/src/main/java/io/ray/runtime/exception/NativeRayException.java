package io.ray.runtime.exception;

import com.google.common.base.FinalizablePhantomReference;
import com.google.common.base.FinalizableReferenceQueue;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.ray.api.id.ObjectId;
import io.ray.api.runtimecontext.RuntimeContext;
import io.ray.runtime.generated.Common;
import io.ray.runtime.generated.Common.RayException;
import io.ray.runtime.generated.Common.Language;
import io.ray.runtime.generated.Common.RayException.Builder;
import io.ray.runtime.RayRuntimeInternal;
import io.ray.runtime.context.WorkerContext;
import io.ray.runtime.serializer.Serializer;
import io.ray.runtime.util.SystemUtil;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;

import java.lang.ref.Reference;
import java.util.Arrays;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.exception.ExceptionUtils;

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
