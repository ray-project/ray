package io.ray.runtime.exception;

import io.ray.api.id.ObjectId;
import io.ray.api.runtimecontext.RuntimeContext;
import io.ray.runtime.RayRuntimeInternal;
import io.ray.runtime.context.WorkerContext;
import io.ray.runtime.generated.Common.ErrorType;
import io.ray.runtime.generated.Common.Language;
import io.ray.runtime.serializer.Serializer;
import io.ray.runtime.util.SystemUtil;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.commons.lang3.tuple.Pair;

public class NativeRayException {

  private long nativeHandle;

  public NativeRayException(RayRuntimeInternal runtime, ErrorType errorType, Throwable e) {
    RuntimeContext runtimeContext = runtime.getRuntimeContext();
    WorkerContext workerContext = runtime.getWorkerContext();
    StackTraceElement[] stackTrace = e.getStackTrace();
    String file = stackTrace[0].getFileName();
    int lineNo = stackTrace[0].getLineNumber();
    String function = stackTrace[0].getMethodName();
    StringWriter errors = new StringWriter();
    e.printStackTrace(new PrintWriter(errors));
    String traceBack = errors.toString();
    Pair<byte[], Boolean> serialized = Serializer.encode(e);
    this.nativeHandle = nativeCreateRayException(
        errorType.getNumber(),
        e.getMessage(),
        Language.JAVA_VALUE,
        runtimeContext.getCurrentJobId().getBytes(),
        workerContext.getCurrentWorkerId().getBytes(),
        workerContext.getCurrentTaskId().getBytes(),
        workerContext.getCurrentActorId().getBytes(),
        ObjectId.nil().getBytes(),
        runtime.getRayConfig().nodeIp,
        SystemUtil.pid(),
        "Java Worker",
        file,
        lineNo,
        function,
        traceBack,
        serialized.getLeft());
  }

  protected NativeRayException(byte[] serialized) {
    this.nativeHandle = nativeDeserialize(serialized);
  }

  public static NativeRayException fromBytes(byte[] serialized) {
    return new NativeRayException(serialized);
  }

  public void destroy() {
    nativeDestroy(this.nativeHandle);
  }

  public Language getLanguge() {
    return Language.forNumber(nativeLanguage(this.nativeHandle));
  }

  public Throwable getJavaException() {
    // byte[] serialized = nativeData(this.nativeHandle);
    //    if (getLanguge() == Language.JAVA && serialized.length > 0) {
    return Serializer.decode(nativeData(this.nativeHandle), Throwable.class);
    //    }
  }

  public byte[] serialize() {
    return nativeSerialize(this.nativeHandle);
  }

  @Override
  public String toString() {
    return nativeToString(this.nativeHandle);
  }

  private static native long nativeCreateRayException(int errorType, String errorMessage,
      int language, byte[] jobId, byte[] workerId, byte[] taskId, byte[] actorId, byte[] objectId,
      String ip, int pid, String procTitle, String file, long lineNo, String function,
      String traceBack, byte[] data);

  private static native long nativeDeserialize(byte[] data);

  private static native void nativeDestroy(long handle);

  private static native int nativeLanguage(long handle);

  private static native int nativeErrorType(long handle);

  private static native String nativeToString(long handle);

  private static native byte[] nativeSerialize(long handle);

  private static native byte[] nativeData(long handle);
}
