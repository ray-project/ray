package io.ray.runtime.exception;

import com.google.common.base.FinalizablePhantomReference;
import com.google.common.base.FinalizableReferenceQueue;
import com.google.common.collect.Sets;
import io.ray.api.exception.RayException;
import io.ray.api.id.ObjectId;
import io.ray.api.runtimecontext.RuntimeContext;
import io.ray.runtime.RayRuntimeInternal;
import io.ray.runtime.context.WorkerContext;
import io.ray.runtime.generated.Common.ErrorType;
import io.ray.runtime.generated.Common.Language;
import io.ray.runtime.serializer.Serializer;
import io.ray.runtime.util.SystemUtil;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;

import java.lang.ref.Reference;
import java.util.Arrays;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;

public class NativeRayException extends RayException {

  private static final FinalizableReferenceQueue REFERENCE_QUEUE = new FinalizableReferenceQueue();
  // This ensures that the FinalizablePhantomReference itself is not garbage-collected.
  private static final Set<Reference<?>> references = Sets.newConcurrentHashSet();

  private long nativeHandle;

  /**
   * Wrapper class for PrintStream and PrintWriter to enable a single implementation of
   * printStackTrace.
   */
  private abstract static class PrintStreamOrWriter {

    /**
     * Returns the object to be locked when using this StreamOrWriter
     */
    abstract Object lock();

    /**
     * Prints the specified string as a line on this StreamOrWriter
     */
    abstract void println(Object o);
  }

  private static class WrappedPrintStream extends NativeRayException.PrintStreamOrWriter {

    private final PrintStream printStream;

    WrappedPrintStream(PrintStream printStream) {
      this.printStream = printStream;
    }

    Object lock() {
      return printStream;
    }

    void println(Object o) {
      printStream.println(o);
    }
  }

  private static class WrappedPrintWriter extends NativeRayException.PrintStreamOrWriter {

    private final PrintWriter printWriter;

    WrappedPrintWriter(PrintWriter printWriter) {
      this.printWriter = printWriter;
    }

    Object lock() {
      return printWriter;
    }

    void println(Object o) {
      printWriter.println(o);
    }
  }

  public NativeRayException(RayRuntimeInternal runtime, ErrorType errorType, Throwable e,
      NativeRayException cause) {
    RuntimeContext runtimeContext = runtime.getRuntimeContext();
    WorkerContext workerContext = runtime.getWorkerContext();
    StackTraceElement[] stackTrace = filterTrace(e);
    String file = stackTrace[0].getFileName();
    int lineNo = stackTrace[0].getLineNumber();
    String function = stackTrace[0].getMethodName();
    String traceBack = traceToString(stackTrace);
    Pair<byte[], Boolean> serialized = Serializer.encode(e);
    this.nativeHandle = nativeCreateRayException(
        errorType.getNumber(),
        cause == null ? e.getMessage() : "Get object failed because of deeper errors.",
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
        serialized.getLeft(),
        cause == null ? new byte[0] : cause.toBytes());
    autoDestroyNative(this.nativeHandle);
  }

  protected NativeRayException(byte[] serialized) {
    this.nativeHandle = nativeDeserialize(serialized);
    autoDestroyNative(this.nativeHandle);
  }

  public static NativeRayException fromBytes(byte[] serialized) {
    return new NativeRayException(serialized);
  }

  public byte[] toBytes() {
    return nativeSerialize(this.nativeHandle);
  }

  @Override
  public void printStackTrace(PrintStream s) {
    printStackTrace(new NativeRayException.WrappedPrintStream(s));
  }

  @Override
  public void printStackTrace(PrintWriter s) {
    printStackTrace(new NativeRayException.WrappedPrintWriter(s));
  }

  private void printStackTrace(NativeRayException.PrintStreamOrWriter s) {
    synchronized (s.lock()) {
      String stack = traceToString(filterTrace(this));
      s.println(stack);
      s.println("Caused by:");
      s.println(this);
    }
  }

  @Override
  public String toString() {
    return "\n" + nativeToString(this.nativeHandle);
  }

  @Override
  public Throwable getCause() {
    byte[] serializedCause = nativeCause(this.nativeHandle);
    if (serializedCause.length > 0) {
      return fromBytes(serializedCause);
    }
    return null;
  }

  private void autoDestroyNative(long nativeHandle) {
    if (nativeHandle == 0) {
      return;
    }
    Reference<NativeRayException> reference =
        new FinalizablePhantomReference<NativeRayException>(this, REFERENCE_QUEUE) {
          @Override
          public void finalizeReferent() {
            nativeDestroy(nativeHandle);
            references.remove(this);
          }
        };
    references.add(reference);
  }

  public Language getLanguage() {
    return Language.forNumber(nativeLanguage(this.nativeHandle));
  }

  public Throwable getJavaException() {
    byte[] serialized = nativeData(this.nativeHandle);
    if (getLanguage() == Language.JAVA && serialized.length > 0) {
      return Serializer.decode(serialized, Throwable.class);
    }
    return null;
  }

  private StackTraceElement[] filterTrace(Throwable e) {
    StackTraceElement[] filteredStack = Arrays.stream(e.getStackTrace())
        .filter(se -> !se.getClassName().startsWith("com.sun.proxy") &&
            !se.getClassName().startsWith("sun.reflect") &&
            !se.getClassName().startsWith("java.lang.reflect") &&
            !se.getClassName().startsWith("org.testng") &&
            !se.getClassName().startsWith("com.intellij") &&
            (se.getClassName().startsWith("io.ray.api.test") ||
                !se.getClassName().startsWith("io.ray")))
        .toArray(StackTraceElement[]::new);
    if (filteredStack.length == 0) {
      return e.getStackTrace();
    }
    return filteredStack;
  }

  private String traceToString(StackTraceElement[] trace) {
    StringWriter errors = new StringWriter();
    PrintWriter s = new PrintWriter(errors);
    for (StackTraceElement traceElement : trace) {
      s.println("\tat " + traceElement);
    }
    return errors.toString();
  }

  private static native long nativeCreateRayException(int errorType, String errorMessage,
      int language, byte[] jobId, byte[] workerId, byte[] taskId, byte[] actorId, byte[] objectId,
      String ip, int pid, String procTitle, String file, long lineNo, String function,
      String traceBack, byte[] data, byte[] cause);

  private static native long nativeDeserialize(byte[] data);

  private static native void nativeDestroy(long handle);

  private static native int nativeLanguage(long handle);

  private static native String nativeToString(long handle);

  private static native byte[] nativeSerialize(long handle);

  private static native byte[] nativeData(long handle);

  private static native byte[] nativeCause(long handle);
}
