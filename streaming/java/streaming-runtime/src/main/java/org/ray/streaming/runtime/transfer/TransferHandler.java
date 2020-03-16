package org.ray.streaming.runtime.transfer;

import com.google.common.base.Preconditions;
import org.ray.runtime.RayNativeRuntime;
import org.ray.runtime.util.JniUtils;

/**
 * TransferHandler is used for handle direct call based data transfer between workers.
 * TransferHandler is used by streaming queue for data transfer.
 */
public class TransferHandler {

  static {
    try {
      Class.forName(RayNativeRuntime.class.getName());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    JniUtils.loadLibrary("streaming_java");
  }

  private long writerClientNative;
  private long readerClientNative;

  public TransferHandler(long coreWorkerNative) {
    Preconditions.checkArgument(coreWorkerNative != 0);
    writerClientNative = createWriterClientNative(
        coreWorkerNative);
    readerClientNative = createReaderClientNative(
        coreWorkerNative);
  }

  public void onWriterMessage(byte[] buffer) {
    handleWriterMessageNative(writerClientNative, buffer);
  }

  public byte[] onWriterMessageSync(byte[] buffer) {
    return handleWriterMessageSyncNative(writerClientNative, buffer);
  }

  public void onReaderMessage(byte[] buffer) {
    handleReaderMessageNative(readerClientNative, buffer);
  }

  public byte[] onReaderMessageSync(byte[] buffer) {
    return handleReaderMessageSyncNative(readerClientNative, buffer);
  }

  private native long createWriterClientNative(
      long coreWorkerNative);

  private native long createReaderClientNative(
      long coreWorkerNative);

  private native void handleWriterMessageNative(long handler, byte[] buffer);

  private native byte[] handleWriterMessageSyncNative(long handler, byte[] buffer);

  private native void handleReaderMessageNative(long handler, byte[] buffer);

  private native byte[] handleReaderMessageSyncNative(long handler, byte[] buffer);
}
