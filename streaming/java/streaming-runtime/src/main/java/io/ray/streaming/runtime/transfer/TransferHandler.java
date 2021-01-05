package io.ray.streaming.runtime.transfer;

import io.ray.runtime.util.BinaryFileUtil;
import io.ray.runtime.util.JniUtils;

/**
 * TransferHandler is used for handle direct call based data transfer between workers.
 * TransferHandler is used by streaming queue for data transfer.
 */
public class TransferHandler {

  static {
    JniUtils.loadLibrary(BinaryFileUtil.CORE_WORKER_JAVA_LIBRARY, true);
    JniUtils.loadLibrary("streaming_java");
  }

  private long writerClientNative;
  private long readerClientNative;

  public TransferHandler() {
    writerClientNative = createWriterClientNative();
    readerClientNative = createReaderClientNative();
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

  private native long createWriterClientNative();

  private native long createReaderClientNative();

  private native void handleWriterMessageNative(long handler, byte[] buffer);

  private native byte[] handleWriterMessageSyncNative(long handler, byte[] buffer);

  private native void handleReaderMessageNative(long handler, byte[] buffer);

  private native byte[] handleReaderMessageSyncNative(long handler, byte[] buffer);
}
