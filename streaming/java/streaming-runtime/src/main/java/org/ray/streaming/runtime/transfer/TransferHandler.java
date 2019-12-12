package org.ray.streaming.runtime.transfer;

public class TransferHandler {

  private long writerClientNative;
  private long readerClientNative;

  public TransferHandler(long coreWorkerNative) {
    writerClientNative = createWriterClientNative(coreWorkerNative);
    readerClientNative = createReaderClientNative(coreWorkerNative);
  }

  public void handleWriterMessage(byte[] buffer) {
    handleWriterMessageNative(writerClientNative, buffer);
  }

  public byte[] handleWriterMessageSync(byte[] buffer) {
    return handleWriterMessageSyncNative(writerClientNative, buffer);
  }

  public void handleReaderMessage(byte[] buffer) {
    handleReaderMessageNative(readerClientNative, buffer);
  }

  public byte[] handleReaderMessageSync(byte[] buffer) {
    return handleReaderMessageSyncNative(readerClientNative, buffer);
  }

  private native long createWriterClientNative(long coreWorkerNative);
  private native long createReaderClientNative(long coreWorkerNative);
  private native void handleWriterMessageNative(long handler, byte[] buffer);
  private native byte[] handleWriterMessageSyncNative(long handler, byte[] buffer);
  private native void handleReaderMessageNative(long handler, byte[] buffer);
  private native byte[] handleReaderMessageSyncNative(long handler, byte[] buffer);
}
