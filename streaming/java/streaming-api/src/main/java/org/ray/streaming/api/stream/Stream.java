package org.ray.streaming.api.stream;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import org.ray.streaming.api.Language;
import org.ray.streaming.api.context.StreamingContext;
import org.ray.streaming.api.partition.Partition;
import org.ray.streaming.api.partition.impl.RoundRobinPartition;
import org.ray.streaming.operator.StreamOperator;
import org.ray.streaming.python.PythonPartition;

/**
 * Abstract base class of all stream types.
 *
 * @param <S> Type of stream class
 * @param <T> Type of the data in the stream.
 */
public abstract class Stream<S extends Stream<S, T>, T>
    implements Serializable {
  private int id;
  private int parallelism = 1;
  private StreamOperator operator;
  private Stream inputStream;
  private StreamingContext streamingContext;
  private Partition<T> partition;

  private Stream referencedStream;

  public Stream(StreamingContext streamingContext, StreamOperator streamOperator) {
    this.streamingContext = streamingContext;
    this.operator = streamOperator;
    this.id = streamingContext.generateId();
    this.partition = selectPartition();
  }

  public Stream(Stream inputStream, StreamOperator streamOperator) {
    this.inputStream = inputStream;
    this.parallelism = inputStream.getParallelism();
    this.streamingContext = this.inputStream.getStreamingContext();
    this.operator = streamOperator;
    this.id = streamingContext.generateId();
    this.partition = selectPartition();
  }

  /**
   * Create a reference of referenced stream.
   * Changes in new stream will be reflected in referenced stream and vice versa
   */
  protected Stream(Stream referencedStream) {
    this.referencedStream = referencedStream;
  }

  @SuppressWarnings("unchecked")
  private Partition<T> selectPartition() {
    switch (operator.getLanguage()) {
      case PYTHON:
        return PythonPartition.RoundRobinPartition;
      case JAVA:
        return new RoundRobinPartition<>();
      default:
        throw new UnsupportedOperationException(
            "Unsupported language " + operator.getLanguage());
    }
  }

  public Stream getInputStream() {
    return referencedStream != null ? referencedStream.getInputStream() : inputStream;
  }

  public StreamOperator getOperator() {
    return referencedStream != null ? referencedStream.getOperator() : operator;
  }

  public StreamingContext getStreamingContext() {
    return referencedStream != null ? referencedStream.getStreamingContext() : streamingContext;
  }

  public int getParallelism() {
    return referencedStream != null ? referencedStream.getParallelism() : parallelism;
  }

  @SuppressWarnings("unchecked")
  private S self() {
    return (S) this;
  }

  public S setParallelism(int parallelism) {
    if (referencedStream != null) {
      referencedStream.setParallelism(parallelism);
    } else {
      this.parallelism = parallelism;
    }
    return self();
  }

  public int getId() {
    return referencedStream != null ? referencedStream.getId() : id;
  }

  public Partition<T> getPartition() {
    return referencedStream != null ? referencedStream.getPartition() : partition;
  }

  protected void setPartition(Partition<T> partition) {
    if (referencedStream != null) {
      referencedStream.setPartition(partition);
    } else {
      this.partition = partition;
    }
  }

  public boolean isReferenceStream() {
    return referencedStream != null;
  }

  public Stream getReferencedStream() {
    Preconditions.checkArgument(isReferenceStream());
    return referencedStream;
  }

  public abstract Language getLanguage();
}
