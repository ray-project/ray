package org.ray.streaming.api.stream;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import org.ray.streaming.api.Language;
import org.ray.streaming.api.context.StreamingContext;
import org.ray.streaming.api.partition.Partition;
import org.ray.streaming.api.partition.impl.RoundRobinPartition;
import org.ray.streaming.operator.Operator;
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
  private final int id;
  private final StreamingContext streamingContext;
  private final Stream inputStream;
  private final StreamOperator operator;
  private int parallelism = 1;
  private Partition<T> partition;
  private Stream referencedStream;

  public Stream(StreamingContext streamingContext, StreamOperator streamOperator) {
    this(streamingContext, null, streamOperator,
         selectPartition(streamOperator));
  }

  public Stream(StreamingContext streamingContext,
                StreamOperator streamOperator,
                Partition<T> partition) {
    this(streamingContext, null, streamOperator, partition);
  }

  public Stream(Stream inputStream, StreamOperator streamOperator) {
    this(inputStream.getStreamingContext(), inputStream, streamOperator,
         selectPartition(streamOperator));
  }

  public Stream(Stream inputStream, StreamOperator streamOperator, Partition<T> partition) {
    this(inputStream.getStreamingContext(), inputStream, streamOperator, partition);
  }

  protected Stream(StreamingContext streamingContext,
                Stream inputStream,
                StreamOperator streamOperator,
                Partition<T> partition) {
    this.streamingContext = streamingContext;
    this.inputStream = inputStream;
    this.operator = streamOperator;
    this.partition = partition;
    this.id = streamingContext.generateId();
    if (inputStream != null) {
      this.parallelism = inputStream.getParallelism();
    }
  }

  /**
   * Create a reference of referenced stream.
   * Changes in new stream will be reflected in referenced stream and vice versa
   */
  protected Stream(Stream referencedStream) {
    this.referencedStream = referencedStream;
    this.id = referencedStream.getId();
    this.streamingContext = referencedStream.getStreamingContext();
    this.inputStream = referencedStream.getInputStream();
    this.operator = referencedStream.getOperator();
  }

  @SuppressWarnings("unchecked")
  private static <T> Partition<T> selectPartition(Operator operator) {
    switch (operator.getLanguage()) {
      case PYTHON:
        return (Partition<T>) PythonPartition.RoundRobinPartition;
      case JAVA:
        return new RoundRobinPartition<>();
      default:
        throw new UnsupportedOperationException(
            "Unsupported language " + operator.getLanguage());
    }
  }

  public int getId() {
    return id;
  }

  public StreamingContext getStreamingContext() {
    return streamingContext;
  }

  public Stream getInputStream() {
    return inputStream;
  }

  public StreamOperator getOperator() {
    return operator;
  }

  @SuppressWarnings("unchecked")
  private S self() {
    return (S) this;
  }

  public int getParallelism() {
    return referencedStream != null ? referencedStream.getParallelism() : parallelism;
  }

  public S setParallelism(int parallelism) {
    if (referencedStream != null) {
      referencedStream.setParallelism(parallelism);
    } else {
      this.parallelism = parallelism;
    }
    return self();
  }

  @SuppressWarnings("unchecked")
  public Partition<T> getPartition() {
    return referencedStream != null ? referencedStream.getPartition() : partition;
  }

  @SuppressWarnings("unchecked")
  protected S setPartition(Partition<T> partition) {
    if (referencedStream != null) {
      referencedStream.setPartition(partition);
    } else {
      this.partition = partition;
    }
    return self();
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
