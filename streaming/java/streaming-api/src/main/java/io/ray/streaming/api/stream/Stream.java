package io.ray.streaming.api.stream;

import io.ray.streaming.api.context.StreamingContext;
import io.ray.streaming.api.partition.Partition;
import io.ray.streaming.api.partition.impl.RoundRobinPartition;
import io.ray.streaming.operator.StreamOperator;
import io.ray.streaming.python.PythonOperator;
import io.ray.streaming.python.PythonPartition;
import io.ray.streaming.python.stream.PythonStream;
import java.io.Serializable;

/**
 * Abstract base class of all stream types.
 *
 * @param <T> Type of the data in the stream.
 */
public abstract class Stream<T> implements Serializable {
  protected int id;
  protected int parallelism = 1;
  protected StreamOperator operator;
  protected Stream<T> inputStream;
  protected StreamingContext streamingContext;
  protected Partition<T> partition;

  @SuppressWarnings("unchecked")
  public Stream(StreamingContext streamingContext, StreamOperator streamOperator) {
    this.streamingContext = streamingContext;
    this.operator = streamOperator;
    this.id = streamingContext.generateId();
    if (streamOperator instanceof PythonOperator) {
      this.partition = PythonPartition.RoundRobinPartition;
    } else {
      this.partition = new RoundRobinPartition<>();
    }
  }

  public Stream(Stream<T> inputStream, StreamOperator streamOperator) {
    this.inputStream = inputStream;
    this.parallelism = inputStream.getParallelism();
    this.streamingContext = this.inputStream.getStreamingContext();
    this.operator = streamOperator;
    this.id = streamingContext.generateId();
    this.partition = selectPartition();
  }

  @SuppressWarnings("unchecked")
  private Partition<T> selectPartition() {
    if (inputStream instanceof PythonStream) {
      return PythonPartition.RoundRobinPartition;
    } else {
      return new RoundRobinPartition<>();
    }
  }

  public Stream<T> getInputStream() {
    return inputStream;
  }

  public StreamOperator getOperator() {
    return operator;
  }

  public void setOperator(StreamOperator operator) {
    this.operator = operator;
  }

  public StreamingContext getStreamingContext() {
    return streamingContext;
  }

  public int getParallelism() {
    return parallelism;
  }

  public Stream<T> setParallelism(int parallelism) {
    this.parallelism = parallelism;
    return this;
  }

  public int getId() {
    return id;
  }

  public Partition<T> getPartition() {
    return partition;
  }

  public void setPartition(Partition<T> partition) {
    this.partition = partition;
  }
}
