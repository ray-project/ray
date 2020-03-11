package org.ray.streaming.api.stream;

import java.io.Serializable;
import org.ray.streaming.api.context.StreamContext;
import org.ray.streaming.api.partition.Partition;
import org.ray.streaming.api.partition.impl.RoundRobinPartition;
import org.ray.streaming.operator.StreamOperator;
import org.ray.streaming.python.PythonOperator;
import org.ray.streaming.python.PythonPartition;
import org.ray.streaming.python.stream.PythonStream;

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
  protected StreamContext streamContext;
  protected Partition<T> partition;

  @SuppressWarnings("unchecked")
  public Stream(StreamContext streamContext, StreamOperator streamOperator) {
    this.streamContext = streamContext;
    this.operator = streamOperator;
    this.id = streamContext.generateId();
    if (streamOperator instanceof PythonOperator) {
      this.partition = PythonPartition.RoundRobinPartition;
    } else {
      this.partition = new RoundRobinPartition<>();
    }
  }

  public Stream(Stream<T> inputStream, StreamOperator streamOperator) {
    this.inputStream = inputStream;
    this.parallelism = inputStream.getParallelism();
    this.streamContext = this.inputStream.getStreamContext();
    this.operator = streamOperator;
    this.id = streamContext.generateId();
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

  public StreamContext getStreamContext() {
    return streamContext;
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
