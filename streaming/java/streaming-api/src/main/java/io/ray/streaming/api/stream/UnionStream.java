package io.ray.streaming.api.stream;

import io.ray.streaming.operator.StreamOperator;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a union DataStream.
 *
 * @param <T> The type of union data.
 */
public class UnionStream<T> extends DataStream<T> {

  private List<DataStream<T>> unionStreams;

  public UnionStream(DataStream<T> input, StreamOperator streamOperator, DataStream<T> other) {
    super(input, streamOperator);
    this.unionStreams = new ArrayList<>();
    this.unionStreams.add(other);
  }

  public List<DataStream<T>> getUnionStreams() {
    return unionStreams;
  }
}
