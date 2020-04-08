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

  private List<DataStream> unionStreams;

  public UnionStream(DataStream input, StreamOperator streamOperator, DataStream<T> other) {
    super(input, streamOperator);
    this.unionStreams = new ArrayList<>();
    this.unionStreams.add(other);
  }

  public List<DataStream> getUnionStreams() {
    return unionStreams;
  }
}
