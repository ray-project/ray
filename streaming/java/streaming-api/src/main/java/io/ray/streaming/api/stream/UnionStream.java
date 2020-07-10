package io.ray.streaming.api.stream;

import io.ray.streaming.operator.impl.UnionOperator;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a union DataStream.
 *
 * <p>This stream does not create a physical operation, it only affects how upstream data are
 *  connected to downstream data.
 *
 * @param <T> The type of union data.
 */
public class UnionStream<T> extends DataStream<T> {
  private List<DataStream<T>> unionStreams;

  public UnionStream(DataStream<T> input, List<DataStream<T>> streams) {
    // Union stream does not create a physical operation, so we don't have to set partition
    // function for it.
    super(input, new UnionOperator());
    this.unionStreams = new ArrayList<>();
    streams.forEach(this::addStream);
  }

  void addStream(DataStream<T> stream) {
    if (stream instanceof UnionStream) {
      this.unionStreams.addAll(((UnionStream<T>) stream).getUnionStreams());
    } else {
      this.unionStreams.add(stream);
    }
  }

  public List<DataStream<T>> getUnionStreams() {
    return unionStreams;
  }
}
