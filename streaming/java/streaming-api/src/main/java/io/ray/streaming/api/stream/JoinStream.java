package io.ray.streaming.api.stream;

import io.ray.streaming.api.function.impl.JoinFunction;
import io.ray.streaming.api.function.impl.KeyFunction;
import java.io.Serializable;

/**
 * Represents a DataStream of two joined DataStream.
 *
 * @param <L> Type of the data in the left stream.
 * @param <R> Type of the data in the right stream.
 * @param <J> Type of the data in the joined stream.
 */
public class JoinStream<L, R, J> extends DataStream<L> {

  public JoinStream(DataStream<L> leftStream, DataStream<R> rightStream) {
    super(leftStream, null);
  }

  /**
   * Apply key-by to the left join stream.
   */
  public <K> Where<L, R, J, K> where(KeyFunction<L, K> keyFunction) {
    return new Where<>(this, keyFunction);
  }

  /**
   * Where clause of the join transformation.
   *
   * @param <L> Type of the data in the left stream.
   * @param <R> Type of the data in the right stream.
   * @param <J> Type of the data in the joined stream.
   * @param <K> Type of the join key.
   */
  class Where<L, R, J, K> implements Serializable {

    private JoinStream<L, R, J> joinStream;
    private KeyFunction<L, K> leftKeyByFunction;

    public Where(JoinStream<L, R, J> joinStream, KeyFunction<L, K> leftKeyByFunction) {
      this.joinStream = joinStream;
      this.leftKeyByFunction = leftKeyByFunction;
    }

    public Equal<L, R, J, K> equalLo(KeyFunction<R, K> rightKeyFunction) {
      return new Equal<>(joinStream, leftKeyByFunction, rightKeyFunction);
    }
  }

  /**
   * Equal clause of the join transformation.
   *
   * @param <L> Type of the data in the left stream.
   * @param <R> Type of the data in the right stream.
   * @param <J> Type of the data in the joined stream.
   * @param <K> Type of the join key.
   */
  class Equal<L, R, J, K> implements Serializable {

    private JoinStream<L, R, J> joinStream;
    private KeyFunction<L, K> leftKeyByFunction;
    private KeyFunction<R, K> rightKeyByFunction;

    public Equal(JoinStream<L, R, J> joinStream, KeyFunction<L, K> leftKeyByFunction,
                 KeyFunction<R, K> rightKeyByFunction) {
      this.joinStream = joinStream;
      this.leftKeyByFunction = leftKeyByFunction;
      this.rightKeyByFunction = rightKeyByFunction;
    }

    public DataStream<J> with(JoinFunction<L, R, J> joinFunction) {
      return (DataStream<J>) joinStream;
    }
  }

}
