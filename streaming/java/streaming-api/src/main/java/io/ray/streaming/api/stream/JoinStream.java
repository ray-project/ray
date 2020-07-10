package io.ray.streaming.api.stream;

import io.ray.streaming.api.function.impl.JoinFunction;
import io.ray.streaming.api.function.impl.KeyFunction;
import io.ray.streaming.operator.impl.JoinOperator;
import java.io.Serializable;

/**
 * Represents a DataStream of two joined DataStream.
 *
 * @param <L> Type of the data in the left stream.
 * @param <R> Type of the data in the right stream.
 * @param <O> Type of the data in the joined stream.
 */
public class JoinStream<L, R, O> extends DataStream<L> {
  private final DataStream<R> rightStream;

  public JoinStream(DataStream<L> leftStream, DataStream<R> rightStream) {
    super(leftStream, new JoinOperator<>());
    this.rightStream = rightStream;
  }

  public DataStream<R> getRightStream() {
    return rightStream;
  }

  /**
   * Apply key-by to the left join stream.
   */
  public <K> Where<K> where(KeyFunction<L, K> keyFunction) {
    return new Where<>(this, keyFunction);
  }

  /**
   * Where clause of the join transformation.
   *
   * @param <K> Type of the join key.
   */
  class Where<K> implements Serializable {
    private JoinStream<L, R, O> joinStream;
    private KeyFunction<L, K> leftKeyByFunction;

    Where(JoinStream<L, R, O> joinStream, KeyFunction<L, K> leftKeyByFunction) {
      this.joinStream = joinStream;
      this.leftKeyByFunction = leftKeyByFunction;
    }

    public Equal<K> equalTo(KeyFunction<R, K> rightKeyFunction) {
      return new Equal<>(joinStream, leftKeyByFunction, rightKeyFunction);
    }
  }

  /**
   * Equal clause of the join transformation.
   *
   * @param <K> Type of the join key.
   */
  class Equal<K> implements Serializable {
    private JoinStream<L, R, O> joinStream;
    private KeyFunction<L, K> leftKeyByFunction;
    private KeyFunction<R, K> rightKeyByFunction;

    Equal(JoinStream<L, R, O> joinStream, KeyFunction<L, K> leftKeyByFunction,
          KeyFunction<R, K> rightKeyByFunction) {
      this.joinStream = joinStream;
      this.leftKeyByFunction = leftKeyByFunction;
      this.rightKeyByFunction = rightKeyByFunction;
    }

    @SuppressWarnings("unchecked")
    public DataStream<O> with(JoinFunction<L, R, O> joinFunction) {
      JoinOperator joinOperator = (JoinOperator) joinStream.getOperator();
      joinOperator.setFunction(joinFunction);
      return (DataStream<O>) joinStream;
    }
  }

}
