package io.ray.streaming.api.function.internal;

import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.api.function.Function;
import io.ray.streaming.api.function.RichFunction;

/** A util class for {@link Function} */
public class Functions {

  private static class DefaultRichFunction implements RichFunction {

    private final Function function;

    private DefaultRichFunction(Function function) {
      this.function = function;
    }

    @Override
    public void open(RuntimeContext runtimeContext) {}

    @Override
    public void close() {}

    public Function getFunction() {
      return function;
    }
  }

  public static RichFunction wrap(Function function) {
    if (function instanceof RichFunction) {
      return (RichFunction) function;
    } else {
      return new DefaultRichFunction(function);
    }
  }

  public static RichFunction emptyFunction() {
    return new DefaultRichFunction(null);
  }
}
