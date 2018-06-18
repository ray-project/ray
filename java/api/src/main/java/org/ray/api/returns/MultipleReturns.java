package org.ray.api.returns;

/**
 * Multiple return objects for user's method.
 */
public class MultipleReturns {

  protected final Object[] values;

  public MultipleReturns(Object[] values) {
    this.values = values;
  }

  public Object[] getValues() {
    return values;
  }

}
