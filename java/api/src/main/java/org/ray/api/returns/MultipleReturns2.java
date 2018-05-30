package org.ray.api.returns;

@SuppressWarnings("unchecked")
public class MultipleReturns2<R0, R1> extends MultipleReturns {

  public MultipleReturns2(R0 r0, R1 r1) {
    super(new Object[]{r0, r1});
  }

  public R0 get0() {
    return (R0) this.values[0];
  }

  public R1 get1() {
    return (R1) this.values[1];
  }
}
