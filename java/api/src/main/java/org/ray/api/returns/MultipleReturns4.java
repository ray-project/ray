package org.ray.api.returns;

@SuppressWarnings("unchecked")
public class MultipleReturns4<R0, R1, R2, R3> extends MultipleReturns {

  public MultipleReturns4(R0 r0, R1 r1, R2 r2, R3 r3) {
    super(new Object[] {r0, r1, r2, r3});
  }

  public R0 get0() {
    return (R0) this.values[0];
  }

  public R1 get1() {
    return (R1) this.values[1];
  }

  public R2 get2() {
    return (R2) this.values[2];
  }

  public R3 get3() {
    return (R3) this.values[3];
  }
}
