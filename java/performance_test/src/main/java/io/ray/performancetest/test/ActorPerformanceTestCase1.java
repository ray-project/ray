package io.ray.performancetest.test;

/** 1对1单向ray call，一收一发，测试引擎本身的吞吐. */
public class ActorPerformanceTestCase1 {

  public static void main(String[] args) {
    final int[] layers = new int[] {1, 1};
    final int[] actorsPerLayer = new int[] {1, 1};
    final boolean hasReturn = false;
    final int argSize = 0;
    final boolean useDirectByteBuffer = false;
    final boolean ignoreReturn = false;
    final int numJavaWorkerPerProcess = 1;
    ActorPerformanceTestBase.run(
        args,
        layers,
        actorsPerLayer,
        hasReturn,
        ignoreReturn,
        argSize,
        useDirectByteBuffer,
        numJavaWorkerPerProcess);
  }
}
