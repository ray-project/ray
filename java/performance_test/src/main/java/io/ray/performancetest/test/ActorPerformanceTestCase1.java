package io.ray.performancetest.test;

/**
 * 1-to-1 ray call, one receiving actor and one sending actor, test the throughput of the engine
 * itself.
 */
public class ActorPerformanceTestCase1 {

  public static void main(String[] args) {
    final int[] layers = new int[] {1, 1};
    final int[] actorsPerLayer = new int[] {1, 1};
    final boolean hasReturn = false;
    final int argSize = 0;
    final boolean useDirectByteBuffer = false;
    final boolean ignoreReturn = false;
    ActorPerformanceTestBase.run(
        args,
        layers,
        actorsPerLayer,
        hasReturn,
        ignoreReturn,
        argSize,
        useDirectByteBuffer);
  }
}
