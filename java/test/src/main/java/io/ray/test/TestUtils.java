package io.ray.test;

import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.runtime.RayRuntimeInternal;
import io.ray.runtime.RayRuntimeProxy;
import io.ray.runtime.config.RunMode;
import java.io.Serializable;
import java.util.function.Supplier;
import org.testng.Assert;

public class TestUtils {

  public static class LargeObject implements Serializable {

    public byte[] data = new byte[1024 * 1024];
  }

  private static final int WAIT_INTERVAL_MS = 5;

  public static boolean isSingleProcessMode() {
    return getRuntime().getRayConfig().runMode == RunMode.SINGLE_PROCESS;
  }

  /**
   * Wait until the given condition is met.
   *
   * @param condition A function that predicts the condition.
   * @param timeoutMs Timeout in milliseconds.
   * @return True if the condition is met within the timeout, false otherwise.
   */
  public static boolean waitForCondition(Supplier<Boolean> condition, int timeoutMs) {
    int waitTime = 0;
    while (true) {
      if (condition.get()) {
        return true;
      }

      try {
        java.util.concurrent.TimeUnit.MILLISECONDS.sleep(WAIT_INTERVAL_MS);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      waitTime += WAIT_INTERVAL_MS;
      if (waitTime > timeoutMs) {
        break;
      }
    }
    return false;
  }

  private static String hi() {
    return "hi";
  }

  /**
   * Warm up the cluster to make sure there's at least one idle worker.
   * <p>
   * This is needed before calling `wait`. Because, in Travis CI, starting a new worker
   * process could be slower than the wait timeout.
   * TODO(hchen): We should consider supporting always reversing a certain number of
   * idle workers in Raylet's worker pool.
   */
  public static void warmUpCluster() {
    ObjectRef<String> obj = Ray.task(TestUtils::hi).remote();
    Assert.assertEquals(obj.get(), "hi");
  }

  public static RayRuntimeInternal getRuntime() {
    return (RayRuntimeInternal) Ray.internal();
  }

  public static RayRuntimeInternal getUnderlyingRuntime() {
    RayRuntimeProxy proxy = (RayRuntimeProxy) (java.lang.reflect.Proxy
        .getInvocationHandler(Ray.internal()));
    return proxy.getRuntimeObject();
  }
}
