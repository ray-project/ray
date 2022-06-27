package io.ray.test;

import com.google.common.base.Preconditions;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.runtime.AbstractRayRuntime;
import io.ray.runtime.config.RayConfig;
import io.ray.runtime.config.RunMode;
import io.ray.runtime.task.ArgumentsBuilder;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import org.testng.Assert;

public class TestUtils {

  public static class LargeObject implements Serializable {

    public byte[] data;

    public LargeObject() {
      this(1024 * 1024);
    }

    public LargeObject(int size) {
      Preconditions.checkState(size > ArgumentsBuilder.LARGEST_SIZE_PASS_BY_VALUE);
      data = new byte[size];
    }
  }

  private static final int WAIT_INTERVAL_MS = 5;

  public static boolean isLocalMode() {
    return getRuntime().getRayConfig().runMode == RunMode.LOCAL;
  }

  /**
   * Assert that the given runnable finishes within given timeout.
   *
   * @param runnable A runnable that should finish within given timeout.
   * @param timeoutMs Timeout in milliseconds.
   */
  public static void executeWithinTime(Runnable runnable, int timeoutMs) {
    executeWithinTimeRange(runnable, 0, timeoutMs);
  }

  /**
   * Assert that the given runnable finishes within given time range.
   *
   * @param runnable A runnable that should finish within given timeout.
   * @param minTimeMs The minimum time for execution.
   * @param maxTimeMs The maximum time for execution.
   */
  public static void executeWithinTimeRange(Runnable runnable, int minTimeMs, int maxTimeMs) {
    Instant start = Instant.now();
    runnable.run();
    Instant end = Instant.now();
    long duration = Duration.between(start, end).toMillis();
    Assert.assertTrue(
        duration >= minTimeMs,
        "The given runnable didn't run for at least "
            + minTimeMs
            + "ms. "
            + "Actual execution time: "
            + duration
            + " ms.");
    Assert.assertTrue(
        duration <= maxTimeMs,
        "The given runnable didn't finish within "
            + maxTimeMs
            + "ms. "
            + "Actual execution time: "
            + duration
            + " ms.");
  }

  /**
   * Wait until the given condition is met.
   *
   * @param condition A function that predicts the condition.
   * @param timeoutMs Timeout in milliseconds.
   * @return True if the condition is met within the timeout, false otherwise.
   */
  public static boolean waitForCondition(Supplier<Boolean> condition, int timeoutMs) {
    long endTime = System.currentTimeMillis() + timeoutMs;
    while (true) {
      if (condition.get()) {
        return true;
      }

      if (System.currentTimeMillis() >= endTime) {
        break;
      }
      try {
        java.util.concurrent.TimeUnit.MILLISECONDS.sleep(WAIT_INTERVAL_MS);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    return false;
  }

  private static String hi() {
    return "hi";
  }

  /**
   * Warm up the cluster to make sure there's at least one idle worker.
   *
   * <p>This is needed before calling `wait`. Because, in Travis CI, starting a new worker process
   * could be slower than the wait timeout.
   *
   * <p>TODO(hchen): We should consider supporting always reversing a certain number of idle workers
   * in Raylet's worker pool.
   */
  public static void warmUpCluster() {
    ObjectRef<String> obj = Ray.task(TestUtils::hi).remote();
    Assert.assertEquals(obj.get(), "hi");
  }

  public static AbstractRayRuntime getRuntime() {
    return (AbstractRayRuntime) Ray.internal();
  }

  public static ProcessBuilder buildDriver(Class<?> mainClass, String[] args) {
    RayConfig rayConfig = TestUtils.getRuntime().getRayConfig();

    List<String> fullArgs = new ArrayList<>();
    fullArgs.add("java");
    fullArgs.add("-cp");
    fullArgs.add(System.getProperty("java.class.path"));
    fullArgs.add("-Dray.address=" + rayConfig.getBootstrapAddress());
    fullArgs.add("-Dray.object-store.socket-name=" + rayConfig.objectStoreSocketName);
    fullArgs.add("-Dray.raylet.socket-name=" + rayConfig.rayletSocketName);
    fullArgs.add("-Dray.raylet.node-manager-port=" + rayConfig.getNodeManagerPort());
    fullArgs.add(mainClass.getName());
    if (args != null) {
      fullArgs.addAll(Arrays.asList(args));
    }

    return new ProcessBuilder(fullArgs);
  }
}
