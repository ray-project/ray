package io.ray.benchmark;

import io.ray.api.Ray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MicroBenchmarks {

  private static final Logger LOGGER = LoggerFactory.getLogger(MicroBenchmarks.class);

  public static Object simpleFunction() {
    return null;
  }

  private static void time(Runnable runnable, int numRepeats, String name) {
    LOGGER.info("Benchmark \"{}\" started.", name);
    final long start = System.nanoTime();
    for (int i = 0; i < numRepeats; i++) {
      runnable.run();
    }
    final long duration = System.nanoTime() - start;
    LOGGER.info(
        "Benchmark \"{}\" finished, repeated {} times, total duration {} ms,"
            + " average duration {} ns.",
        name,
        numRepeats,
        duration / 1_000_000,
        duration / numRepeats);
  }

  /**
   * Benchmark task submission.
   *
   * <p>Note, this benchmark is supposed to measure the elapased time in Java worker, we should
   * disable submitting tasks to raylet in `raylet_client.cc` before running this benchmark.
   */
  public static void benchmarkTaskSubmission() {
    final int numRepeats = 1_000_000;
    Ray.init();
    try {
      time(
          () -> {
            Ray.task(MicroBenchmarks::simpleFunction).remote();
          },
          numRepeats,
          "task submission");
    } finally {
      Ray.shutdown();
    }
  }

  public static void main(String[] args) {
    benchmarkTaskSubmission();
  }
}
