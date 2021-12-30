package io.ray.runtime.serialization;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;

public class ThreadSafeRaySerdeTest {
  private volatile boolean hasException;

  @Test
  public void testSerialize() throws InterruptedException {
    ComplexObjects.BeanA beanA = ComplexObjects.BeanA.createBeanA(2);
    ThreadSafeRaySerde serde =
        RaySerde.builder().withReferenceTracking(true).buildThreadSafeSerde();
    ExecutorService executorService = Executors.newFixedThreadPool(12);
    for (int i = 0; i < 2000; i++) {
      executorService.execute(
          () -> {
            for (int j = 0; j < 10; j++) {
              try {
                assertEquals(serde.deserialize(serde.serialize(beanA)), beanA);
              } catch (Throwable e) {
                hasException = true;
                e.printStackTrace();
                throw e;
              }
            }
          });
    }
    executorService.shutdown();
    assertTrue(executorService.awaitTermination(30, TimeUnit.SECONDS));
    assertFalse(hasException);
  }
}
