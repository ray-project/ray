package io.ray.serialization;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import io.ray.serialization.bean.BeanA;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;

public class ThreadSafeFuryTest {
  private volatile boolean hasException;

  @Test
  public void testSerialize() throws InterruptedException {
    BeanA beanA = BeanA.createBeanA(2);
    ThreadSafeFury fury = Fury.builder().withReferenceTracking(true).buildThreadSafeFury();
    ExecutorService executorService = Executors.newFixedThreadPool(12);
    for (int i = 0; i < 2000; i++) {
      executorService.execute(
          () -> {
            for (int j = 0; j < 10; j++) {
              try {
                assertEquals(fury.deserialize(fury.serialize(beanA)), beanA);
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
