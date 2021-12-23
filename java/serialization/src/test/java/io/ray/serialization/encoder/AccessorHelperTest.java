package io.ray.serialization.encoder;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import io.ray.serialization.bean.Foo;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testng.annotations.Test;

public class AccessorHelperTest {
  public static class A {
    protected String f1;
    String f2;
  }

  @Test
  public void genCode() {
    AccessorHelper.genCode(A.class);
  }

  @Test
  public void defineAccessorClass() {
    assertTrue(AccessorHelper.defineAccessorClass(A.class));
    Class<?> aClass = AccessorHelper.getAccessorClass(A.class);
    assertEquals(aClass.getClassLoader(), A.class.getClassLoader());
    try {
      A a = new A();
      a.f1 = "str";
      a.f2 = "str";
      Method f1 = aClass.getDeclaredMethod("f1", A.class);
      Method f2 = aClass.getDeclaredMethod("f2", A.class);
      assertEquals(f1.invoke(null, a), a.f1);
      assertEquals(f2.invoke(null, a), a.f2);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new IllegalStateException(e);
    }
  }

  @Test
  public void defineAccessorClassConcurrent() throws InterruptedException {
    ExecutorService executorService = Executors.newFixedThreadPool(10);
    AtomicBoolean hasException = new AtomicBoolean(false);
    for (int i = 0; i < 1000; i++) {
      executorService.execute(
          () -> {
            try {
              assertTrue(AccessorHelper.defineAccessorClass(A.class));
              assertTrue(AccessorHelper.defineAccessorClass(Foo.class));
            } catch (Exception e) {
              hasException.set(true);
            }
          });
    }
    executorService.shutdown();
    assertTrue(executorService.awaitTermination(30, TimeUnit.SECONDS));
    assertFalse(hasException.get());
  }
}
