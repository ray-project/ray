package org.ray.api.test;

import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.TestUtils;
import org.ray.api.id.ObjectId;
import org.ray.runtime.object.NativeObjectStore;
import org.ray.runtime.object.RayObjectImpl;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test
public class ReferenceCountingTest extends BaseTest {

  @BeforeMethod
  public void setUp() {
    TestUtils.skipTestUnderSingleProcess();
    TestUtils.skipTestIfDirectActorCallDisabled();
  }

  private void checkRefCounts(Map<ObjectId, long[]> expected, Duration timeout) {
    Instant start = Instant.now();
    while (true) {
      Map<ObjectId, long[]> actual = ((NativeObjectStore) TestUtils.getRuntime().getObjectStore())
          .getAllReferenceCounts();
      try {
        Assert.assertEquals(actual, expected);
      } catch (AssertionError e) {
        if (Duration.between(start, Instant.now()).compareTo(timeout) >= 0) {
          throw e;
        } else {
          try {
            Thread.sleep(100);
          } catch (InterruptedException ex) {
            throw new RuntimeException(e);
          }
        }
      }
    }
  }

  private void checkRefCounts(Map<ObjectId, long[]> expected) {
    checkRefCounts(expected, Duration.ofSeconds(10));
  }

  public static int foo(int x) {
    return x + 1;
  }

  public static int sleep() {
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return 1;
  }

  public void testDirectCallRefCount() {
    // Multiple gets should not hang with ref counting enabled.
    RayObject<Integer> x = Ray.call(ReferenceCountingTest::foo, 2);
    x.get();
    x.get();

    // Temporary objects should be retained for chained callers.
    RayObject<Integer> y = Ray
        .call(ReferenceCountingTest::foo, Ray.call(ReferenceCountingTest::sleep));
    Assert.assertEquals(y.get(), Integer.valueOf(2));
  }

  public void testLocalRefCounts() {
    RayObject<Object> obj1 = Ray.put(null);
    checkRefCounts(ImmutableMap.of(obj1.getId(), new long[]{1, 0}));
    RayObject<Object> obj1Copy = new RayObjectImpl<>(obj1.getId());
    checkRefCounts(ImmutableMap.of(obj1.getId(), new long[]{2, 0}));

    ((RayObjectImpl)obj1).removeLocalReference();
    checkRefCounts(ImmutableMap.of(obj1.getId(), new long[]{1, 0}));
    ((RayObjectImpl)obj1Copy).removeLocalReference();
    checkRefCounts(ImmutableMap.of());
  }
}
