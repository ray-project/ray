package org.ray.api.test;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import com.google.common.collect.ImmutableMap;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.TestUtils;
import org.ray.api.TestUtils.LargeObject;
import org.ray.api.TestUtils.TestLock;
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
      Map<ObjectId, long[]> actual =
          ((NativeObjectStore) TestUtils.getRuntime().getObjectStore()).getAllReferenceCounts();
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

  // Return a large object that will be spilled to plasma.
  public static LargeObject getLargeObject() {
    return new TestUtils.LargeObject();
  }

  public static Object oneDepencency(RayObject<?> obj, TestLock testLock) {
    if (testLock != null) {
      testLock.waitLock();
    }
    return null;
  }

  public void testDirectCallRefCount() {
    // Multiple gets should not hang with ref counting enabled.
    RayObject<Integer> x = Ray.call(ReferenceCountingTest::foo, 2);
    x.get();
    x.get();

    // Temporary objects should be retained for chained callers.
    RayObject<Integer> y =
        Ray.call(ReferenceCountingTest::foo, Ray.call(ReferenceCountingTest::sleep));
    Assert.assertEquals(y.get(), Integer.valueOf(2));
  }

  public void testLocalRefCounts() {
    RayObject<Object> obj1 = Ray.put(null);
    checkRefCounts(ImmutableMap.of(obj1.getId(), new long[] {1, 0}));
    RayObject<Object> obj1Copy = new RayObjectImpl<>(obj1.getId());
    checkRefCounts(ImmutableMap.of(obj1.getId(), new long[] {2, 0}));

    ((RayObjectImpl<?>) obj1).removeLocalReference();
    checkRefCounts(ImmutableMap.of(obj1.getId(), new long[] {1, 0}));
    ((RayObjectImpl<?>) obj1Copy).removeLocalReference();
    checkRefCounts(ImmutableMap.of());
  }

  public void testDependencyRefCounts() {
    // Test that regular plasma dependency refcounts are decremented once the
    // task finishes.
    RayObject<LargeObject> largeDepencency;
    RayObject<Object> result;
    RayObject<Object> dependency;
    try (TestLock testLock = TestUtils.newLock()) {
      largeDepencency = Ray.put(getLargeObject());
      result = Ray.call(ReferenceCountingTest::oneDepencency, largeDepencency, testLock);
      checkRefCounts(ImmutableMap.of(largeDepencency.getId(), new long[] {1, 1}, result.getId(),
          new long[] {1, 0}));
    }
    // Reference count should be removed once the task finishes.
    checkRefCounts(ImmutableMap.of(largeDepencency.getId(), new long[] {1, 0}, result.getId(),
        new long[] {1, 0}));
    ((RayObjectImpl<?>) largeDepencency).removeLocalReference();
    ((RayObjectImpl<?>) result).removeLocalReference();
    checkRefCounts(ImmutableMap.of());

    // Test that inlined dependency refcounts are decremented once they are
    // inlined.
    try (TestLock testLock = TestUtils.newLock()) {
      dependency =
          Ray.call(ReferenceCountingTest::oneDepencency, (RayObject<Object>) null, testLock);
      checkRefCounts(ImmutableMap.of(dependency.getId(), new long[] {1, 0}));
      result = Ray.call(ReferenceCountingTest::oneDepencency, dependency, (TestLock) null);
      checkRefCounts(ImmutableMap.of(dependency.getId(), new long[] {1, 1}, result.getId(),
          new long[] {1, 0}));
    }
    // Reference count should be removed as soon as the dependency is inlined.
    checkRefCounts(
        ImmutableMap.of(dependency.getId(), new long[] {1, 0}, result.getId(), new long[] {1, 0}),
        Duration.ofSeconds(1));
    ((RayObjectImpl<?>) dependency).removeLocalReference();
    ((RayObjectImpl<?>) result).removeLocalReference();
    checkRefCounts(ImmutableMap.of());
  }

  public void testBasicNestedIds() {
    RayObject<byte[]> inner = Ray.put(new byte[40 * 1024 * 1024]);
    ObjectId innerId = inner.getId();
    checkRefCounts(ImmutableMap.of(innerId, new long[] {1, 0}));

    RayObject<List<RayObject<byte[]>>> outer = Ray.put(Collections.singletonList(inner));
    checkRefCounts(ImmutableMap.of(innerId, new long[] {2, 0},
            outer.getId(), new long[] {1, 0}));

    ((RayObjectImpl) inner).removeLocalReference();
    checkRefCounts(ImmutableMap.of(innerId, new long[] {1, 0},
            outer.getId(), new long[] {1, 0}));

    inner = new RayObjectImpl<>(innerId);
    Assert.assertNotNull(inner.get());
  }
}
