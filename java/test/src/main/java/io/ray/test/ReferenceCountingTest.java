package io.ray.test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.id.ObjectId;
import io.ray.runtime.object.NativeObjectStore;
import io.ray.runtime.object.ObjectRefImpl;
import io.ray.test.TestUtils.TestLock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test
public class ReferenceCountingTest extends BaseTest {
  @BeforeClass
  public void setUp() {
    System.setProperty("ray.object-store.size", "100 MB");
  }

  @AfterClass
  public void tearDown() {
    System.clearProperty("ray.object-store.size");
  }

  @BeforeMethod
  public void setUpCase() {
    TestUtils.skipTestUnderSingleProcess();
  }

  /**
   * Because we can't explicitly GC an Java object. We use this helper method to manually remove
   * an local reference.
   */
  private void del(ObjectRef<?> obj) {
    ((ObjectRefImpl<?>) obj).removeLocalReference();
  }

  private void checkRefCounts(Map<ObjectId, long[]> expected, Duration timeout) {
    Instant start = Instant.now();
    while (true) {
      Map<ObjectId, long[]> actual =
          ((NativeObjectStore) TestUtils.getRuntime().getObjectStore())
              .getAllReferenceCounts();
      try {
        Assert.assertEqualsDeep(actual, expected);
        return;
      } catch (AssertionError e) {
        if (Duration.between(start, Instant.now()).compareTo(timeout) >= 0) {
          System.out.println("Actual: " + new Gson().toJson(actual));
          System.out.println("Expected: " + new Gson().toJson(expected));
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

  private static int foo(int x) {
    return x + 1;
  }

  private static int sleep() {
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return 1;
  }

  private static Object oneDepencency(ObjectRef<?> obj, TestLock testLock) {
    if (testLock != null) {
      testLock.waitLock();
    }
    return null;
  }

  private static void fillObjectStoreAndGet(ObjectId objectId, boolean succeed) {
    fillObjectStoreAndGet(objectId, succeed, 40 * 1024 * 1024, 5);
  }

  private static void fillObjectStoreAndGet(ObjectId objectId, boolean succeed, int objectSize, int numObjects) {
    for (int i = 0; i < numObjects; i++) {
      Ray.put(new TestUtils.LargeObject(objectSize));
    }
    if (succeed) {
      TestUtils.getRuntime().getObjectStore().getRaw(ImmutableList.of(objectId), Long.MAX_VALUE);
    } else {
      List<Boolean> result = TestUtils.getRuntime().getObjectStore().wait(ImmutableList.of(objectId), 1, 100);
      Assert.assertFalse(result.get(0));
    }
  }

  /**
   * Based on Python test case `test_local_refcounts`.
   */
  public void testLocalRefCounts() {
    ObjectRef<Object> obj1 = Ray.put(null);
    checkRefCounts(ImmutableMap.of(obj1.getId(), new long[] {1, 0}));
    ObjectRef<Object> obj1Copy = new ObjectRefImpl<>(obj1.getId(), obj1.getType());
    checkRefCounts(ImmutableMap.of(obj1.getId(), new long[] {2, 0}));

    del(obj1);
    checkRefCounts(ImmutableMap.of(obj1.getId(), new long[] {1, 0}));
    del(obj1Copy);
    checkRefCounts(ImmutableMap.of());
  }

  /**
   * Based on Python test case `test_dependency_refcounts`.
   */
  public void testDependencyRefCounts() {
    // Test that regular plasma dependency refcounts are decremented once the
    // task finishes.
    ObjectRef<TestUtils.LargeObject> largeDepencency;
    ObjectRef<Object> result;
    ObjectRef<Object> dependency;
    try (TestLock testLock = TestUtils.newLock()) {
      largeDepencency = Ray.put(new TestUtils.LargeObject());
      result = Ray.task(ReferenceCountingTest::oneDepencency, largeDepencency, testLock)
                   .remote();
      checkRefCounts(ImmutableMap.of(
          largeDepencency.getId(), new long[] {1, 1}, result.getId(), new long[] {1, 0}));
    }
    // Reference count should be removed once the task finishes.
    checkRefCounts(ImmutableMap.of(
        largeDepencency.getId(), new long[] {1, 0}, result.getId(), new long[] {1, 0}));
    del(largeDepencency);
    del(result);
    checkRefCounts(ImmutableMap.of());

    // Test that inlined dependency refcounts are decremented once they are
    // inlined.
    try (TestLock testLock = TestUtils.newLock()) {
      dependency = Ray.task(ReferenceCountingTest::oneDepencency,
                          (ObjectRef<Object>) null, testLock)
                       .remote();
      checkRefCounts(ImmutableMap.of(dependency.getId(), new long[] {1, 0}));
      result = Ray.task(ReferenceCountingTest::oneDepencency, dependency, (TestLock) null)
                   .remote();
      checkRefCounts(ImmutableMap.of(
          dependency.getId(), new long[] {1, 1}, result.getId(), new long[] {1, 0}));
    }
    // Reference count should be removed as soon as the dependency is inlined.
    checkRefCounts(ImmutableMap.of(dependency.getId(), new long[] {1, 0}, result.getId(),
                       new long[] {1, 0}),
        Duration.ofSeconds(1));
    del(dependency);
    del(result);
    checkRefCounts(ImmutableMap.of());

    // TODO(kfstorm): Add remaining code of this test case based on Python test case
    // `test_dependency_refcounts`.
  }

  private static int fooBasicPinning(Object arg) {
    return 0;
  }

  public static class ActorBasicPinning {
    private ObjectRef<TestUtils.LargeObject> largeObject;

    public ActorBasicPinning() {
      // Hold a long-lived reference to a ray.put object's ID. The object
      // should not be garbage collected while the actor is alive because
      // the object is pinned by the raylet.
      largeObject = Ray.put(new TestUtils.LargeObject(25 * 1024 * 1024));
    }

    public TestUtils.LargeObject getLargeObject() {
      return largeObject.get();
    }
  }

  /**
   * Based on Python test case `test_basic_pinning`.
   */
  public void testBasicPinning() {
    // TODO(kfstorm): Set plasma store size to 100MB.

    ActorHandle<ReferenceCountingTest.ActorBasicPinning> actor =
        Ray.actor(ReferenceCountingTest.ActorBasicPinning::new).remote();
    // Fill up the object store with short-lived objects. These should be
    // evicted before the long-lived object whose reference is held by
    // the actor.
    for (int i = 0; i < 10; i++) {
      ObjectRef<Integer> intermediateResult =
          Ray.task(ReferenceCountingTest::fooBasicPinning,
                 new TestUtils.LargeObject(10 * 1024 * 1024))
              .remote();
      intermediateResult.get();
    }
    // The ray.get below would fail with only LRU eviction, as the object
    // that was ray.put by the actor would have been evicted.
    actor.task(ActorBasicPinning::getLargeObject).remote().get();
  }

  private static Object pending(TestUtils.LargeObject input1, int input2) {
    return null;
  }

  private static int signal(TestLock testLock) {
    testLock.waitLock();
    return 0;
  }

  /**
   * Based on Python test case `test_pending_task_dependency_pinning`.
   */
  public void testPendingTaskDependencyPinning() {
    // TODO(kfstorm): Set plasma store size to 100MB.

    // The object that is ray.put here will go out of scope immediately, so if
    // pending task dependencies aren't considered, it will be evicted before
    // the ray.get below due to the subsequent ray.puts that fill up the object
    // store.
    ObjectRef<Object> result;
    try (TestLock testLock = TestUtils.newLock()) {
      TestUtils.LargeObject input1 = new TestUtils.LargeObject(40 * 1024 * 1024);
      ObjectRef<Integer> input2 =
          Ray.task(ReferenceCountingTest::signal, testLock).remote();
      result = Ray.task(ReferenceCountingTest::pending, input1, input2).remote();

      for (int i = 0; i < 2; i++) {
        Ray.put(new TestUtils.LargeObject(40 * 1024 * 1024));
      }
    }
    result.get();
  }

  /**
   * Test that an object containing object IDs within it pins the inner IDs.
   * Based on Python test case `test_basic_nested_ids`.
   */
  public void testBasicNestedIds() {
    ObjectRef<byte[]> inner = Ray.put(new byte[40 * 1024 * 1024]);
    ObjectRef<List<ObjectRef<byte[]>>> outer = Ray.put(Collections.singletonList(inner));

    // Remove the local reference to the inner object.
    del(inner);

    // Check that the outer reference pins the inner object.
    fillObjectStoreAndGet(inner.getId(), true);

    // Remove the outer reference and check that the inner object gets evicted.
    del(outer);
    fillObjectStoreAndGet(inner.getId(), false);
  }

  // TODO(kfstorm): Add more test cases
}
