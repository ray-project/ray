package org.ray.api.test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.TestUtils;
import org.ray.api.TestUtils.LargeObject;
import org.ray.api.TestUtils.TestLock;
import org.ray.api.annotation.RayRemote;
import org.ray.api.id.ObjectId;
import org.ray.api.runtime.RayRuntimeFactory;
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

  public static long fBasicPinning(List<Byte> array) {
    long result = 0;
    for (byte x : array) {
      result += x;
    }
    return result;
  }

  @RayRemote
  public static class ActorBasicPinning {
    private RayObject<List<Byte>> largeObject;
    public ActorBasicPinning() {
      largeObject = Ray.put(Stream.generate(() -> (byte) 0)
              .limit(25 * 1024 * 1024)
              .collect(Collectors.toList()));
    }

    public List<Byte> getLargeObject() {
      return largeObject.get();
    }
  }

  public void testBasicPinning() {
    RayActor<ReferenceCountingTest.ActorBasicPinning> actor =
            Ray.createActor(ReferenceCountingTest.ActorBasicPinning::new);
    //    # Fill up the object store with short-lived objects. These should be
    //    # evicted before the long-lived object whose reference is held by
    //    # the actor.
    for (int i = 0; i < 10; i++) {
      RayObject<Long> intermediateResult = Ray.call(ReferenceCountingTest::fBasicPinning,
              Stream.generate(() -> (byte) 0)
                      .limit(25 * 1024 * 1024)
                      .collect(Collectors.toList()));
      intermediateResult.get();
    }
    //    # The ray.get below would fail with only LRU eviction, as the object
    //    # that was ray.put by the actor would have been evicted.
    Ray.call(ActorBasicPinning::getLargeObject, actor).get();
  }

  public static Object pending(List<Byte> input1, Object input2) {
    return null;
  }

  @Test(enabled = false)
  public void testPendingTaskDependencyPinning() {
    //    # The object that is ray.put here will go out of scope immediately, so if
    //    # pending task dependencies aren't considered, it will be evicted before
    //    # the ray.get below due to the subsequent ray.puts that fill up the object
    //    # store.
    List<Byte> array = Stream.generate(() -> (byte) 0)
            .limit(25 * 1024 * 1024)
            .collect(Collectors.toList());
    ObjectId randomId = ObjectId.fromRandom();
    RayObject<Object> result = Ray.call(ReferenceCountingTest::pending, array, randomId);

    for (int i = 0; i < 2; i++) {
      Ray.put(Stream.generate(() -> (byte) 0)
              .limit(25 * 1024 * 1024)
              .collect(Collectors.toList()));
    }
    //
    //    ray.worker.global_worker.put_object(None, object_id=random_oid)
    //    ray.get(oid)
  }

  @Test(enabled = false)
  public void testFeatureFlag() {
  }

  @Test(enabled = false)
  public void testBasicSerializedReference() {

  }

  @Test(enabled = false)
  public void test_recursive_serialized_reference() {

  }

  @Test(enabled = false)
  public void test_actor_holding_serialized_reference() {

  }

  @Test(enabled = false)
  public void test_worker_holding_serialized_reference() {

  }

  @Test(enabled = false)
  public void test_basic_nested_ids() {

  }

  @Test(enabled = false)
  public void test_recursively_nest_ids() {

  }

  @Test(enabled = false)
  public void test_return_object_id() {

  }

  @Test(enabled = false)
  public void test_pass_returned_object_id() {

  }

  @Test(enabled = false)
  public void test_recursively_pass_returned_object_id() {

  }

}
