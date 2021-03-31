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
import java.lang.ref.Reference;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class ReferenceCountingTest extends BaseTest {
  @BeforeClass
  public void setUp() {
    System.setProperty("ray.head-args.0", "--object-store-memory=" + 100L * 1024 * 1024);
  }

  /**
   * Because we can't explicitly GC an Java object. We use this helper method to manually remove an
   * local reference.
   */
  private static void del(ObjectRef<?> obj) {
    try {
      Field referencesField = ObjectRefImpl.class.getDeclaredField("REFERENCES");
      referencesField.setAccessible(true);
      Set<?> references = (Set<?>) referencesField.get(null);
      Class<?> referenceClass =
          Class.forName("io.ray.runtime.object.ObjectRefImpl$ObjectRefImplReference");
      Method finalizeReferentMethod = referenceClass.getDeclaredMethod("finalizeReferent");
      finalizeReferentMethod.setAccessible(true);
      for (Object reference : references) {
        if (obj.equals(((Reference<?>) reference).get())) {
          finalizeReferentMethod.invoke(reference);
          break;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void checkRefCounts(Map<ObjectId, long[]> expected, Duration timeout) {
    Instant start = Instant.now();
    while (true) {
      Map<ObjectId, long[]> actual =
          ((NativeObjectStore) TestUtils.getRuntime().getObjectStore()).getAllReferenceCounts();
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

  private void checkRefCounts(ObjectId objectId, long localRefCount, long submittedTaskRefCount) {
    checkRefCounts(ImmutableMap.of(objectId, new long[] {localRefCount, submittedTaskRefCount}));
  }

  private void checkRefCounts(
      ObjectId objectId1,
      long localRefCount1,
      long submittedTaskRefCount1,
      ObjectId objectId2,
      long localRefCount2,
      long submittedTaskRefCount2) {
    checkRefCounts(
        ImmutableMap.of(
            objectId1,
            new long[] {localRefCount1, submittedTaskRefCount1},
            objectId2,
            new long[] {localRefCount2, submittedTaskRefCount2}));
  }

  private static void fillObjectStoreAndGet(ObjectId objectId, boolean succeed) {
    fillObjectStoreAndGet(objectId, succeed, 40 * 1024 * 1024, 5);
  }

  private static void fillObjectStoreAndGet(
      ObjectId objectId, boolean succeed, int objectSize, int numObjects) {
    for (int i = 0; i < numObjects; i++) {
      Ray.put(new TestUtils.LargeObject(objectSize));
    }
    if (succeed) {
      TestUtils.getRuntime().getObjectStore().getRaw(ImmutableList.of(objectId), Long.MAX_VALUE);
    } else {
      List<Boolean> result =
          TestUtils.getRuntime().getObjectStore().wait(ImmutableList.of(objectId), 1, 100, true);
      Assert.assertFalse(result.get(0));
    }
  }

  /** Based on Python test case `test_local_refcounts`. */
  public void testLocalRefCounts() {
    ObjectRefImpl<Object> obj1 = (ObjectRefImpl<Object>) Ray.put(null);
    checkRefCounts(obj1.getId(), 1, 0);
    ObjectRef<Object> obj1Copy = new ObjectRefImpl<>(obj1.getId(), obj1.getType());
    checkRefCounts(obj1.getId(), 2, 0);

    del(obj1);
    checkRefCounts(obj1.getId(), 1, 0);
    del(obj1Copy);
    checkRefCounts(ImmutableMap.of());
  }

  private static int oneDep(Object obj) {
    return oneDep(obj, null);
  }

  private static int oneDep(Object obj, ActorHandle<SignalActor> singal) {
    return oneDep(obj, singal, false);
  }

  private static int oneDep(Object obj, ActorHandle<SignalActor> singal, boolean fail) {
    if (singal != null) {
      singal.task(SignalActor::waitSignal).remote().get();
    }
    if (fail) {
      throw new RuntimeException("failed on purpose");
    }
    return 0;
  }

  private static TestUtils.LargeObject oneDepLarge(Object obj, ActorHandle<SignalActor> singal) {
    if (singal != null) {
      singal.task(SignalActor::waitSignal).remote().get();
    }
    // This will be spilled to plasma.
    return new TestUtils.LargeObject(10 * 1024 * 1024);
  }

  private static void sendSignal(ActorHandle<SignalActor> signal) {
    ObjectRef<Integer> result = signal.task(SignalActor::sendSignal).remote();
    result.get();
    // Remove the reference immediately, otherwise it will affect subsequent tests.
    del(result);
  }

  /** Based on Python test case `test_dependency_refcounts`. */
  public void testDependencyRefCounts() {
    {
      // Test that regular plasma dependency refcounts are decremented once the
      // task finishes.
      ActorHandle<SignalActor> signal = SignalActor.create();
      ObjectRefImpl<TestUtils.LargeObject> largeDep =
          (ObjectRefImpl<TestUtils.LargeObject>) Ray.put(new TestUtils.LargeObject());
      ObjectRefImpl<Object> result =
          (ObjectRefImpl<Object>)
              Ray.<TestUtils.LargeObject, ActorHandle<SignalActor>, Object>task(
                      ReferenceCountingTest::oneDep, largeDep, signal)
                  .remote();
      checkRefCounts(largeDep.getId(), 1, 1, result.getId(), 1, 0);
      sendSignal(signal);
      // Reference count should be removed once the task finishes.
      checkRefCounts(largeDep.getId(), 1, 0, result.getId(), 1, 0);
      del(largeDep);
      del(result);
      checkRefCounts(ImmutableMap.of());
    }

    {
      // Test that inlined dependency refcounts are decremented once they are
      // inlined.
      ActorHandle<SignalActor> signal = SignalActor.create();
      ObjectRefImpl<Integer> dep =
          (ObjectRefImpl<Integer>)
              Ray.<Integer, ActorHandle<SignalActor>, Integer>task(
                      ReferenceCountingTest::oneDep, Integer.valueOf(1), signal)
                  .remote();
      checkRefCounts(dep.getId(), 1, 0);
      ObjectRefImpl<Object> result =
          (ObjectRefImpl<Object>)
              Ray.<Integer, Object>task(ReferenceCountingTest::oneDep, dep).remote();
      checkRefCounts(dep.getId(), 1, 1, result.getId(), 1, 0);
      sendSignal(signal);
      // Reference count should be removed as soon as the dependency is inlined.
      checkRefCounts(dep.getId(), 1, 0, result.getId(), 1, 0);
      del(dep);
      del(result);
      checkRefCounts(ImmutableMap.of());
    }

    {
      // Test that spilled plasma dependency refcounts are decremented once
      // the task finishes.
      ActorHandle<SignalActor> signal1 = SignalActor.create();
      ActorHandle<SignalActor> signal2 = SignalActor.create();
      ObjectRefImpl<TestUtils.LargeObject> dep =
          (ObjectRefImpl<TestUtils.LargeObject>)
              Ray.<TestUtils.LargeObject, ActorHandle<SignalActor>, TestUtils.LargeObject>task(
                      ReferenceCountingTest::oneDepLarge, (TestUtils.LargeObject) null, signal1)
                  .remote();
      checkRefCounts(dep.getId(), 1, 0);
      ObjectRefImpl<Integer> result =
          (ObjectRefImpl<Integer>)
              Ray.<TestUtils.LargeObject, ActorHandle<SignalActor>, Integer>task(
                      ReferenceCountingTest::oneDep, dep, signal2)
                  .remote();
      checkRefCounts(dep.getId(), 1, 1, result.getId(), 1, 0);
      sendSignal(signal1);
      dep.get(); // TODO(kfstorm): timeout=10
      // Reference count should remain because the dependency is in plasma.
      checkRefCounts(dep.getId(), 1, 1, result.getId(), 1, 0);
      sendSignal(signal2);
      // Reference count should be removed because the task finished.
      checkRefCounts(dep.getId(), 1, 0, result.getId(), 1, 0);
      del(dep);
      del(result);
      checkRefCounts(ImmutableMap.of());
    }

    {
      // Test that regular plasma dependency refcounts are decremented if a task
      // fails.
      ActorHandle<SignalActor> signal = SignalActor.create();
      ObjectRefImpl<TestUtils.LargeObject> largeDep =
          (ObjectRefImpl<TestUtils.LargeObject>)
              Ray.put(new TestUtils.LargeObject(10 * 1024 * 1024));
      ObjectRefImpl<Integer> result =
          (ObjectRefImpl<Integer>)
              Ray.<TestUtils.LargeObject, ActorHandle<SignalActor>, Boolean, Integer>task(
                      ReferenceCountingTest::oneDep, largeDep, signal, /* fail= */ true)
                  .remote();
      checkRefCounts(largeDep.getId(), 1, 1, result.getId(), 1, 0);
      sendSignal(signal);
      // Reference count should be removed once the task finishes.
      checkRefCounts(largeDep.getId(), 1, 0, result.getId(), 1, 0);
      del(largeDep);
      del(result);
      checkRefCounts(ImmutableMap.of());
    }

    {
      // Test that spilled plasma dependency refcounts are decremented if a task
      // fails.
      ActorHandle<SignalActor> signal1 = SignalActor.create();
      ActorHandle<SignalActor> signal2 = SignalActor.create();
      ObjectRefImpl<TestUtils.LargeObject> dep =
          (ObjectRefImpl<TestUtils.LargeObject>)
              Ray.<Integer, ActorHandle<SignalActor>, TestUtils.LargeObject>task(
                      ReferenceCountingTest::oneDepLarge, (Integer) null, signal1)
                  .remote();
      checkRefCounts(dep.getId(), 1, 0);
      ObjectRefImpl<Integer> result =
          (ObjectRefImpl<Integer>)
              Ray.<TestUtils.LargeObject, ActorHandle<SignalActor>, Boolean, Integer>task(
                      ReferenceCountingTest::oneDep, dep, signal2, /* fail= */ true)
                  .remote();
      checkRefCounts(dep.getId(), 1, 1, result.getId(), 1, 0);
      sendSignal(signal1);
      dep.get(); // TODO(kfstorm): timeout=10
      // Reference count should remain because the dependency is in plasma.
      checkRefCounts(dep.getId(), 1, 1, result.getId(), 1, 0);
      sendSignal(signal2);
      // Reference count should be removed because the task finished.
      checkRefCounts(dep.getId(), 1, 0, result.getId(), 1, 0);
      del(dep);
      del(result);
      checkRefCounts(ImmutableMap.of());
    }
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

  /** Based on Python test case `test_basic_pinning`. */
  public void testBasicPinning() {
    ActorHandle<ActorBasicPinning> actor = Ray.actor(ActorBasicPinning::new).remote();

    // Fill up the object store with short-lived objects. These should be
    // evicted before the long-lived object whose reference is held by
    // the actor.
    for (int i = 0; i < 10; i++) {
      ObjectRef<Integer> intermediateResult =
          Ray.task(
                  ReferenceCountingTest::fooBasicPinning,
                  new TestUtils.LargeObject(10 * 1024 * 1024))
              .remote();
      intermediateResult.get();
    }
    // The ray.get below would fail with only LRU eviction, as the object
    // that was ray.put by the actor would have been evicted.
    actor.task(ActorBasicPinning::getLargeObject).remote().get();
  }

  private static Object pending(TestUtils.LargeObject input1, Integer input2) {
    return null;
  }

  /** Based on Python test case `test_pending_task_dependency_pinning`. */
  public void testPendingTaskDependencyPinning() {
    // The object that is ray.put here will go out of scope immediately, so if
    // pending task dependencies aren't considered, it will be evicted before
    // the ray.get below due to the subsequent ray.puts that fill up the object
    // store.
    TestUtils.LargeObject input1 = new TestUtils.LargeObject(40 * 1024 * 1024);
    ActorHandle<SignalActor> signal = SignalActor.create();
    ObjectRef<Object> result =
        Ray.task(
                ReferenceCountingTest::pending,
                input1,
                signal.task(SignalActor::waitSignal).remote())
            .remote();

    for (int i = 0; i < 2; i++) {
      Ray.put(new TestUtils.LargeObject(40 * 1024 * 1024));
    }

    sendSignal(signal);
    result.get();
  }

  /**
   * Test that an object containing object IDs within it pins the inner IDs. Based on Python test
   * case `test_basic_nested_ids`.
   */
  public void testBasicNestedIds() {
    ObjectRefImpl<byte[]> inner = (ObjectRefImpl<byte[]>) Ray.put(new byte[40 * 1024 * 1024]);
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
