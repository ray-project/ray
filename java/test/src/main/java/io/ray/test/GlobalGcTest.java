package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import java.lang.ref.WeakReference;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class GlobalGcTest extends BaseTest {

  @BeforeClass
  public void setUp() {
    System.setProperty("ray.head-args.0", "--object-store-memory=" + 140L * 1024 * 1024);
  }

  public static class LargeObjectWithCyclicRef {

    private final LargeObjectWithCyclicRef loop;

    private final ObjectRef<TestUtils.LargeObject> largeObject;

    public LargeObjectWithCyclicRef() {
      this.loop = this;
      this.largeObject = Ray.put(new TestUtils.LargeObject(40 * 1024 * 1024));
    }
  }

  public static class GarbageHolder {

    // Hold a strong reference initially so that the JVM cannot collect the
    // cyclic object before the test explicitly triggers the global GC event
    // below. This mirrors the `gc.disable()` call used in the Python
    // equivalent of this test (see python/ray/tests/test_global_gc.py).
    private LargeObjectWithCyclicRef strongRef;

    private WeakReference<LargeObjectWithCyclicRef> garbage;

    public GarbageHolder() {
      strongRef = new LargeObjectWithCyclicRef();
      garbage = new WeakReference<>(strongRef);
    }

    public boolean hasGarbage() {
      return garbage.get() != null;
    }

    /**
     * Release the strong reference so the cyclic object becomes eligible for
     * collection by the next JVM GC. Call this immediately before triggering
     * the global GC event the test is exercising. Returns a boolean so the
     * caller can `.get()` on the resulting ObjectRef to synchronously wait for
     * the release to take effect on the actor process; Ray's Java API does not
     * surface an ObjectRef for void actor methods.
     */
    public boolean releaseStrongRef() {
      strongRef = null;
      return true;
    }

    public TestUtils.LargeObject returnLargeObject() {
      return new TestUtils.LargeObject(80 * 1024 * 1024);
    }
  }

  private void testGlobalGcWhenFull(boolean withPut) {
    // Local driver. Hold a strong reference until we explicitly want to allow
    // GC (mirrors `gc.disable()` in the Python equivalent test).
    LargeObjectWithCyclicRef localStrong = new LargeObjectWithCyclicRef();
    WeakReference<LargeObjectWithCyclicRef> localRef = new WeakReference<>(localStrong);

    // Remote workers.
    List<ActorHandle<GarbageHolder>> actors =
        IntStream.range(0, 2)
            .mapToObj(i -> Ray.actor(GarbageHolder::new).remote())
            .collect(Collectors.toList());

    Assert.assertNotNull(localRef.get());
    for (ActorHandle<GarbageHolder> actor : actors) {
      Assert.assertTrue(actor.task(GarbageHolder::hasGarbage).remote().get());
    }

    // Now release the strong references so the cyclic objects become eligible
    // for collection once the global GC event is triggered below.
    localStrong = null;
    for (ActorHandle<GarbageHolder> actor : actors) {
      actor.task(GarbageHolder::releaseStrongRef).remote().get();
    }

    if (withPut) {
      // GC should be triggered for all workers, including the local driver,
      // when the driver tries to Ray.put a value that doesn't fit in the
      // object store. This should cause the captured ObjectRefs to be evicted.
      Ray.put(new TestUtils.LargeObject(80 * 1024 * 1024));
    } else {
      // GC should be triggered for all workers, including the local driver,
      // when a remote task tries to put a return value that doesn't fit in
      // the object store. This should cause the captured ObjectRefs' to be evicted.
      actors.get(0).task(GarbageHolder::returnLargeObject).remote().get();
    }

    TestUtils.waitForCondition(
        () ->
            localRef.get() == null
                && actors.stream().noneMatch(a -> a.task(GarbageHolder::hasGarbage).remote().get()),
        10 * 1000);
  }

  public void testGlobalGcWhenFullWithPut() {
    testGlobalGcWhenFull(true);
  }

  public void testGlobalGcWhenFullWithReturn() {
    testGlobalGcWhenFull(false);
  }
}
