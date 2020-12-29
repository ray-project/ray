package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import java.lang.ref.WeakReference;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class GlobalGcTest extends BaseTest {

  @BeforeClass
  public void setUp() {
    System.setProperty("ray.head-args.0", "--object-store-memory=" + 140L * 1024 * 1024);
  }

  @AfterClass
  public void tearDown() {
    System.clearProperty("ray.head-args.0");
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

    private WeakReference<LargeObjectWithCyclicRef> garbage;

    public GarbageHolder() {
      LargeObjectWithCyclicRef x = new LargeObjectWithCyclicRef();
      garbage = new WeakReference<>(x);
    }

    public boolean hasGarbage() {
      return garbage.get() != null;
    }

    public TestUtils.LargeObject returnLargeObject() {
      return new TestUtils.LargeObject(80 * 1024 * 1024);
    }
  }

  private void testGlobalGcWhenFull(boolean withPut) {
    // Local driver.
    WeakReference<LargeObjectWithCyclicRef> localRef =
        new WeakReference<>(new LargeObjectWithCyclicRef());

    // Remote workers.
    List<ActorHandle<GarbageHolder>> actors =
        IntStream.range(0, 2)
            .mapToObj(i -> Ray.actor(GarbageHolder::new).remote())
            .collect(Collectors.toList());

    Assert.assertNotNull(localRef.get());
    for (ActorHandle<GarbageHolder> actor : actors) {
      Assert.assertTrue(actor.task(GarbageHolder::hasGarbage).remote().get());
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
