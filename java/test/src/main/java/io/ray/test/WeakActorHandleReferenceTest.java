package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.exception.RayActorException;
import io.ray.api.exception.RayTimeoutException;
import io.ray.runtime.actor.NativeActorHandle;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Native-runtime regressions for Java actor-handle cleanup ownership.
 *
 * <p>H0 denotes the owning handle returned by actor creation, H1 a weak named lookup, and H2 a
 * handle obtained by deserialization. For the non-detached actors exercised here, Java reachability
 * keeps a wrapper alive, while native lifecycle references keep the actor alive. Lookup and self
 * wrappers acquire no such reference, whereas creation and deserialization do.
 *
 * <p>The tests cover weak lookup cleanup, real lookup GC, repeated lookups, self cleanup in a
 * worker retaining a deserialized argument, weak lookup without an owner, and acquisition/release
 * of a deserialized owning reference. Weak cleanup must preserve other owners, but must not become
 * a source of actor keepalive itself; owning cleanup must continue releasing its reference.
 *
 * <p>Deterministic cases invoke Ray's actual registered cleanup callback. GC cases additionally
 * require both an unreachable wrapper and evidence that Ray entered cleanup. Actor calls then check
 * the lifecycle effect; a cleanup-started flag alone does not prove native release finished.
 *
 * <p>These are cluster-mode tests using the native runtime. Explicit teardown kills occur after
 * assertions, so they cannot satisfy an expected-death assertion. Death checks require a
 * RayActorException rather than a task timeout, but do not assert a specific GCS death cause.
 */
@Test(groups = {"cluster"})
public class WeakActorHandleReferenceTest {
  // A static root prevents JIT liveness analysis from dropping the surviving wrapper.
  private static final Map<String, ActorHandle<ProbeActor>> KEEP = new HashMap<>();

  public static class ProbeActor {
    // Keep the deserialized owning wrapper reachable after the actor task returns. A separate
    // self wrapper must not release this argument's native reference.
    private ActorHandle<ProbeActor> retainedArgument;

    public ProbeActor() {}

    public String ping() {
      return "pong";
    }

    public String releaseSelfHandle(ActorHandle<ProbeActor> argument) throws Exception {
      retainedArgument = argument;
      ActorHandle<ProbeActor> self = Ray.getRuntimeContext().getCurrentActorHandle();
      if (!self.getId().equals(argument.getId())) {
        throw new IllegalStateException("Self handle does not identify the argument actor");
      }
      releaseActualReference(findCallback(self));
      return "pong";
    }
  }

  // Neither observer retains the wrapper strongly: the registered callback itself is a weak
  // reference. Keeping the callback reachable lets us inspect cleanup after wrapper collection.
  private static final class CollectionTarget {
    final WeakReference<Object> wrapper;
    final Object callback;

    CollectionTarget(Object handle) throws Exception {
      wrapper = new WeakReference<>(handle);
      callback = findCallback(handle);
    }
  }

  @BeforeMethod(alwaysRun = true)
  public void startRuntime() {
    KEEP.clear();
    Ray.init();
  }

  @AfterMethod(alwaysRun = true)
  public void stopRuntime() {
    try {
      // Cleanup is deliberately after each test's observations, including expected deaths.
      if (Ray.isInitialized() && KEEP.get("survivor") != null) {
        KEEP.get("survivor").kill();
      }
    } finally {
      KEEP.clear();
      Ray.shutdown();
    }
  }

  private static Object findCallback(Object handle) throws Exception {
    Field references = NativeActorHandle.class.getDeclaredField("REFERENCES");
    references.setAccessible(true);
    for (Object reference : (Set<?>) references.get(null)) {
      // Match wrapper identity, not actor ID: H0 and H1 share an ID but have separate callbacks.
      if (((Reference<?>) reference).get() == handle) {
        return reference;
      }
    }
    throw new AssertionError("Ray cleanup callback was not registered for this wrapper");
  }

  private static boolean cleanupStarted(Object callback) throws Exception {
    Field removed = callback.getClass().getDeclaredField("removed");
    removed.setAccessible(true);
    return ((AtomicBoolean) removed.get(callback)).get();
  }

  // Execute production cleanup without waiting for GC. Despite the helper name, weak-wrapper
  // cleanup must release no native reference; it still removes its callback registration.
  private static void releaseActualReference(Object callback) throws Exception {
    Method cleanup = callback.getClass().getDeclaredMethod("finalizeReferent");
    cleanup.setAccessible(true);
    cleanup.invoke(callback);
    Assert.assertTrue(cleanupStarted(callback));
  }

  private static String createOwner() {
    String name = "java-weak-handle-" + System.nanoTime();
    ActorHandle<ProbeActor> owner =
        Ray.actor(ProbeActor::new).setName(name).setMaxRestarts(-1).remote();
    KEEP.put("survivor", owner);
    Assert.assertEquals(owner.task(ProbeActor::ping).remote().get(60000), "pong");
    return name;
  }

  private static CollectionTarget lookupTarget(String name) throws Exception {
    ActorHandle<ProbeActor> lookup = Ray.<ActorHandle<ProbeActor>>getActor(name).get();
    Assert.assertNotSame(lookup, KEEP.get("survivor"));
    Assert.assertEquals(lookup.getId(), KEEP.get("survivor").getId());
    return new CollectionTarget(lookup);
  }

  private static void awaitCollected(CollectionTarget target) throws Exception {
    long deadline = System.nanoTime() + 30_000_000_000L;
    while (System.nanoTime() < deadline) {
      // GC is only a request. Require wrapper collection AND Ray's cleanup-started marker.
      System.gc();
      System.runFinalization();
      if (target.wrapper.get() == null && cleanupStarted(target.callback)) {
        return;
      }
      Thread.sleep(50);
    }
    Assert.fail("INCONCLUSIVE: no real GC and Ray cleanup evidence within 30 seconds");
  }

  private static void assertSurvivorCallable() throws InterruptedException {
    for (int i = 0; i < 10; i++) {
      Assert.assertNotNull(KEEP.get("survivor"));
      Assert.assertEquals(KEEP.get("survivor").task(ProbeActor::ping).remote().get(5000), "pong");
      Thread.sleep(100);
    }
  }

  // Require an actor-death exception, not a task timeout. This does not distinguish REF_DELETED
  // from other death causes.
  private static void assertActorDied() throws InterruptedException {
    long deadline = System.nanoTime() + 10_000_000_000L;
    while (System.nanoTime() < deadline) {
      try {
        KEEP.get("survivor").task(ProbeActor::ping).remote().get(3000);
      } catch (RayActorException expected) {
        return;
      } catch (RayTimeoutException ignored) {
        // A timeout is not evidence of death; keep polling until the overall deadline.
      }
      Thread.sleep(50);
    }
    Assert.fail("An actor without any owning handles remained callable");
  }

  /** H1's actual cleanup callback must not consume the native reference still owned by H0. */
  public void testLookupCleanupDoesNotReleaseOwner() throws Exception {
    String name = createOwner();
    releaseActualReference(lookupTarget(name).callback);
    assertSurvivorCallable();
  }

  /** Verify the same ownership rule through real GC and Ray's registered cleanup machinery. */
  public void testLookupGcDoesNotReleaseOwner() throws Exception {
    String name = createOwner();
    awaitCollected(lookupTarget(name));
    assertSurvivorCallable();
  }

  /** Ten distinct H1 wrappers must not cumulatively consume H0's native reference. */
  public void testRepeatedLookupCleanupDoesNotReleaseOwner() throws Exception {
    String name = createOwner();
    for (int i = 0; i < 10; i++) {
      releaseActualReference(lookupTarget(name).callback);
      Assert.assertEquals(KEEP.get("survivor").task(ProbeActor::ping).remote().get(5000), "pong");
    }
  }

  private static void retainArgumentAndReleaseSelf() throws Exception {
    ActorHandle<ProbeActor> owner = KEEP.get("survivor");
    Assert.assertEquals(
        owner.task(ProbeActor::releaseSelfHandle, owner).remote().get(10000), "pong");
  }

  /**
   * Preserve the worker's owning argument reference when cleaning its separate self wrapper.
   * Dropping driver H0 is essential: otherwise H0 could keep the actor alive and hide the bug.
   */
  public void testSelfCleanupDoesNotReleaseArgumentReference() throws Exception {
    String name = createOwner();
    retainArgumentAndReleaseSelf();
    // The actor worker now retains the only owning wrapper. The driver's
    // lookup is weak, so it cannot mask an erroneous release by self cleanup.
    awaitCollected(replaceOwnerWithLookup(name));
    assertSurvivorCallable();
  }

  // Return only weak observers of H0. Ending this helper's scope also drops its local owner,
  // while the static root retains H1 so its Java reachability cannot be mistaken for keepalive.
  private static CollectionTarget replaceOwnerWithLookup(String name) throws Exception {
    ActorHandle<ProbeActor> owner = KEEP.get("survivor");
    ActorHandle<ProbeActor> lookup = Ray.<ActorHandle<ProbeActor>>getActor(name).get();
    CollectionTarget target = new CollectionTarget(owner);
    KEEP.put("survivor", lookup);
    return target;
  }

  /** With H0 collected, a Java-reachable weak H1 must not prevent actor death. */
  public void testWeakLookupDoesNotKeepActorAlive() throws Exception {
    String name = createOwner();
    awaitCollected(replaceOwnerWithLookup(name));
    assertActorDied();
  }

  @SuppressWarnings("unchecked")
  private static CollectionTarget replaceOwnerWithDeserializedHandle() throws Exception {
    NativeActorHandle owner = (NativeActorHandle) KEEP.get("survivor");
    NativeActorHandle copy = NativeActorHandle.fromBytes(owner.toBytes());
    CollectionTarget target = new CollectionTarget(owner);
    KEEP.put("survivor", (ActorHandle<ProbeActor>) copy);
    return target;
  }

  /**
   * H2 must keep the actor alive after H0 is collected, then release it when H2 is cleaned up. This
   * guards against fixing weak cleanup by disabling native release for every wrapper.
   */
  public void testDeserializedHandleOwnsAndReleasesReference() throws Exception {
    createOwner();
    awaitCollected(replaceOwnerWithDeserializedHandle());
    assertSurvivorCallable();
    releaseActualReference(findCallback(KEEP.get("survivor")));
    assertActorDied();
  }
}
