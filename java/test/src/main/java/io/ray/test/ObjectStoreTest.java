package io.ray.test;

import com.google.common.collect.ImmutableList;
import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.exception.RayTaskException;
import io.ray.api.exception.UnreconstructableException;
import java.util.List;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.Test;

/** Test putting and getting objects. */
public class ObjectStoreTest extends BaseTest {

  public static class Creator {
    public ObjectRef<Integer> put(int value, ActorHandle owner) {
      return Ray.put(value, owner);
    }

    public void exit() {
      Ray.exitActor();
    }
  }

  public static class Owner {

    private ObjectRef<Integer> ref = null;

    public int warmup() {
      return 0;
    }

    public int set(ObjectRef<Integer> ref) {
      this.ref = ref;
      return 0;
    }

    public void exit() {
      Ray.exitActor();
    }
  }

  public static class Borrower {
    public int get(ObjectRef<Integer> ref) {
      return Ray.get(ref);
    }
  }

  @Test
  public void testPutAndGet() {
    {
      ObjectRef<Integer> obj = Ray.put(1);
      Assert.assertEquals(1, (int) obj.get());
    }

    {
      String s = null;
      ObjectRef<String> obj = Ray.put(s);
      Assert.assertNull(obj.get());
    }

    {
      List<List<String>> l = ImmutableList.of(ImmutableList.of("abc"));
      ObjectRef<List<List<String>>> obj = Ray.put(l);
      Assert.assertEquals(obj.get(), l);
    }
  }

  @Test
  public void testGetMultipleObjects() {
    List<Integer> ints = ImmutableList.of(1, 2, 3, 4, 5);
    List<ObjectRef<Integer>> refs = ints.stream().map(Ray::put).collect(Collectors.toList());
    Assert.assertEquals(ints, Ray.get(refs));
  }

  @Test(groups = {"cluster"})
  public void testOwnerAssignWhenPut() throws Exception {
    // This test should align with test_owner_assign_when_put in Python
    ActorHandle<Creator> creator = Ray.actor(Creator::new).remote();
    ActorHandle<Owner> owner = Ray.actor(Owner::new).remote();
    ActorHandle<Borrower> borrower = Ray.actor(Borrower::new).remote();
    Ray.get(owner.task(Owner::warmup).remote());
    ObjectRef<ObjectRef<Integer>> ref = creator.task(Creator::put, 1, owner).remote();
    Ray.get(owner.task(Owner::set, ref).remote());
    creator.task(Creator::exit).remote();
    Thread.sleep(10000);
    int data = Ray.get(borrower.task(Borrower::get, ref).remote());
    Assert.assertEquals(data, 1);
    owner.task(Owner::exit).remote();
    Thread.sleep(2000);
    try {
      // Object should be lost
      data = Ray.get(borrower.task(Borrower::get, ref).remote());
    } catch (RayTaskException e) {
      Assert.assertEquals(e.getCause() instanceof UnreconstructableException, true);
    }
  }
}
