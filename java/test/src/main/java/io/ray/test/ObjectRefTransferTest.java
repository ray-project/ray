package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ObjectRefTransferTest extends BaseTest {

  @BeforeClass
  public void setUpClass() {
    System.setProperty("ray.raylet.config.num_workers_per_process_java", "1");
  }

  @AfterClass
  public void tearDownClass() {
    System.clearProperty("ray.raylet.config.num_workers_per_process_java");
  }

  @Test
  public void testObjectTransfer() {
    ObjectRef<String> objectRef = Ray.put("test");
    List<ObjectRef<String>> data = new ArrayList<>();
    data.add(objectRef);

    ActorHandle<RemoteActor> handle = Ray.actor(RemoteActor::new).remote();
    String result = handle.task(RemoteActor::get, data).remote().get();
    Assert.assertEquals(result, "test");
    handle.kill();
  }

  @Test
  public void testNestedObjectId() {
    ObjectRef<String> inner = Ray.put("inner");
    ObjectRef<ObjectRef<String>> outer = Ray.put(inner);
    List<ObjectRef<ObjectRef<String>>> data = new ArrayList<>();
    data.add(outer);

    ActorHandle<RemoteActor> handle = Ray.actor(RemoteActor::new).remote();
    String result = handle.task(RemoteActor::getNested, data).remote().get();
    Assert.assertEquals(result, "inner");
    handle.kill();
  }

  public static class RemoteActor {
    public String get(List<ObjectRef<String>> value) {
      return Ray.get(value.get(0));
    }

    public String getNested(List<ObjectRef<ObjectRef<String>>> value) {
      return Ray.get(value.get(0).get());
    }
  }
}
