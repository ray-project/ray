package org.ray.api.test;

import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.annotation.RayRemote;
import org.ray.api.id.UniqueId;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RuntimeContextTest extends BaseTest {

  private static UniqueId DRIVER_ID = UniqueId.randomId();
  private static String RAYLET_SOCKET_NAME = "/tmp/ray/test/raylet_socket";
  private static String OBJECT_STORE_SOCKET_NAME = "/tmp/ray/test/object_store_socket";

  @Override
  public void beforeInitRay() {
    System.setProperty("ray.driver.id", DRIVER_ID.toString());
    System.setProperty("ray.raylet.socket-name", RAYLET_SOCKET_NAME);
    System.setProperty("ray.object-store.socket-name", OBJECT_STORE_SOCKET_NAME);
  }

  @Test
  public void testRuntimeContext() {
    Assert.assertEquals(DRIVER_ID, Ray.getRuntimeContext().getCurrentDriverId());
    Assert.assertEquals(RAYLET_SOCKET_NAME, Ray.getRuntimeContext().getRayletSocketName());
    Assert.assertEquals(OBJECT_STORE_SOCKET_NAME,
        Ray.getRuntimeContext().getObjectStoreSocketName());
  }

  // test in actor.
  @RayRemote
  public static class Info {

    public Info() {

    }

    public UniqueId getCurrentDriverId() {
      return Ray.getRuntimeContext().getCurrentDriverId();
    }

    public UniqueId getCurrentActorId() {
      return Ray.getRuntimeContext().getCurrentActorId();
    }

    public String getRayletSocketName() {
      return Ray.getRuntimeContext().getRayletSocketName();
    }

    public String getObjectStoreSocketName() {
      return Ray.getRuntimeContext().getObjectStoreSocketName();
    }
  }

  @Test
  public void testRuntimeContextInActor() {
    RayActor<Info> actor = Ray.createActor(Info::new);

    Assert.assertEquals(DRIVER_ID, Ray.call(Info::getCurrentDriverId, actor).get());
    Assert.assertEquals(actor.getId(), Ray.call(Info::getCurrentActorId, actor).get());
    Assert.assertEquals(RAYLET_SOCKET_NAME, Ray.call(Info::getRayletSocketName, actor).get());
    Assert.assertEquals(OBJECT_STORE_SOCKET_NAME,
        Ray.call(Info::getObjectStoreSocketName, actor).get());

  }

}
