package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class GetCurrentActorHandleTest extends BaseTest {

  private static class Echo {

    public Echo() {}

    public ObjectRef<String> echo(String str) {
      ActorHandle<Echo> self = Ray.getRuntimeContext().getCurrentActorHandle();
      return self.task(Echo::echo2, str).remote();
    }

    public String echo2(String str) {
      return str;
    }
  }

  @Test
  public void testGetCurrentActorHandle() {
    ActorHandle<Echo> echo1 = Ray.actor(Echo::new).remote();
    ObjectRef<String> obj = echo1.task(Echo::echo, "hello").remote().get();
    Assert.assertEquals("hello", obj.get());
  }
}
