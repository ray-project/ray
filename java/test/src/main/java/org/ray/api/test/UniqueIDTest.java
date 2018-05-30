package org.ray.api.test;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ray.api.RayObject;
import org.ray.api.RayRemote;
import org.ray.api.UniqueID;
import org.ray.api.funcs.RayFunc_1_1;
import org.ray.core.RayRuntime;
import org.ray.core.UniqueIdHelper;

@RunWith(MyRunner.class)
public class UniqueIDTest {

  @Test
  public void test() {
    UniqueID tid = UniqueIdHelper.nextTaskId(0xdeadbeefL);
    UniqueIdHelper.setTest(tid, true);
    System.out.println("Tested task id = " + tid);
    RayFunc_1_1<Integer, String> f = UniqueIDTest::hi;
    RayObject<String> result = new RayObject<>(
        RayRuntime.getInstance().call(
            tid,
            RayFunc_1_1.class,
            f,
            1, 1
        ).getObjs()[0].getId()
    );
    System.out.println("Tested task return object id = " + result.getId());
    Assert.assertEquals("hi1", result.get());
  }

  @RayRemote
  public static String hi(Integer i) {
    return "hi" + i;
  }
}
