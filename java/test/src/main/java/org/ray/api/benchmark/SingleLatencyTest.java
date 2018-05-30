package org.ray.api.benchmark;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.RayRemote;
import org.ray.api.test.MyRunner;

@RunWith(MyRunner.class)
public class SingleLatencyTest extends RayBenchmarkTest {

  private static final long serialVersionUID = 3559601273941694468L;

  public static final int totalNum = 10;

  @Test
  public void Test() {
    super.singleLatencyTest(totalNum, null);
  }

  @RayRemote
  public static RemoteResult<Integer> doFunc() {
    RemoteResult<Integer> remoteResult = new RemoteResult<>();
    remoteResult.setResult(1);
    return remoteResult;
  }

  @Override
  public RayObject<RemoteResult<Integer>> rayCall(RayActor rayActor) {
    return Ray.call(SingleLatencyTest::doFunc);
  }

  @Override
  public boolean checkResult(Object o) {
    return (int) o == 1;
  }
}
