package org.ray.api.benchmark;

import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.testng.annotations.Test;

public class SingleLatencyTest extends RayBenchmarkTest {

  public static final int totalNum = 10;
  private static final long serialVersionUID = 3559601273941694468L;

  public static RemoteResult<Integer> doFunc() {
    RemoteResult<Integer> remoteResult = new RemoteResult<>();
    remoteResult.setResult(1);
    return remoteResult;
  }

  @Test
  public void test() {
    super.singleLatencyTest(totalNum, null);
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
