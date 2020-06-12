package io.ray.benchmark;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
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
  public ObjectRef<RemoteResult<Integer>> rayCall(ActorHandle rayActor) {
    return Ray.task(SingleLatencyTest::doFunc).remote();
  }

  @Override
  public boolean checkResult(Object o) {
    return (int) o == 1;
  }
}
