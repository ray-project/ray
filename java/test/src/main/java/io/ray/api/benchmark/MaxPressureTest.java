package io.ray.api.benchmark;

import io.ray.api.Ray;
import io.ray.api.RayActor;
import io.ray.api.RayObject;
import org.testng.annotations.Test;

public class MaxPressureTest extends RayBenchmarkTest {

  public static final int clientNum = 2;
  public static final int totalNum = 10;
  private static final long serialVersionUID = -1684518885171395952L;

  public static RemoteResult<Integer> currentTime() {
    RemoteResult<Integer> remoteResult = new RemoteResult<>();
    remoteResult.setFinishTime(System.nanoTime());
    remoteResult.setResult(0);
    return remoteResult;
  }

  @Test
  public void test() {
    PressureTestParameter pressureTestParameter = new PressureTestParameter();
    pressureTestParameter.setClientNum(clientNum);
    pressureTestParameter.setTotalNum(totalNum);
    pressureTestParameter.setRayBenchmarkTest(this);
    super.maxPressureTest(pressureTestParameter);
  }

  @Override
  public RayObject<RemoteResult<Integer>> rayCall(RayActor rayActor) {

    return Ray.call(MaxPressureTest::currentTime);
  }

  @Override
  public boolean checkResult(Object o) {
    return (int) o == 0;
  }

}
