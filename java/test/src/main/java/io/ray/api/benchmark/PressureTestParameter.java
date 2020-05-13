package io.ray.api.benchmark;

import io.ray.api.RayActor;
import java.io.Serializable;

public class PressureTestParameter implements Serializable {

  private static final long serialVersionUID = -52054601722982473L;

  private Integer clientNum = 1; //number of test client

  private PressureTestType pressureTestType = PressureTestType.RATE_LIMITER; //pressure test type

  private Integer totalNum = 1; //total number of task under the mode of MAX

  private Integer totalQps = 1; //total qps of task under the mode of RATE_LIMITER

  private Integer duration = 1; //duration of the pressure test under the mode of RATE_LIMITER

  private RayBenchmarkTest rayBenchmarkTest; //reference of current test case instance

  private RayActor rayActor; // reference of the Actor, if only test remote funtion it could be null

  public Integer getClientNum() {
    return clientNum;
  }

  public void setClientNum(Integer clientNum) {
    this.clientNum = clientNum;
  }

  public PressureTestType getPressureTestType() {
    return pressureTestType;
  }

  public void setPressureTestType(PressureTestType pressureTestType) {
    this.pressureTestType = pressureTestType;
  }

  public Integer getTotalNum() {
    return totalNum;
  }

  public void setTotalNum(Integer totalNum) {
    this.totalNum = totalNum;
  }

  public Integer getTotalQps() {
    return totalQps;
  }

  public void setTotalQps(Integer totalQps) {
    this.totalQps = totalQps;
  }

  public Integer getDuration() {
    return duration;
  }

  public void setDuration(Integer duration) {
    this.duration = duration;
  }

  public RayBenchmarkTest getRayBenchmarkTest() {
    return rayBenchmarkTest;
  }

  public void setRayBenchmarkTest(RayBenchmarkTest rayBenchmarkTest) {
    this.rayBenchmarkTest = rayBenchmarkTest;
  }

  public RayActor getRayActor() {
    return rayActor;
  }

  public void setRayActor(RayActor rayActor) {
    this.rayActor = rayActor;
  }
}
