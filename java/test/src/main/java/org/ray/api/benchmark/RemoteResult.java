package org.ray.api.benchmark;

import java.io.Serializable;

public class RemoteResult<T> implements Serializable {

  private static final long serialVersionUID = -3825949468039358540L;

  private long finishTime;

  private T result;

  public long getFinishTime() {
    return finishTime;
  }

  public void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
  }

  public T getResult() {
    return result;
  }

  public void setResult(T result) {
    this.result = result;
  }
}
