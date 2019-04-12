package org.ray.streaming.util;

import java.io.Serializable;

public class Record3 implements Serializable {
  private String f0;
  private String f1;
  private String f2;

  public Record3(String f0, String f1, String f2) {
    this.f0 = f0;
    this.f1 = f1;
    this.f2 = f2;
  }

  public String getF0() {
    return f0;
  }

  public void setF0(String f0) {
    this.f0 = f0;
  }

  public String getF1() {
    return f1;
  }

  public void setF1(String f1) {
    this.f1 = f1;
  }

  public String getF2() {
    return f2;
  }

  public void setF2(String f2) {
    this.f2 = f2;
  }

}
