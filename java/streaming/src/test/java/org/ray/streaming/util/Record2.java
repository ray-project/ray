package org.ray.streaming.util;

import java.io.Serializable;

public class Record2 implements Serializable {

  private String f0;
  private String f1;

  public Record2(String f0, String f1) {
    this.f0 = f0;
    this.f1 = f1;
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

}
