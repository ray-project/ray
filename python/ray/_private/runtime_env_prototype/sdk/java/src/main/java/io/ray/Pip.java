package io.ray;

// This class should be a POJO https://en.wikipedia.org/wiki/Plain_old_Java_object
public class Pip {
  private String[] packages;
  private Boolean pip_check;

  public String[] getPackages() {
    return packages;
  }

  public void setPackages(String[] packages) {
    this.packages = packages;
  }

  public Boolean getPip_check() {
    return pip_check;
  }

  public void setPip_check(Boolean pip_check) {
    this.pip_check = pip_check;
  }
}
