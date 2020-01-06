package org.ray.deploy.rps.model;

public class ResourceDescription {

  public String cpu;
  public String gpu;
  public String memoryMB;

  public ResourceDescription() {
  }

  public String getCpu() {
    return cpu;
  }

  public void setCpu(String cpu) {
    this.cpu = cpu;
  }

  public String getGpu() {
    return gpu;
  }

  public void setGpu(String gpu) {
    this.gpu = gpu;
  }

  public String getMemoryMB() {
    return memoryMB;
  }

  public void setMemoryMB(String memoryMB) {
    this.memoryMB = memoryMB;
  }

  @Override
  public String toString() {
    return "ResourceDescription{" +
        "cpu='" + cpu + '\'' +
        ", gpu='" + gpu + '\'' +
        ", memoryMB='" + memoryMB + '\'' +
        '}';
  }
}
