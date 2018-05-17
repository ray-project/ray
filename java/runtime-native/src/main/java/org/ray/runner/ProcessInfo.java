package org.ray.runner;

public class ProcessInfo {

  public Process process;
  public String[] cmd;
  public RunInfo.ProcessType type;
  public String workDir;
  public String redisAddress;
  public String ip;
  public boolean redirect;
  public boolean cleanup;
}