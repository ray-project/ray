/**
 * Alipay.com Inc. Copyright (c) 2004-2019 All Rights Reserved.
 */
package org.ray.deploy.rps.model;

public abstract class Request {

  public String clusterName;
  public Long sequence;

  public String getClusterName() {
    return clusterName;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public Long getSequence() {
    return sequence;
  }

  public void setSequence(Long sequence) {
    this.sequence = sequence;
  }

  @Override
  public String toString() {
    return "Request{" +
        "clusterName='" + clusterName + '\'' +
        ", sequence=" + sequence +
        '}';
  }
}