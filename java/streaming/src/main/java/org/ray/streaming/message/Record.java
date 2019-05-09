package org.ray.streaming.message;

import java.io.Serializable;


public class Record<T> implements Serializable {

  protected transient String stream;
  protected transient long batchId;
  protected T value;

  public Record(T value) {
    this.value = value;
  }

  public Record(long batchId, T value) {
    this.batchId = batchId;
    this.value = value;
  }

  public T getValue() {
    return value;
  }

  public void setValue(T value) {
    this.value = value;
  }

  public String getStream() {
    return stream;
  }

  public void setStream(String stream) {
    this.stream = stream;
  }

  public long getBatchId() {
    return batchId;
  }

  public void setBatchId(long batchId) {
    this.batchId = batchId;
  }

  @Override
  public String toString() {
    return value.toString();
  }

}
