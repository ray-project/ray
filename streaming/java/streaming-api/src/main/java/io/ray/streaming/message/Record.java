package io.ray.streaming.message;

import java.io.Serializable;


public class Record<T> implements Serializable {
  protected transient String stream;
  protected T value;

  public Record(T value) {
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

  @Override
  public String toString() {
    return value.toString();
  }

}
