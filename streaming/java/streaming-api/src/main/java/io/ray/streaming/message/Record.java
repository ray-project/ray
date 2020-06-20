package io.ray.streaming.message;

import java.io.Serializable;
import java.util.Objects;

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
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Record<?> record = (Record<?>) o;
    return Objects.equals(stream, record.stream) &&
        Objects.equals(value, record.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(stream, value);
  }

  @Override
  public String toString() {
    return value.toString();
  }

}
