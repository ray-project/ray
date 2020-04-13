package io.ray.streaming.message;


public class KeyRecord<K, T> extends Record<T> {

  private K key;

  public KeyRecord(K key, T value) {
    super(value);
    this.key = key;
  }

  public K getKey() {
    return key;
  }

  public void setKey(K key) {
    this.key = key;
  }
}
