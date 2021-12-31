package io.ray.runtime.serialization.util;

import java.util.Arrays;

/** A resizable int array which avoids the boxing that occurs with {@code ArrayList<Integer>}. */
public class IntArray {
  public int[] items;
  public int size;

  public IntArray(int capacity) {
    items = new int[capacity];
  }

  public void add(int value) {
    int[] items = this.items;
    if (size == items.length) {
      int newSize = size * 2;
      int[] newItems = new int[newSize];
      System.arraycopy(items, 0, newItems, 0, items.length);
      this.items = newItems;
      items = newItems;
    }
    items[size++] = value;
  }

  public int get(int index) {
    if (index >= size) {
      throw new IndexOutOfBoundsException(String.valueOf(index));
    }
    return items[index];
  }

  public void set(int index, int value) {
    if (index >= size) {
      throw new IndexOutOfBoundsException(String.valueOf(index));
    }
    items[index] = value;
  }

  /** Removes and returns the last item. */
  public int pop() {
    return items[--size];
  }

  public void clear() {
    size = 0;
  }

  public String toString() {
    return Arrays.toString(Arrays.copyOfRange(items, 0, size));
  }
}
