package io.ray.runtime.serialization.util;

import java.util.Arrays;

/**
 * A resizable, ordered or unordered int array. Avoids the boxing that occurs with {@code
 * ArrayList<Integer>}.
 *
 * @author Nathan Sweet
 */
public class IntArray {
  public int[] items;
  public int size;
  public boolean ordered;

  /** Creates an ordered array with the specified capacity. */
  public IntArray(int capacity) {
    this(true, capacity);
  }

  /**
   * Create an int array.
   *
   * @param ordered If false, methods that remove elements may change the order of other elements in
   *     the array, which avoids a memory copy.
   * @param capacity Any elements added beyond this will cause the backing array to be grown.
   */
  public IntArray(boolean ordered, int capacity) {
    this.ordered = ordered;
    items = new int[capacity];
  }

  public void add(int value) {
    int[] items = this.items;
    if (size == items.length) {
      items = resize(Math.max(8, (int) (size * 1.75f)));
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

  private int[] resize(int newSize) {
    int[] newItems = new int[newSize];
    int[] items = this.items;
    System.arraycopy(items, 0, newItems, 0, Math.min(items.length, newItems.length));
    this.items = newItems;
    return newItems;
  }

  public String toString() {
    return Arrays.toString(Arrays.copyOfRange(items, 0, size));
  }
}
