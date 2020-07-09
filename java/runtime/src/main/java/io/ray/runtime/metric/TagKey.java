package io.ray.runtime.metric;

import com.google.common.base.Preconditions;

/**
 * Tagkey mapping java object to stats tagkey object.
 */
public class TagKey {

  /**
   * Raw pointer for tag key.
   */
  private long tagKeyPointer = 0L;

  private String tagKey;

  public TagKey(String key) {
    tagKey =  key;
    tagKeyPointer = registerTagkeyNative(key);
    Preconditions.checkState(tagKeyPointer != 0,
      "Tagkey native pointer must not be 0.");
  }

  private native long registerTagkeyNative(String tagKey);

  /**
   * Destroy raw object from stats.
   */
  public void unregisterTagKey() {
    if (0 != tagKeyPointer) {
      unregisterTagKey(tagKeyPointer);
    }
    tagKeyPointer = 0;
  }

  private native void unregisterTagKey(long tagKeyPtr);

  public long getNativePointer() {
    return tagKeyPointer;
  }

  @Override
  public String toString() {
    return "TagKey{" +
      "tagKeyPointer=" + tagKeyPointer +
      ", tagKey='" + tagKey + '\'' +
      '}';
  }
}