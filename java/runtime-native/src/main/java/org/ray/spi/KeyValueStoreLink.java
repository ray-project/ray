package org.ray.spi;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Ray K/V abstraction
 */
public interface KeyValueStoreLink {

  /**
   * set address of kv store: format "ip:port"
   */
  void SetAddr(String addr);

  /**
   * check if the kvstore client connected
   */
  void CheckConnected() throws Exception;

  /**
   * Set Key-value into State Store, such as redis
   *
   * @param key the key to set
   * @param value the value to set
   * @param field the field is being set when the item is a hash If it is not hash field should be
   * filled with null
   * @return If the key(or field) already exists, and the StateStoreSet just produced an update of
   * the value, 0 is returned, otherwise if a new key(or field) is created 1 is returned.
   */
  Long Set(final String key, final String value, final String field);

  Long Set(final byte[] key, final byte[] value, final byte[] field);

  /**
   * multi hash value set
   *
   * @param key the key in kvStore
   * @param hash the multi hash value to be set
   * @return Return OK or Exception if hash is empty
   */
  String Hmset(final String key, final Map<String, String> hash);

  String Hmset(final byte[] key, final Map<byte[], byte[]> hash);

  /**
   * multi hash value get
   *
   * @param key the key in kvStore
   * @param fields the fields to be get
   * @return Multi Bulk Reply specifically a list of all the values associated with the specified
   * fields, in the same order of the request.
   */
  List<String> Hmget(final String key, final String... fields);

  List<byte[]> Hmget(final byte[] key, final byte[]... fields);

  /**
   * Get the value of the specified key from State Store
   *
   * @param key the key to get
   * @param field the field is being got when the item is a hash If it is not hash field should be
   * filled with null
   * @return Bulk reply If the key does not exist null is returned.
   */
  String Get(final String key, final String field);

  byte[] Get(final byte[] key, final byte[] field);

  /**
   * Delete the key(or the specified field of the key) from State Store
   *
   * @param key the key to delete
   * @param field the field is to delete when the item is a hash If it is not hash field should be
   * filled with null
   * @return Integer reply, specifically: an integer greater than 0 if the key(or the field) was
   * removed 0 if none of the specified key existed
   */
  Long Delete(final String key, final String field);

  Long Delete(final byte[] key, final byte[] field);

  /**
   * get all keys which fit the pattern
   */
  Set<byte[]> Keys(final byte[] pattern);

  /**
   * get all keys which fit the pattern
   */
  Set<String> Keys(String pattern);

  /**
   * get all hash of the key
   */
  Map<byte[], byte[]> hgetAll(final byte[] key);

  /**
   * Return the specified elements of the list stored at the specified key.
   *
   * @return Multi bulk reply, specifically a list of elements in the specified range.
   */
  List<String> Lrange(final String key, final long start, final long end);

  /**
   * @return Integer reply, specifically, the number of elements inside the list after the push
   * operation.
   */
  Long Rpush(final String key, final String... strings);

  Long Rpush(final byte[] key, final byte[]... strings);

  /**
   *
   * @param channel To which channel the message will be published
   * @param message What to publish
   * @return the number of clients that received the message
   */
  Long Publish(final String channel, final String message);

  Long Publish(byte[] channel, byte[] message);

  Object GetImpl();
}
