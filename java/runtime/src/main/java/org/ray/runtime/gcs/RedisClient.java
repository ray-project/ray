package org.ray.runtime.gcs;

import com.google.common.base.Strings;
import java.util.List;
import java.util.Map;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Redis client class.
 */
public class RedisClient {

  private static final int JEDIS_POOL_SIZE = 1;

  private JedisPool jedisPool;

  public RedisClient(String redisAddress) {
    this(redisAddress, null);
  }

  public RedisClient(String redisAddress, String password) {
    String[] ipAndPort = redisAddress.split(":");
    if (ipAndPort.length != 2) {
      throw new IllegalArgumentException("The argument redisAddress " +
          "should be formatted as ip:port.");
    }

    JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
    jedisPoolConfig.setMaxTotal(JEDIS_POOL_SIZE);

    if (Strings.isNullOrEmpty(password)) {
      jedisPool = new JedisPool(jedisPoolConfig,
          ipAndPort[0], Integer.parseInt(ipAndPort[1]), 30000);
    } else {
      jedisPool = new JedisPool(jedisPoolConfig, ipAndPort[0],
          Integer.parseInt(ipAndPort[1]), 30000, password);
    }
  }

  public Long set(final String key, final String value, final String field) {
    try (Jedis jedis = jedisPool.getResource()) {
      if (field == null) {
        jedis.set(key, value);
        return (long) 1;
      } else {
        return jedis.hset(key, field, value);
      }
    }
  }

  public String hmset(String key, Map<String, String> hash) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.hmset(key, hash);
    }
  }

  public Map<byte[], byte[]> hgetAll(byte[] key) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.hgetAll(key);
    }
  }

  public String get(final String key, final String field) {
    try (Jedis jedis = jedisPool.getResource()) {
      if (field == null) {
        return jedis.get(key);
      } else {
        return jedis.hget(key, field);
      }
    }
  }

  public byte[] get(byte[] key) {
    return get(key, null);
  }

  public byte[] get(byte[] key, byte[] field) {
    try (Jedis jedis = jedisPool.getResource()) {
      if (field == null) {
        return jedis.get(key);
      } else {
        return jedis.hget(key, field);
      }
    }
  }

  /**
   * Return the specified elements of the list stored at the specified key.
   *
   * @return Multi bulk reply, specifically a list of elements in the specified range.
   */
  public List<byte[]> lrange(byte[] key, long start, long end) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.lrange(key, start, end);
    }
  }

  /**
   * Whether the key exists in Redis.
   */
  public boolean exists(byte[] key) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.exists(key);
    }
  }

  public long incr(byte[] key) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.incr(key).intValue();
    }
  }
}
