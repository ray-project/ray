package org.ray.runtime.gcs;

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
    String[] ipAndPort = redisAddress.split(":");
    if (ipAndPort.length != 2) {
      throw new IllegalArgumentException("The argument redisAddress " +
          "should be formatted as ip:port.");
    }

    JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
    jedisPoolConfig.setMaxTotal(JEDIS_POOL_SIZE);
    jedisPool = new JedisPool(jedisPoolConfig, ipAndPort[0],
        Integer.parseInt(ipAndPort[1]), 30000);
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

  public String get(final String key, final String field) {
    try (Jedis jedis = jedisPool.getResource()) {
      if (field == null) {
        return jedis.get(key);
      } else {
        return jedis.hget(key, field);
      }
    }

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

}
