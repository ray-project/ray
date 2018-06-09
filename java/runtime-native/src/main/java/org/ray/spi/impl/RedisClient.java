package org.ray.spi.impl;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.ray.spi.KeyValueStoreLink;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisClient implements KeyValueStoreLink {

  private String redisAddress;
  private JedisPool jedisPool;

  public RedisClient() {
  }

  public RedisClient(String addr) {
    setAddr(addr);
  }

  @Override
  public synchronized void setAddr(String addr) {
    if (StringUtils.isEmpty(redisAddress)) {
      redisAddress = addr;
      String[] ipPort = addr.split(":");
      JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
      //TODO NUM maybe equels to the thread num
      jedisPoolConfig.setMaxTotal(1);
      jedisPool = new JedisPool(jedisPoolConfig, ipPort[0], Integer.parseInt(ipPort[1]), 30000);
    }
  }

  @Override
  public void checkConnected() throws Exception {
    if (jedisPool == null) {
      throw new Exception("the GlobalState API can't be used before ray init.");
    }
  }

  @Override
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

  @Override
  public Long set(byte[] key, byte[] value, byte[] field) {
    try (Jedis jedis = jedisPool.getResource()) {
      if (field == null) {
        jedis.set(key, value);
        return (long) 1;
      } else {
        return jedis.hset(key, field, value);
      }
    }

  }

  @Override
  public String hmset(String key, Map<String, String> hash) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.hmset(key, hash);
    }

  }

  @Override
  public String hmset(byte[] key, Map<byte[], byte[]> hash) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.hmset(key, hash);
    }
  }

  @Override
  public List<String> hmget(String key, String... fields) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.hmget(key, fields);
    }
  }

  @Override
  public List<byte[]> hmget(byte[] key, byte[]... fields) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.hmget(key, fields);
    }
  }

  @Override
  public String get(final String key, final String field) {
    try (Jedis jedis = jedisPool.getResource()) {
      if (field == null) {
        return jedis.get(key);
      } else {
        return jedis.hget(key, field);
      }
    }

  }

  @Override
  public byte[] get(byte[] key, byte[] field) {
    try (Jedis jedis = jedisPool.getResource()) {
      if (field == null) {
        return jedis.get(key);
      } else {
        return jedis.hget(key, field);
      }
    }

  }

  @Override
  public Long delete(final String key, final String field) {
    try (Jedis jedis = jedisPool.getResource()) {
      if (field == null) {
        return jedis.del(key);
      } else {
        return jedis.hdel(key, field);
      }
    }

  }

  @Override
  public Long delete(byte[] key, byte[] field) {
    try (Jedis jedis = jedisPool.getResource()) {
      if (field == null) {
        return jedis.del(key);
      } else {
        return jedis.hdel(key, field);
      }
    }

  }

  @Override
  public Set<byte[]> keys(byte[] pattern) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.keys(pattern);
    }
  }

  @Override
  public Set<String> keys(String pattern) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.keys(pattern);
    }
  }

  @Override
  public Map<byte[], byte[]> hgetAll(byte[] key) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.hgetAll(key);
    }
  }

  @Override
  public List<String> lrange(String key, long start, long end) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.lrange(key, start, end);
    }
  }

  @Override
  public Long rpush(String key, String... strings) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.rpush(key, strings);
    }
  }

  @Override
  public Long rpush(byte[] key, byte[]... strings) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.rpush(key, strings);
    }
  }

  @Override
  public Long publish(String channel, String message) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.publish(channel, message);
    }
  }

  @Override
  public Long publish(byte[] channel, byte[] message) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.publish(channel, message);
    }
  }

  @Override
  public Object getImpl() {
    return jedisPool;
  }
}
