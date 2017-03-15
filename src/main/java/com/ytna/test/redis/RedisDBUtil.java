package com.ytna.test.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

/**
 * 
 * @author lichao
 * 满足集群分片
 */
public class RedisDBUtil extends RedisSource{

	private static Logger logger = Logger.getLogger(RedisDBUtil.class);
	private static ShardedJedisPool pool;
	static {
		logger.info("开始初始化redis连接池");
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxActive(Integer.valueOf(max_active));
		config.setMaxIdle(Integer.valueOf(max_idle));
		config.setMaxWait(Integer.valueOf(max_wait));
		config.setTestOnBorrow(Boolean.valueOf(isTest));
		List<JedisShardInfo> list = new ArrayList<JedisShardInfo>();
		String[] addressArr = redis_address.split(",");
		for (String str : addressArr) {
			JedisShardInfo shardInfo = new JedisShardInfo(str.split(":")[0], Integer.parseInt(str.split(":")[1]),str.split(":")[2]);
			shardInfo.setTimeout(Integer.valueOf(timeout));
			list.add(shardInfo);
		}
		pool = new ShardedJedisPool(config, list);
		logger.info("成功初始化redis连接池");
	}
	
	public static ShardedJedis getRedisTemplate() {
		ShardedJedis shardedJedis = pool.getResource();
		return shardedJedis;
	}
	
	public static void setValue(byte[] key, byte[] value) {
		ShardedJedis jedis = null;
		try {
			jedis = RedisDBUtil.getRedisTemplate();
			jedis.set(key, value);
			RedisDBUtil.closeJedis(jedis);
		} catch (Exception e) {
			e.printStackTrace();
			RedisDBUtil.closeBreakJedis(jedis);
		}
	}
	
	public static void delKey(String key) {
		ShardedJedis jedis = null;
		try {
			jedis = RedisDBUtil.getRedisTemplate();
			jedis.del(key);
			RedisDBUtil.closeJedis(jedis);
		} catch (Exception e) {
			e.printStackTrace();
			RedisDBUtil.closeBreakJedis(jedis);
		}
	}
	
	public static void setExpireValue(byte[] key, byte[] value, int second){
		ShardedJedis jedis = null;
		try {
			jedis = RedisDBUtil.getRedisTemplate();
			jedis.set(key, value);
			jedis.expire(key, second);
			RedisDBUtil.closeJedis(jedis);
		} catch (Exception e) {
			e.printStackTrace();
			RedisDBUtil.closeBreakJedis(jedis);
		}
	}
	
	public static void setHashValue(String mapName, String key, String value){
		logger.info("调用写入redis");
		ShardedJedis jedis = null;
		try {
			jedis = RedisDBUtil.getRedisTemplate();
			jedis.hset(mapName, key, value);
			RedisDBUtil.closeJedis(jedis);
		} catch (Exception e) {
			logger.info("调用写入redis失败");
			e.printStackTrace();
			RedisDBUtil.closeBreakJedis(jedis);
		}
	}
	
	public static void delHashValue(String mapName, String key){
		ShardedJedis jedis = null;
		try {
			jedis = RedisDBUtil.getRedisTemplate();
//			jedis.hset(mapName, key, value);
			jedis.hdel(mapName,key);
			RedisDBUtil.closeJedis(jedis);
		} catch (Exception e) {
			e.printStackTrace();
			RedisDBUtil.closeBreakJedis(jedis);
		}
	}
	
	public static byte[] getValue(byte[] key) {
		ShardedJedis jedis = null;
		byte[] re = null;
		try {
			jedis = RedisDBUtil.getRedisTemplate();
			re = jedis.get(key);
			RedisDBUtil.closeJedis(jedis);
		} catch (Exception e) {
			e.printStackTrace();
			RedisDBUtil.closeBreakJedis(jedis);
		}
		return re;
	}
	
	public static String getHashValue(String mapName, String key){
		ShardedJedis jedis = null;
		String re = null;
		try {
			jedis = RedisDBUtil.getRedisTemplate();
			re = jedis.hget(mapName, key);
			RedisDBUtil.closeJedis(jedis);
		} catch (Exception e) {
			e.printStackTrace();
			RedisDBUtil.closeBreakJedis(jedis);
		}
		return re;
	}
	
	public static Map<String, String> getAllKeys(String mapName){
		ShardedJedis jedis = null;
		Map<String, String> re = null;
		try {
			jedis = RedisDBUtil.getRedisTemplate();
			re = jedis.hgetAll(mapName);
			RedisDBUtil.closeJedis(jedis);
		} catch (Exception e) {
			e.printStackTrace();
			RedisDBUtil.closeBreakJedis(jedis);
		}
		return re;
	}
	
	/**
	 * 
	 * @param collectionName缓存的集合名称
	 * @param key缓存值的key
	 * @param value缓存值
	 * @return 
	 */
	public static Boolean existKey(String collectionName) {
		ShardedJedis jedis = null;
		try {
			jedis = RedisDBUtil.getRedisTemplate();
			return jedis.exists(collectionName);
		} catch (Exception e) {
			e.printStackTrace();
			RedisDBUtil.closeBreakJedis(jedis);
		}
		return false;
	}
	
	/**
	 * 
	 * @param collectionName缓存的集合名称
	 * @param key缓存值的key
	 * @param value缓存值
	 */
	public static void setValueExpire(String collectionName, String key, String value,String keyid,int time) {
		ShardedJedis jedis = RedisDBUtil.getRedisTemplate();
		try {
			jedis.hset(collectionName, key, value);
			jedis.expire(keyid, time);
			//jedis.expire("obd_oil_id_cache_map", 3600);
			RedisDBUtil.closeJedis(jedis);
		} catch (Exception e) {
			e.printStackTrace();
			RedisDBUtil.closeBreakJedis(jedis);
		}
	}
	
	
	/**
	 * 正常连接池回收
	 * @param jedis
	 */
	public static void closeJedis(ShardedJedis jedis){
		   if(jedis!=null){
			   pool.returnResource(jedis);
		   }
	}
	
	/**
	 * 异常连接池回收
	 * @param jedis
	 */
	public static void closeBreakJedis(ShardedJedis jedis){
		if(jedis!=null){
			pool.returnBrokenResource(jedis);
		}
	}
}
