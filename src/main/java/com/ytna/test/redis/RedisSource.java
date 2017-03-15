package com.ytna.test.redis;


import com.ytna.test.PropertiesUtil;

/**
 * 
 * @author lichao
 * redis的常量配置
 */
public class RedisSource {
	//redis的服务器地址，满足分片
	protected static final String redis_address= PropertiesUtil.getPropertiesByKey("redis.properties", "redis.server");
	//设置最大的连接数
	protected static final String max_active = PropertiesUtil.getPropertiesByKey("redis.properties", "redis.max_active");
	//设置池中最大的空闲数量
	protected static final String max_idle = PropertiesUtil.getPropertiesByKey("redis.properties", "redis.redis.max_idle");
	//连接等待的最大时间
	protected static final String max_wait = PropertiesUtil.getPropertiesByKey("redis.properties", "redis.max_wait");
	//连接超时时间
	protected static final String timeout = PropertiesUtil.getPropertiesByKey("redis.properties", "redis.timeout");
	//是否进行测试连接
	protected static final String isTest = PropertiesUtil.getPropertiesByKey("redis.properties", "redis.isTest");
}
