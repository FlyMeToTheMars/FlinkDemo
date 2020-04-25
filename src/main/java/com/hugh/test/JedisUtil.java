package com.hugh.test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @program: FlinkDemo
 * @description: test redis
 * @author: Fly.Hugh
 * @create: 2020-03-23 00:42
 **/
public class JedisUtil {
    public static void main(String[] args) {
        //ip地址，端口号
        Jedis jedis = cli_single("192.168.229.132", 6379);
        jedis.set("key", "first Java connect!");
        String value = jedis.get("key");
        System.out.println(value);
    }

    /**
     * 单个连接
     *
     * @param host
     * @param port
     * @return
     */
    public static Jedis cli_single(String host, int port) {
        try {
            return new Jedis(host, port);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 连接池
     *
     * @param host
     * @param port
     * @return
     */
    public static Jedis cli_pool(String host, int port) {
        JedisPoolConfig config = new JedisPoolConfig();
        // 最大连接数
        config.setMaxTotal(10);
        // 最大连接空闲数
        config.setMaxIdle(2);
        JedisPool jedisPool = new JedisPool(config, host, port);
        try{

            return jedisPool.getResource();
        }catch(Exception e){
            e.printStackTrace();
            return null;
        }
    }
}
