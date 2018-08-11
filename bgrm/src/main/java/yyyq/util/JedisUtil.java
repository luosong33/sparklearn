package yyyq.util;

import java.util.HashMap;
import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisUtil
{
 
    private JedisUtil(){}
 
    private static class RedisUtilHolder{
        private static final JedisUtil instance = new JedisUtil();
    }
 
    public static JedisUtil getInstance(){
        return RedisUtilHolder.instance;
    }
 
    private static Map<String,JedisPool> maps = new HashMap<String,JedisPool>();
 
    private synchronized static JedisPool getPool(String ip, int port){
        String key = ip+":"+port;
        JedisPool pool = null;
        if(!maps.containsKey(key))
        {
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxIdle(RedisConfig.MAX_IDLE);
            config.setTestOnBorrow(true);
            config.setTestOnReturn(true);
 
            pool = new JedisPool(config,ip,port,RedisConfig.TIMEOUT,"yylc8888");
            maps.put(key, pool);
        }
        else
        {
            pool = maps.get(key);
        }
        return pool;        
    }
 
    public Jedis getJedis(String ip, int port)
    {
        Jedis jedis = null;
        int count = 0;
        do
        {
            try
            {
                jedis = getPool(ip,port).getResource();
            }
            catch (Exception e)
            {
                getPool(ip,port).returnBrokenResource(jedis);
            }
        }
        while(jedis == null && count<RedisConfig.RETRY_NUM);
        return jedis;
    }
 
    public  void closeJedis(Jedis jedis, String ip, int port){
        if(jedis != null)
        {
            getPool(ip,port).returnResource(jedis);
        }
    }
}

