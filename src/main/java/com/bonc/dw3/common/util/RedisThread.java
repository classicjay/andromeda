package com.bonc.dw3.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zg on 2017/9/26.
 */
public class RedisThread  extends Thread {
    private Logger log = LoggerFactory.getLogger(RedisThread.class);


    List<String> list;
    JedisClusterPipeline jcp;
    JedisCluster jedisCluster;
    List<String> rs = new ArrayList<>();
    List<Object> result = null;

    public RedisThread(List<String> list, JedisCluster jedisCluster){
        this.list = list;
        this.jedisCluster = jedisCluster;
    }

    @Override
    public void run() {
        //List<Object> result = new ArrayList<>();
        JedisClusterPipeline jcp = JedisClusterPipeline.pipelined(jedisCluster);
        long start = System.currentTimeMillis();
        //System.out.println(list.size());
        for(int i = 0;i<list.size();i++){
            jcp.get(list.get(i));
        }
        result = jcp.syncAndReturnAll();
        jcp.close();
        //System.out.println("线程耗时"+(System.currentTimeMillis()-start)+"ms,有"+result.size()+"条返回结果");
    }
}
