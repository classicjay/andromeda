package com.bonc.dw3.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.util.*;

/**
 * <p>Title: BONC -  RedisUtils</p>
 * <p>Description:  </p>
 * <p>Copyright: Copyright BONC(c) 2013 - 2025 </p>
 * <p>Company: 北京东方国信科技股份有限公司 </p>
 *
 * @author zhaojie
 * @version 1.0.0
 */
public class RedisUtils {
    private static Logger log = LoggerFactory.getLogger(RedisUtils.class);

    private static Set<HostAndPort> nodes = new HashSet<>();
    static {
        nodes.add(new HostAndPort("10.249.216.109", 7000));
        nodes.add(new HostAndPort("10.249.216.116", 7000));
        nodes.add(new HostAndPort("10.249.216.117", 7000));
    }

    public static List<Object> getResult(List<String> keyList){
        JedisCluster jedisCluster = null;
        List<Object> batchResult = new ArrayList<>();
        try {
            jedisCluster = new JedisCluster(nodes);
            /*JedisClusterPipeline jcp = JedisClusterPipeline.pipelined(jc);
            jcp.refreshCluster();
            List<Object> batchResult = null;
            for (String key:keyList){
                jcp.get(key);
            }
            batchResult = jcp.syncAndReturnAll();*/
            int threadCount = 50;
            RedisThread[] threadArr = new RedisThread[threadCount];

            int count = keyList.size();
            int avg = count / threadCount;
            int start = 0;
            for (int i = 0; i < threadCount; i++) {
                List<String> listTmp = new ArrayList<>();
                if (i == threadCount - 1) {
                    listTmp = keyList.subList(start, count);
                    threadArr[i] = new RedisThread(listTmp, jedisCluster);
                    threadArr[i].start();
                    //System.out.println(start+"###"+count);
                    break;
                }
                listTmp = keyList.subList(start, start + avg);
                threadArr[i] = new RedisThread(listTmp, jedisCluster);
                threadArr[i].start();
                //System.out.println(start+"###"+(start+avg));
                start = start + avg;
            }

            for (int i = 0; i < threadCount; i++) {
                threadArr[i].join();
            }
            for (int i = 0; i < threadCount; i++) {
                if(null != threadArr[i]){
                    batchResult.addAll(threadArr[i].result);
                }
            }
            log.info("redis 结果数："+batchResult.size());
        }catch (Exception e){
            log.error("查询redis出错", e);
        }finally {
            if(jedisCluster != null){
                try {
                    jedisCluster.close();
                } catch (IOException e) {
                    log.error("", e);
                }

            }
        }
        return batchResult;
    }

}
