package com.bonc.dw3.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

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
        JedisCluster jc = new JedisCluster(nodes);
        JedisClusterPipeline jcp = JedisClusterPipeline.pipelined(jc);
        jcp.refreshCluster();
        List<Object> batchResult = null;
        for (String key:keyList){
            jcp.get(key);
        }
        batchResult = jcp.syncAndReturnAll();
        return batchResult;
    }

//    public static void main(String[] args) {
//        List<String> list = Arrays.asList(new String[]{"DAY#20170806#CKP_23353#010#V0152302#**#20AAAAAA#10AA#881","DAY#20170817#CKP_23353#038#V0350600#**#30AAAAAA#99AA#01"});
//        List<Object> result = getResult(list);
//        for (Object object: result){
//            if (null!=object){
//                System.out.println(String.valueOf(object));
//            }
//
//        }
//
//    }
}
