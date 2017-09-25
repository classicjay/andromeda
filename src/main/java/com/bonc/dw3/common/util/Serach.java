package com.bonc.dw3.common.util;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by chou on 2017/9/25.
 */
public class Serach {
    public static void main(String[] args) {
        Set<HostAndPort> nodes = new HashSet<HostAndPort>();
        nodes.add(new HostAndPort("10.249.216.109", 7000));
        nodes.add(new HostAndPort("10.249.216.116", 7000));
        nodes.add(new HostAndPort("10.249.216.117", 7000));
        JedisCluster jc = new JedisCluster(nodes);
        JedisClusterPipeline jcp = JedisClusterPipeline.pipelined(jc);
        jcp.refreshCluster();
        List<Object> batchResult = null;

            // batch read
            for (int i = 0; i <1; i++) {
                jcp.get("DAY#20170817#CKP_23353#038#V0350600#**#30AAAAAA#99AA#01");
            }
            batchResult = jcp.syncAndReturnAll();
        System.out.println(batchResult.get(0).toString());
    }
}
