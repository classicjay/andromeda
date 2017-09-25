package com.bonc.dw3.common.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Component
@org.springframework.context.annotation.Configuration
public class HBasePool implements EnvironmentAware {
    @Autowired
    private static Environment ev;
    //客服查询
    private static  String QUORUM;
    private static  String PORT;
    private static  String[] ZKROOT;

    private static Map<String, HConnection> connectionMap = new HashMap<String, HConnection>();
    private static Map<String, Configuration> confMap = new HashMap<String, Configuration>();

    public HBasePool() {
        setEnvironment(ev);
    }
    @Override
    public  void  setEnvironment(Environment environment) {
        this.ev=environment;
    }
    public synchronized   Configuration getConfiguration(String zkRoot) {
        if (confMap == null || confMap.size() == 0) {
            QUORUM=ev.getProperty("QUORUM");
            PORT=ev.getProperty("PORT");
            ZKROOT=ev.getProperty("ZKROOT").split(",");
            for (String zkroot : ZKROOT) {
                Configuration create = HBaseConfiguration.create();
                create.set("hbase.zookeeper.quorum", QUORUM);
                create.set("hbase.zookeeper.property.clientPort", PORT);
                create.set("hbase.client.retries.number", "4");
                create.set("zookeeper.session.timeout", "900000");
                create.set("zookeeper.znode.parent", zkroot);
                confMap.put(zkroot, create);
            }
        }
        return confMap.get(zkRoot);
    }

    public synchronized   HConnection getHConnection(String zkRoot,Configuration conf)
            throws IOException {
        if (connectionMap == null || connectionMap.size() == 0) {
            for (String zkroot : ZKROOT) {
                connectionMap.put(zkroot, HConnectionManager
                        .createConnection(conf));
            }
        }
        return connectionMap.get(zkRoot);
    }
}
