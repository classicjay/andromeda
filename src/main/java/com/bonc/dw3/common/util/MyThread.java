package com.bonc.dw3.common.util;

import org.apache.hadoop.hbase.client.Get;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class MyThread extends Thread {

    private static Logger log = LoggerFactory.getLogger(MyThread.class);


    List<Get> listGet;
    String tableName;
    public List<String> rs;

    public MyThread(List<Get> listGet,String tableName){
        this.listGet = listGet;
        this.tableName = tableName;
    }

    @Override
    public void run() {
        //List<String> rs=new ArrayList<>();
        try {
            //long time=System.currentTimeMillis();
            this.rs=HbaseRexUtil.getListByHtable(tableName,listGet);
            //System.out.println("线程：[],时间为："+(System.currentTimeMillis()-time)+"ms");
            //System.out.println("result from hbase:...." + this.rs);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
