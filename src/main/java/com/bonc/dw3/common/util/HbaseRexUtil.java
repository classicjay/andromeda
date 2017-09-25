package com.bonc.dw3.common.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by chou on 2017/8/15.
 */
public class HbaseRexUtil {

    private static Logger log = LoggerFactory.getLogger(HbaseRexUtil.class);

    public static HConnection conn = null;
    private static Configuration conf = null;
    public static synchronized Configuration getConfiguration() {
        if(conf == null)
        {
            conf =  HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "10.249.216.113:2181,10.249.216.114:2181,10.249.216.115:2181");
            conf.set("zookeeper.znode.parent", "/hbase");
        }
        return conf;
    }

    public static synchronized HConnection getHConnection() throws IOException
    {
        if(conn == null)
        {
            conn = HConnectionManager.createConnection(getConfiguration());
        }
        return conn;
    }

    public static Get generateGet(String hashKey, String rowKey){
        byte[] rowKeyByte = Bytes.toBytes((short) (hashKey.hashCode() & 0xFFF));//rowkey添加hash值(0-0xFFF)用来使用预分区,注意只取第0个字段的，与查询对应起来
        rowKeyByte = Bytes.add(rowKeyByte, Bytes.toBytes(rowKey));
        Get get = new Get(rowKeyByte);
        get.addColumn(Bytes.toBytes("f"),Bytes.toBytes("q"));
        return get;
    }

    public static List<String> getListByHtable(String tableName,List<Get> getList) throws Exception {
        conn = getHConnection();
        /*Configuration configuration;//Hbase配置类
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "10.249.216.113:2181,10.249.216.114:2181,10.249.216.115:2181");
        configuration.set("zookeeper.znode.parent", "/hbase");
        HConnection conn = HConnectionManager.createConnection(configuration);*/
        HTableInterface table = conn.getTable(tableName);

        long time1 = System.currentTimeMillis();
       /* HTable table = null;//HTable一个线程一个实例
        table = new HTable(configuration, tableName);*/

        long time2 = System.currentTimeMillis();
        Result[] result = table.get(getList);
        //System.out.println("Htable链接时间为："+(System.currentTimeMillis()-time1)+"ms");
        List<String> resultList = new ArrayList<>();
        if(result==null||result.equals("")){
            table.close();
            return resultList;
        }else{
            for (Result r : result) {
                if(r==null||r.isEmpty()||r.equals("")){
                }else{
                    for(Cell cell:r.listCells()) {
                        String value=null;
                        value=new String(CellUtil.cloneValue(cell));
                        if(value!=null&&value.length()!=0){
                            resultList.add(value);
                        }
                    }
                }

            }
            //log.debug("###############这个线程读取消耗时间为："+(System.currentTimeMillis()-time2)+"ms,记录条数为"+resultList.size());
            table.close();
            return resultList;
        }
    }
    private static void main(String[] args) throws Exception {

        List<String> rs=new ArrayList<>();
        List <Get> getList=new ArrayList<>();

        BufferedReader br = new BufferedReader(new FileReader(args[0]));
        String tableName = args[1];
        int count = 0;
        byte [] temp =null;
        String str = null;
        int threadCount = Integer.parseInt(args[2]);
        MyThread[] threadArr = new MyThread[threadCount];
        while (null != (str = br.readLine())){
            String[] lineStr = str.toString().split(" ");
            StringBuilder sb = new StringBuilder();
            sb.append(lineStr[1]);
            String[] res=sb.toString().split("#",-1);
            String needHashCode = "DAY"+"#"+res[1]+"#"+res[2]+"#"+res[3]+"#"+res[4]+"#"+res[5];
            String lastrowkey= "DAY"+"#"+res[1]+"#"+res[2]+"#"+res[3]+"#"+res[4]+"#"+res[5]+"#"+res[6]+"#"+res[7]+"#"+res[8];
            temp = Bytes.toBytes((short) (needHashCode.hashCode() & 0xFFF));//rowkey添加hash值(0-0xFFF)用来使用预分区,注意只取第0个字段的，与查询对应起来
            temp = Bytes.add(temp, Bytes.toBytes(lastrowkey));
            Get get =new Get(temp);
            get.addColumn(Bytes.toBytes("f"),Bytes.toBytes("q"));
            getList.add(get);
            count++;
        }
        System.out.println("总共读文件"+count+"row");

        int avg = count/threadCount;

        int start = 0;
        for (int i=0;i<threadCount;i++){
            List<Get> listTmp = new ArrayList<>();
            if (i==threadCount-1){
                listTmp = getList.subList(start,count);
                threadArr[i] = new MyThread(listTmp,tableName);
                threadArr[i].start();
                //System.out.println(start+"###"+count);
                break;
            }
            listTmp = getList.subList(start,start+avg);
            threadArr[i] = new MyThread(listTmp,tableName);
            threadArr[i].start();
            //System.out.println(start+"###"+(start+avg));
            start = start+avg;
        }

        //等主线程完毕
        for (int i=0;i<threadCount;i++){
            threadArr[i].join();
        }

        conn.close();

    }
}
