package com.bonc.dw3.common.util;

import com.bonc.dw3.common.hbase.HBasePool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zq on 2016/12/12.
 */
public class HbaseUtils {
    private static Logger log = LoggerFactory.getLogger(HbaseUtils.class);

    private static Configuration create = null;
    public static HConnection hcon = null ;
    public static HBasePool hBasePool=new HBasePool();
    public static Configuration configuration;
    public static void getCreate() throws IOException {
//        if(null == hcon){
//            create= hBasePool.getConfiguration("/hbase_ks");
//            hcon = hBasePool.getHConnection("/hbase_ks",create);


            configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", "10.249.216.113:2181,10.249.216.114:2181,10.249.216.115:2181");
            configuration.set("zookeeper.znode.parent", "/hbase");
      //  }
    }

    public static Get generateGet(String hashKey, String rowKey){
        byte[] rowKeyByte = Bytes.toBytes((short) (hashKey.hashCode() & 0xFFF));//rowkey添加hash值(0-0xFFF)用来使用预分区,注意只取第0个字段的，与查询对应起来
        rowKeyByte = Bytes.add(rowKeyByte, Bytes.toBytes(rowKey));
        Get get = new Get(rowKeyByte);
        get.addColumn(Bytes.toBytes("f"),Bytes.toBytes("q"));
        return get;
    }

    public static List<String> getList(String tableName,List<Get> getList)
            throws Exception {
       // HTableInterface table= hcon.getTable(tableName);
        HTable table = null;//HTable一个线程一个实例
        table = new HTable(configuration, tableName);
        Result[] result = table.get(getList);

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
                            //log.info("hbase value " + value);
                            resultList.add(value);
                        }
                    }
                }
            }
            table.close();
            return resultList;
        }
    }
    public static List<String> getListByHtable(String tableName,List<Get> getList)
            throws Exception {
        //HTableInterface table= hcon.getTable(tableName);



        HTable table = null;//HTable一个线程一个实例
        table = new HTable(configuration, tableName);
        Result[] result = table.get(getList);

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
            table.close();
            return resultList;
        }
    }
    public static List<Get> getIMSIListHashcode(String tableName,List<Get> getList,String Pre)
            throws Exception {
        HTableInterface table= hcon.getTable(tableName);
        byte [] rowkey=null;
        Result[] result = table.get(getList);
        List<Get> resultList = new ArrayList<>();
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
                            rowkey = Bytes.toBytes((short) ((Pre+"#"+value).hashCode() & 0xFFF));//rowkey添加hash值(0-0xFFF)用来使用预分区,注意只取第0个字段的，与查询对应起来
                            rowkey = Bytes.add(rowkey, Bytes.toBytes(Pre+"#"+value));
                            Get get= new Get(rowkey);
                            resultList.add(get);
                        }
                    }
                }

            }
            table.close();
            return resultList;
        }
    }

    public static String getResult(String tableName, String firstKey,String lastKey)
            throws IOException {
//        HTableInterface table= hcon.getTable(tableName);
        HTable table = null;//HTable一个线程一个实例
        table = new HTable(configuration, tableName);
        byte[] temp=null;
        temp = Bytes.toBytes((short) (firstKey.hashCode() & 0xFFF));//rowkey添加hash值(0-0xFFF)用来使用预分区,注意只取第0个字段的，与查询对应起来
        temp = Bytes.add(temp, Bytes.toBytes(lastKey));
        Get get = new Get(temp);
        Result result = table.get(get);
        if(result==null||result.isEmpty()||result.equals("")){
            table.close();
            return "";
        }else{
            //存放查询结果
            String value=null;
            for(Cell cell:result.listCells()) {
                value=new String(CellUtil.cloneValue(cell));
            }
            table.close();
            return value;
        }
    }
    /*
      * 按照前缀查询遍历查询hbase表
      *
      * @tableName 表名
      */
    public static List<String> scanPreRow(String tableName, String hashKey, String lastKey ) throws IOException {
        byte[] startTemp=null;
        startTemp = Bytes.toBytes((short) (hashKey.hashCode() & 0xFFF));//rowkey添加hash值(0-0xFFF)用来使用预分区,注意只取第0个字段的，与查询对应起来
        startTemp = Bytes.add(startTemp, Bytes.toBytes(lastKey));
        Scan scan = new Scan();
        scan.setCaching(500000);
        scan.setCacheBlocks(false);
        scan.addColumn(Bytes.toBytes("f"),Bytes.toBytes("q"));
        ResultScanner rs = null;
        HTableInterface table=null;

        table=hcon.getTable(tableName);
        //按照前缀查询
        PrefixFilter filter = new PrefixFilter(startTemp);
        scan.setFilter(filter);
        //存放查询结果
        List<String>  resultList=new ArrayList<String>();
        try {
            rs = table.getScanner(scan);
            for (Result r : rs) {
                if (r == null ||r.isEmpty() ||  r.equals("")) {

                } else {
                    for (Cell cell : r.listCells()) {
                        //条件都满足才添加
                        if(CellUtil.cloneValue(cell)!=null) {
                            resultList.add(new String(CellUtil.cloneRow(cell)));
                        }

                    }
                }
            }
        } finally {
            rs.close();
        }
        return resultList;
    }

    public static List<String> scanPreValue(String tableName,String firstKey,String lastKey) throws IOException {
        byte[] startTemp=null;
        startTemp = Bytes.toBytes((short) (firstKey.hashCode() & 0xFFF));//rowkey添加hash值(0-0xFFF)用来使用预分区,注意只取第0个字段的，与查询对应起来
        startTemp = Bytes.add(startTemp, Bytes.toBytes(lastKey));
        Scan scan = new Scan();
        scan.setCaching(500000);
        scan.setCacheBlocks(false);
        scan.addColumn(Bytes.toBytes("f"),Bytes.toBytes("q"));
        ResultScanner rs = null;
        HTableInterface table=null;
        table=hcon.getTable(tableName);
        //按照前缀查询
        PrefixFilter filter = new PrefixFilter(startTemp);
        scan.setFilter(filter);
        //存放查询结果
        List<String>  resultList=new ArrayList<String>();
        try {
            rs = table.getScanner(scan);

           for (Result r : rs) {
                if(r==null||r.isEmpty()||r.equals("")){

                }else {
                    for (Cell cell : r.listCells()) {
                        resultList.add(new String(CellUtil.cloneValue(cell)));

                    }
                }
            }
        } finally {
            rs.close();
        }
        return resultList;
    }

    /*
      * 按照前缀查询遍历查询hbase表
      *
      * @tableName 表名
      */
    public static List<String> scanPreKey(String tableName,String firstKey,String lastKey) throws IOException {
        byte[] startTemp=null;
        startTemp = Bytes.toBytes((short) (firstKey.hashCode() & 0xFFF));//rowkey添加hash值(0-0xFFF)用来使用预分区,注意只取第0个字段的，与查询对应起来
        startTemp = Bytes.add(startTemp, Bytes.toBytes(lastKey));
        Scan scan = new Scan();
        scan.setCacheBlocks(false);
        scan.addColumn(Bytes.toBytes("f"),Bytes.toBytes("q"));
        ResultScanner rs = null;
        HTableInterface table=null;
        table= hcon.getTable(tableName);
        //按照前缀查询
        PrefixFilter filter = new PrefixFilter(startTemp);
        scan.setFilter(filter);
        //存放查询结果
        List<String>  resultList=new ArrayList<String>();
        try {
            rs = table.getScanner(scan);
            if(rs==null||rs.equals("")){
                return null;
            }else {
                for (Result r : rs) {
                    for (Cell cell : r.listCells()) {
                        resultList.add(Bytes.toString(CellUtil.cloneRow(cell)));
                    }
                }
            }
        } finally {
            rs.close();
        }
        return resultList;
    }
    /*
* 按照倒序查询查询hbase表并返回最后一条记录
*
* @tableName 表名
*/
//    public static List<String>  getReversed(String tableName) throws IOException {
//        String  resultArray=null;
//        List<String> resultArray
//        Scan scan = new Scan();
//        scan.setReversed(true);
//        scan.setCaching(1);
//        scan.setMaxResultSize(1);
//        ResultScanner rs = null;
//        HTableInterface table=null;
//        table= hcon.getTable(tableName);
//        Filter filter = new PageFilter(1);
//        scan.setFilter(filter);
//        try {
//            rs = table.getScanner(scan);
//            if(rs==null||rs.equals("")){
//                return null;
//            }else {
//                for (Result r : rs) {
//                    for (KeyValue kv : r.list()) {
//                        resultArray = Bytes.toString(kv.getRow());
//
//                    }
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }finally {
//            rs.close();
//        }
//        return resultArray;
//    }

    public static String getResultNonehash(String tableName,String Key)
            throws IOException {
       // HTableInterface table= hcon.getTable(tableName);
        HTable table = null;//HTable一个线程一个实例
        table = new HTable(configuration, tableName);
        byte[] temp=null;
        temp = Bytes.toBytes(Key);//rowkey添加hash值(0-0xFFF)用来使用预分区,注意只取第0个字段的，与查询对应起来
        Get get = new Get(temp);
        Result result = table.get(get);
        if(result==null||result.isEmpty()||result.equals("")){
            table.close();
            return "";
        }else{
            //存放查询结果
            String value=null;
            for(Cell cell:result.listCells()) {
                value=new String(CellUtil.cloneValue(cell));
            }
            table.close();
            return value;
        }
    }

    //拿到最大账期
    public static List<String> getReversed(String tableName) throws IOException {
        List<String>  resultArray=new ArrayList<>();
        Scan scan = new Scan();
        scan.setReversed(true);
        scan.setCaching(1);
        scan.setMaxResultSize(1);
        ResultScanner rs = null;
        HTable table = null;//HTable一个线程一个实例
        table = new HTable(configuration, tableName);
        //table= hcon.getTable(tableName);

        Filter filter = new PageFilter(1);
        scan.setFilter(filter);
        try {
            rs = table.getScanner(scan);
            if(rs==null||rs.equals("")){
                return null;
            }else {
                for (Result r : rs) {
                    for (KeyValue kv : r.list()) {
                        resultArray.add( Bytes.toString(kv.getRow()));
                        resultArray.add( Bytes.toString(kv.getValue()));
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            rs.close();
        }
        return resultArray;
    }

}