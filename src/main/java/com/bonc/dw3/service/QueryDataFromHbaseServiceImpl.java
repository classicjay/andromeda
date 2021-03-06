package com.bonc.dw3.service;

import com.bonc.dw3.bean.DataObject;
import com.bonc.dw3.common.util.HbaseRexUtil;
import org.apache.hadoop.hbase.client.Get;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.CrossOrigin;

import java.util.List;

/**
 * Created by zg on 2017/8/14.
 */
@Service("QueryDataFromHbaseServiceImpl")
@CrossOrigin(origins = "*")
public class QueryDataFromHbaseServiceImpl implements QueryDataService{

    private static Logger log = LoggerFactory.getLogger(QueryDataFromHbaseServiceImpl.class);

    @Override
    public List<String> queryData(List<Get> keyByteList, DataObject dataObject) {
        //List<Get> keyList = new ArrayList<>();
        //2.调用查询，并发？
        List<String> hbaseResult = null;
        /*for(byte[] rowKey : keyByteList) {
            //log.info("query hbase and rowkey is " + rowKey);
            Get get = new Get(rowKey);
            keyList.add(get);
        }*/
        try {
            hbaseResult = HbaseRexUtil.getListByHtable(dataObject.getTableName(), keyByteList);
//            HbaseUtils.getCreate();
//            hbaseResult = HbaseUtils.getList(dataObject.getTableName(), keyByteList);
        } catch (Exception e) {
            log.error("query hbase failure", e);
        }
        //log.info("query hbase result " + hbaseResult);
        return hbaseResult;
    }

    //处理维度为空的情况
    private void isZeroSize(List<String> list) {
        if (list.size() == 0){
            list.add("**");
        }
    }
}
