package com.bonc.dw3.service;

import com.bonc.dw3.bean.DataObject;
import org.apache.hadoop.hbase.client.Get;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.CrossOrigin;

import java.util.List;

/**
 * Created by zg on 2017/8/14.
 */
@Service
@CrossOrigin(origins = "*")
public interface QueryDataService {
    List<String> queryData(List<Get> keyList, DataObject dataObject);
    List<String> queryRedisData(List<String> keyList);
}
