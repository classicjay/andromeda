package com.bonc.dw3.service;

import com.bonc.dw3.bean.DataObject;
import org.apache.hadoop.hbase.client.Get;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.CrossOrigin;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zg on 2017/8/14.
 */
@Service("QueryDataFromFileServiceImpl")
@CrossOrigin(origins = "*")
public class QueryDataFromFileServiceImpl implements QueryDataService{

    private static Logger log = LoggerFactory.getLogger(QueryDataFromFileServiceImpl.class);

    @Override
    public List<String> queryData(List<Get> keyList, DataObject dataObject) {
        List<String> resultList = new ArrayList<>();
        try {
            Resource resource = new ClassPathResource("dw3.0daytest.txt");

            File file = resource.getFile();
            FileReader fileReader = new FileReader(file);

            String str = null;
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            while((str = bufferedReader.readLine()) != null){
                resultList.add(str);
            }
            log.info("read file and result is " + resultList);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("", e);
        }
        return resultList;
    }
}
