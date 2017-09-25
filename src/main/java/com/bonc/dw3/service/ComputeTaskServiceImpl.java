package com.bonc.dw3.service;

import com.bonc.dw3.bean.DataObject;
import com.bonc.dw3.common.util.CommonUtils;
import com.bonc.dw3.common.util.DescartesUtils;
import com.bonc.dw3.common.util.HbaseUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Get;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.CrossOrigin;

import java.util.*;
import java.util.concurrent.Callable;

/**
 * Created by zg on 2017/8/14.
 */
@Service("ComputeTaskServiceImpl")
@CrossOrigin(origins = "*")
public class ComputeTaskServiceImpl implements ComputeTaskService {

    private static Logger log = LoggerFactory.getLogger(ComputeTaskServiceImpl.class);

    @Autowired
    @Qualifier("QueryDataFromHbaseServiceImpl")
    private QueryDataService queryDataService;

    @Autowired
    private ComputeService computeService;

    private static int step = 10000;

    @Override
    public List<Map<String, Object>> run(DataObject dataObject) {
        //1.调用工具类查hdfs数据，数据量打，并发查询
        //每个账期调用引擎计算，考虑并发调用
        Map<String, List<String>> whereMap = dataObject.getWhereMap();
        List<Get> keyList = generateKeyList(dataObject);
//        int cpuNum = Runtime.getRuntime().availableProcessors();
//        ExecutorService executor = new ThreadPoolExecutor(cpuNum, cpuNum * 2, 60000L,
//                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
//        List<Callable<Object>> taskList = new LinkedList<Callable<Object>>();
//        //如果查询key的数目大于100，则多线程查询
//        int keyListSize = keyList.size();
//
//        List<String> hbaseResult = new ArrayList<>();
//        if(keyListSize > step){
//            for(int i=0; i * step < keyListSize; i++){
//                List<Get> subList = keyList.subList(i*step, (i+1)*step > keyListSize ? keyListSize : (i+1)*step);
//                taskList.add(new QueryHbaseTask(subList, dataObject));
//            }
//            try {
//                List<Future<Object>> futures = executor.invokeAll(taskList);
//                for (Future<Object> future : futures) {
//                    List<String> result = (List<String>) future.get();
//                    if (null != result) {
//                        hbaseResult.addAll(result);
//                    }
//                }
//            } catch (InterruptedException e) {
//                log.error("", e);
//            } catch (ExecutionException e) {
//                log.error("", e);
//            }
//        }else {
//            hbaseResult = queryDataService.queryData(keyList, dataObject);
//        }
        List<String> hbaseResult = queryDataService.queryData(keyList, dataObject);

        if(hbaseResult!=null&&hbaseResult.size()>0){
            log.info("查询结果集："+hbaseResult.size());
        }
        //2.查询结果处理，本次循环遍历可以放到计算时做
        List<Map<String, Object>> resultSet = convertResult(dataObject, hbaseResult);
        if(resultSet!=null&&resultSet.size()>0){
            log.info("转换结果："+resultSet.get(0));
        }
        //3.调用计算框架计算，计算压力过大，并发计算convertResult
        List<Map<String, Object>> resultList = computeService.compute(dataObject, resultSet);
        if(resultList!=null&&resultList.size()>0){
            log.info("计算结果是："+resultList.get(0));
        }
        //4.处理结果，合并结果
        List<Map<String, Object>> completeList = processBlank(dataObject, resultList);
        return completeList;
    }

    /**
     * 根据where条件生成key
     * @param dataObject 数据对象
     * @return hbase的key列表
     */
    @Override
    public List<Get> generateKeyList(DataObject dataObject){
        String initKey = "DAY";
        List<String> acctDateList = dataObject.getWhereMap().get(DataObject.DAY_ACCT_DATE);
        if(SystemVariableService.acctTypeMonth.equals(dataObject.getDateType())){
            initKey = "MONTH";
            acctDateList = dataObject.getWhereMap().get(DataObject.MONTH_MONTH_ID);
        }
        List<String> provCityList = dataObject.getWhereMap().get(DataObject.DAY_PROV_ID_AREA_NO);
        List<String> provIdList = dataObject.getWhereMap().get(DataObject.DAY_PROV_ID);
        List<String> areaNoList = dataObject.getWhereMap().get(DataObject.DAY_AREA_NO);
        List<String> kpiCodeList = dataObject.getWhereMap().get(DataObject.DAY_KPI_CODE);
        List<String> serviceList = dataObject.getWhereMap().get(DataObject.DAY_SERVICE_TYPE);
        List<String> channelList = dataObject.getWhereMap().get(DataObject.DAY_CHANNEL_TYPE);
        List<String> productList = dataObject.getWhereMap().get(DataObject.DAY_PRODUCT_ID);

        List<List<String>> dimValue = new ArrayList<List<String>>();
        String acctDate = processMultiDimension(acctDateList, dimValue);
        String kpiCode = processMultiDimension(kpiCodeList, dimValue);
        String provId = null;
        String areaNo = null;
        String provCity = null;
        if(null != provIdList && provIdList.size() > 0 && null != areaNoList && areaNoList.size() > 0) {
            provId = processMultiDimension(provIdList, dimValue, Arrays.asList(new String[]{"111"}));
            areaNo = processMultiDimension(areaNoList, dimValue, Arrays.asList(new String[]{"-1"}));
        }else{
            provCity = processMultiDimension(provCityList, dimValue);
        }
        String service = processMultiDimension(serviceList, dimValue, Arrays.asList(new String[]{"20AAAAAA", "30AAAAAA", "40AAAAAA", "90AAAAAA", "**"}));
        String channel = processMultiDimension(channelList, dimValue, Arrays.asList(new String[]{"10AA", "20AA", "30AA", "99AA", "**"}));
        //String channel = processMultiDimension(channelList, dimValue, new ArrayList<>(DataObject.channelMap.keySet()));
        String product = processMultiDimension(productList, dimValue, Arrays.asList(new String[]{"01", "02", "03", "04", "99", "**"}));

        List<List<String>> recursiveResult = new ArrayList<List<String>>();
        // 递归实现笛卡尔积
        DescartesUtils.recursive(dimValue, recursiveResult, 0, new ArrayList<String>());
        System.out.println("****************************笛卡尔积结果长度"+recursiveResult.size()+"*******************");
        //1.拼接key，规则
        // 散列码：(short)(DAY#账期#指标编码#省分#地市#**).hashcode() & 0x7FFF
        // #DAY账期#指标编码#省分#地市#**#合约#渠道#产品
        //正则匹配怎么写，合约#渠道#产品多个值怎么处理
        List<String> rowKeyList = new ArrayList<>();
        List<Get> keyList = new ArrayList<>();
        StringBuffer hashKey = new StringBuffer();
        StringBuffer rowKey = new StringBuffer();
        //特殊情况处理，各个拼接项都只有一个，笛卡尔积数目为0,直接拼接
        if(recursiveResult.size() == 0 && CommonUtils.isNotBlank(acctDate) && CommonUtils.isNotBlank(kpiCode)
                && CommonUtils.isNotBlank(service) && CommonUtils.isNotBlank(channel) && CommonUtils.isNotBlank(product)){
            hashKey.append(initKey);
            hashKey.append("#");
            hashKey.append(acctDate);
            hashKey.append("#");
            hashKey.append(kpiCode);
            hashKey.append("#");
            if(CommonUtils.isNotBlank(provId) && CommonUtils.isNotBlank(areaNo)) {
                hashKey.append(provId);
                hashKey.append("#");
                hashKey.append(areaNo);
            }else if(CommonUtils.isNotBlank(provCity)){
                hashKey.append(provCity);
            }else{
                return keyList;
            }
            if(SystemVariableService.acctTypeDay.equals(dataObject.getDateType())) {
                hashKey.append("#");
                hashKey.append("**");
            }

            rowKey.append(hashKey);
            rowKey.append("#");
            rowKey.append(service);
            rowKey.append("#");
            rowKey.append(channel);
            rowKey.append("#");
            rowKey.append(product);
            //log.info("---------本次生成的key------"+rowKey);
            keyList.add(HbaseUtils.generateGet(hashKey.toString(), rowKey.toString()));
            rowKeyList.add(rowKey.toString());
        }
        for (List<String> list : recursiveResult) {
            int index = 0;
            hashKey.append(initKey);
            hashKey.append("#");
            index = appendKey(acctDate, list, index, hashKey);
            hashKey.append("#");
            index = appendKey(kpiCode, list, index, hashKey);
            hashKey.append("#");
            if(null != provIdList && provIdList.size() > 0 && null != areaNoList && areaNoList.size() > 0) {
                index = appendKey(provId, list, index, hashKey);
                hashKey.append("#");
                index = appendKey(areaNo, list, index, hashKey);
            }else{
                index = appendKey(provCity, list, index, hashKey);
            }
            if(SystemVariableService.acctTypeDay.equals(dataObject.getDateType())) {
                hashKey.append("#");
                hashKey.append("**");
            }

            rowKey.append(hashKey);
            rowKey.append("#");
            index = appendKey(service, list, index, rowKey);
            rowKey.append("#");
            index = appendKey(channel, list, index, rowKey);
            rowKey.append("#");
            index = appendKey(product, list, index, rowKey);
            //log.info("---------本次生成的key------"+rowKey);
            keyList.add(HbaseUtils.generateGet(hashKey.toString(), rowKey.toString()));
            rowKeyList.add(rowKey.toString());
            hashKey.setLength(0);
            rowKey.setLength(0);
        }
        log.info("rowKey长度"+rowKeyList.size());
        log.info("rowKey集合：  "+(rowKeyList.size() > 1 ? rowKeyList.get(0) : null));
        return keyList;
    }

    /**
     * 根据where条件生成key
     * @param dataObject 数据对象
     * @return hbase的key列表
     */
    @Override
    public List<String> generateRedisKeyList(DataObject dataObject){
        String initKey = "DAY";
        List<String> acctDateList = dataObject.getWhereMap().get(DataObject.DAY_ACCT_DATE);
        if(SystemVariableService.acctTypeMonth.equals(dataObject.getDateType())){
            initKey = "MONTH";
            acctDateList = dataObject.getWhereMap().get(DataObject.MONTH_MONTH_ID);
        }
        List<String> provCityList = dataObject.getWhereMap().get(DataObject.DAY_PROV_ID_AREA_NO);
        List<String> provIdList = dataObject.getWhereMap().get(DataObject.DAY_PROV_ID);
        List<String> areaNoList = dataObject.getWhereMap().get(DataObject.DAY_AREA_NO);
        List<String> kpiCodeList = dataObject.getWhereMap().get(DataObject.DAY_KPI_CODE);
        List<String> serviceList = dataObject.getWhereMap().get(DataObject.DAY_SERVICE_TYPE);
        List<String> channelList = dataObject.getWhereMap().get(DataObject.DAY_CHANNEL_TYPE);
        List<String> productList = dataObject.getWhereMap().get(DataObject.DAY_PRODUCT_ID);

        List<List<String>> dimValue = new ArrayList<List<String>>();
        String acctDate = processMultiDimension(acctDateList, dimValue);
        String kpiCode = processMultiDimension(kpiCodeList, dimValue);
        String provId = null;
        String areaNo = null;
        String provCity = null;
        if(null != provIdList && provIdList.size() > 0 && null != areaNoList && areaNoList.size() > 0) {
            provId = processMultiDimension(provIdList, dimValue, Arrays.asList(new String[]{"111"}));
            areaNo = processMultiDimension(areaNoList, dimValue, Arrays.asList(new String[]{"-1"}));
        }else{
            provCity = processMultiDimension(provCityList, dimValue);
        }
        String service = processMultiDimension(serviceList, dimValue, Arrays.asList(new String[]{"20AAAAAA", "30AAAAAA", "40AAAAAA", "90AAAAAA", "**"}));
        String channel = processMultiDimension(channelList, dimValue, Arrays.asList(new String[]{"10AA", "20AA", "30AA", "99AA", "**"}));
        //String channel = processMultiDimension(channelList, dimValue, new ArrayList<>(DataObject.channelMap.keySet()));
        String product = processMultiDimension(productList, dimValue, Arrays.asList(new String[]{"01", "02", "03", "04", "99", "**"}));

        List<List<String>> recursiveResult = new ArrayList<List<String>>();
        // 递归实现笛卡尔积
        DescartesUtils.recursive(dimValue, recursiveResult, 0, new ArrayList<String>());
        System.out.println("****************************笛卡尔积结果长度"+recursiveResult.size()+"*******************");
        //1.拼接key，规则
        // 散列码：(short)(DAY#账期#指标编码#省分#地市#**).hashcode() & 0x7FFF
        // #DAY账期#指标编码#省分#地市#**#合约#渠道#产品
        //正则匹配怎么写，合约#渠道#产品多个值怎么处理
        List<String> rowKeyList = new ArrayList<>();
        List<Get> keyList = new ArrayList<>();
        StringBuffer hashKey = new StringBuffer();
        StringBuffer rowKey = new StringBuffer();
        //特殊情况处理，各个拼接项都只有一个，笛卡尔积数目为0,直接拼接
        if(recursiveResult.size() == 0 && CommonUtils.isNotBlank(acctDate) && CommonUtils.isNotBlank(kpiCode)
                && CommonUtils.isNotBlank(service) && CommonUtils.isNotBlank(channel) && CommonUtils.isNotBlank(product)){
            hashKey.append(initKey);
            hashKey.append("#");
            hashKey.append(acctDate);
            hashKey.append("#");
            hashKey.append(kpiCode);
            hashKey.append("#");
            if(CommonUtils.isNotBlank(provId) && CommonUtils.isNotBlank(areaNo)) {
                hashKey.append(provId);
                hashKey.append("#");
                hashKey.append(areaNo);
            }else if(CommonUtils.isNotBlank(provCity)){
                hashKey.append(provCity);
            }else{
                return rowKeyList;
            }
            if(SystemVariableService.acctTypeDay.equals(dataObject.getDateType())) {
                hashKey.append("#");
                hashKey.append("**");
            }

            rowKey.append(hashKey);
            rowKey.append("#");
            rowKey.append(service);
            rowKey.append("#");
            rowKey.append(channel);
            rowKey.append("#");
            rowKey.append(product);
            //log.info("---------本次生成的key------"+rowKey);
            keyList.add(HbaseUtils.generateGet(hashKey.toString(), rowKey.toString()));
            rowKeyList.add(rowKey.toString());
        }
        for (List<String> list : recursiveResult) {
            int index = 0;
            hashKey.append(initKey);
            hashKey.append("#");
            index = appendKey(acctDate, list, index, hashKey);
            hashKey.append("#");
            index = appendKey(kpiCode, list, index, hashKey);
            hashKey.append("#");
            if(null != provIdList && provIdList.size() > 0 && null != areaNoList && areaNoList.size() > 0) {
                index = appendKey(provId, list, index, hashKey);
                hashKey.append("#");
                index = appendKey(areaNo, list, index, hashKey);
            }else{
                index = appendKey(provCity, list, index, hashKey);
            }
            if(SystemVariableService.acctTypeDay.equals(dataObject.getDateType())) {
                hashKey.append("#");
                hashKey.append("**");
            }

            rowKey.append(hashKey);
            rowKey.append("#");
            index = appendKey(service, list, index, rowKey);
            rowKey.append("#");
            index = appendKey(channel, list, index, rowKey);
            rowKey.append("#");
            index = appendKey(product, list, index, rowKey);
            //log.info("---------本次生成的key------"+rowKey);
            keyList.add(HbaseUtils.generateGet(hashKey.toString(), rowKey.toString()));
            rowKeyList.add(rowKey.toString());
            hashKey.setLength(0);
            rowKey.setLength(0);
        }
        log.info("rowKey长度"+rowKeyList.size());
        log.info("rowKey集合：  "+(rowKeyList.size() > 1 ? rowKeyList.get(0) : null));
        return rowKeyList;
    }

    /**
     * 拼接key，如果直接是数据，index保持不变，如果从笛卡尔积结果集中取出，index+1
     * @param data 拼接的数据
     * @param list 笛卡尔积中取出index位置的值
     * @param index 当前游标位置
     * @param hashKey 拼接的key
     * @return 下一个游标位置
     */
    private int appendKey(String data, List<String> list, int index, StringBuffer hashKey) {
        if(null != data){
            hashKey.append(data);
        }else{
            hashKey.append(list.get(index++));
        }
        return index;
    }

    /**
     * 处理多种维度情况，list中包含单个值，返回内容，否则加入笛卡尔积集合做运算，不带缺省列表
     * @param list 解析出的where条件列表
     * @param dimValue 笛卡尔积集合
     * @return 单个情况返回结果
     */
    private String processMultiDimension(List<String> list, List<List<String>> dimValue) {
        return processMultiDimension(list, dimValue, null);
    }

    /**
     * 处理多种维度情况，list中包含单个值，返回内容，否则加入笛卡尔积集合做运算
     * @param list 解析出的where条件列表
     * @param dimValue 笛卡尔积集合
     * @param defaultList list为空，赋给的缺省列表
     * @return 单个情况返回结果
     */
    private String processMultiDimension(List<String> list, List<List<String>> dimValue, List<String> defaultList) {
        if(list == null){
            if(null != defaultList) {
                dimValue.add(defaultList);
                return null;
            }
            return "**";
        }
        if(list.size() > 1){
            dimValue.add(list);
            return null;
        }else if(list.size() == 1){
            return list.get(0);
        }
        return null;
    }

    /**
     * 将hbase结果集转换为目标rs
     * @param dataObject
     * @param hbaseResult
     * @return
     */
    public List<Map<String,Object>> convertResult(DataObject dataObject, List<String> hbaseResult) {
        List<Map<String,Object>> resList = new ArrayList<>();
        String delim = "|";
        int maxfields = DataObject.dayKeyArray.length;
        //1、分割字符串
        if(null!=hbaseResult){
           for(String str :hbaseResult){
               Map<String,Object> dataMap = new HashMap<>();
               String[] results = new String[maxfields];
               str = str.replaceAll("\r|\n", "");
               StringTokenizer st = new StringTokenizer(str, delim, true);

               int i = 0;
               // 得到每一个StringTokenizer类
               while (st.hasMoreTokens()) {
                   String s = st.nextToken();
                   if (s.equals(delim)) {
                       if (i++ >= maxfields){
                           //log.info("wwwwwwwwwwwwwwwwwww"+str);
                           throw new IllegalArgumentException("Input line " + str
                                   + " has too many fields");
                       }
                       continue;
                   }
                   if(i < maxfields){
                       results[i] = s;
                   }
                   else if(i == maxfields){
                       results[i] = s.trim();
                   }
                   else{
                       log.error("数组越界了,数据是"+str+",拆分字段为"+maxfields);
                   }
               }
               //2、给数据字段赋key
               String[] keyList = new String[]{};
               if(SystemVariableService.acctTypeDay.equals(dataObject.getDateType())){
                   keyList = DataObject.dayKeyArray;
               }else{
                   keyList = DataObject.monthKeyArray;
               }
               for(int i1=0;i1<keyList.length;i1++){
                   if(StringUtils.isNotBlank(keyList[i1])){
                       dataMap.put(keyList[i1],results[i1]);
                   }
               }
               resList.add(dataMap);
           }
       }
       /* if(resList!=null&&resList.size()>0){
            log.info("转换结果："+resList.get(0).get("AREA_NO"));
        }*/
        return resList;
    }

    /**
     * sum，group后结果处理，空判断
     * @param dataObject 数据对象
     * @param resultList 结果集
     * @return
     */
    public List<Map<String, Object>> processBlank(DataObject dataObject,
                                                   List<Map<String, Object>> resultList) {
        //1、获取需要空处理的字段
        List<String> decodeFieldList = dataObject.getDecodeFieldList();
        //2、对值为null处理为-
        if(null!=decodeFieldList&&decodeFieldList.size()!=0){
            for(Map<String, Object> map :resultList){
                Set<String> keySet = map.keySet();
                for(int i=0;i<decodeFieldList.size();i++){
                    String field = decodeFieldList.get(i);
                    if(keySet.contains(field)){
                        if(null==map.get(field)){
                            map.put(field, "-");
                        }
                    }else{
                        map.put(field, "-");
                    }
                }
            }
        }
        //log.info("空处理后：~~"+resultList);
        return resultList;
    }

    /**
     * 复合指标并发任务类
     */
    class QueryHbaseTask implements Callable {
        private DataObject dataObject;
        private List<Get> keyList;

        public QueryHbaseTask(List<Get> keyList, DataObject dataObject){
            this.keyList = keyList;
            this.dataObject = dataObject;
        }

        @Override
        public Object call() throws Exception {
            return queryDataService.queryData(keyList, dataObject);
        }
    }
}
