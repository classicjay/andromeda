package com.bonc.dw3.service;

import com.bonc.dw3.bean.DataObject;
import com.bonc.dw3.common.util.CommonUtils;
import com.bonc.dw3.common.util.DateUtils;
import com.bonc.dw3.common.util.HbaseUtils;
import com.bonc.dw3.mapper.TestMapper;
import org.apache.hadoop.hbase.client.Get;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.CrossOrigin;

import java.util.*;

/**
 * <p>Title: BONC -  HbaseEfficiencyService</p>
 * <p>Description:  </p>
 * <p>Copyright: Copyright BONC(c) 2013 - 2025 </p>
 * <p>Company: 北京东方国信科技股份有限公司 </p>
 *
 * @author zhaojie
 * @version 1.0.0
 */
@Service
@CrossOrigin(origins = "*")
public class HbaseEfficiencyService {

    private static Logger log = LoggerFactory.getLogger(HbaseEfficiencyService.class);

    @Autowired
    @Qualifier("ComputeTaskServiceImpl")
    private ComputeTaskService computeTaskService;

    @Autowired
    TestMapper testMapper;

    @Autowired
    @Qualifier("QueryDataFromHbaseServiceImpl")
    private QueryDataService queryDataService;



    public Map<String,Map<String,String>> test(Map<String, Object> paramMap) {
        log.info("接收参数" + paramMap);
        if (!validateParams(paramMap)){
            return null;
        }

        //拆分原来sql逻辑，构造数据服务对象
        DataObject dataObject = new DataObject();
        dataObject.setDateType(paramMap.get("dateType").toString());
        dataObject.setQueryFieldList(Arrays.asList(new String[]{DataObject.DAY_KPI_CODE, DataObject.DAY_ACCT_DATE}));
        dataObject.setQueryFieldAliasList(Arrays.asList(new String[]{"kpiCode", "acctDate"}));
        dataObject.setSumFieldList(Arrays.asList(new String[]{DataObject.DAY_KPI_VALUE}));
        dataObject.setSumFieldAliasList(Arrays.asList(new String[]{"dValue"}));
        dataObject.setDecodeFieldList(Arrays.asList(new String[]{"dValue"}));
        dataObject.setTableName(DataObject.DAY_TABLE_NAME);
        Set<String> groupbySet = new LinkedHashSet<>();
        groupbySet.add(DataObject.DAY_KPI_CODE);
        groupbySet.add(DataObject.DAY_ACCT_DATE);
        dataObject.setGroupbySet(groupbySet);

        Map<String, List<String>> whereMap = new HashMap<>();
        //开始时间 结束时间必须都不为空
        if(CommonUtils.isNotBlank(paramMap.get("startDate")) && CommonUtils.isNotBlank(paramMap.get("endDate"))){
            whereMap.put(DataObject.DAY_ACCT_DATE, DateUtils.dateBetweeWith(paramMap.get("startDate").toString(),
                    paramMap.get("endDate").toString(), "1"));
        }else{
            return null;
        }



        List<String> allProvList = Arrays.asList(new String[] {"011","013","018","019","010","091","090","097","017","076","031","034","036","030","038","075","071","074","051","059","050","083","081","085","086","079","084","087","070","088","089"});
        List<String> provList3 = allProvList.subList(0,3);
        List<String> provList5 = allProvList.subList(0,5);
        List<String> provList10 = allProvList.subList(0,10);
        List<String> provList15 = allProvList.subList(0,15);
        List<String> provList20 = allProvList.subList(0,20);
        List<String> provList25 = allProvList.subList(0,25);

        List<List<String>> alllist = new ArrayList<>();
//        alllist.add(provList3);
//        alllist.add(provList5);
//        alllist.add(provList10);
        alllist.add(provList15);
//        alllist.add(provList20);
//        alllist.add(provList25);
//        alllist.add(allProvList);
        Map<String,Map<String,String>> resultMap = new HashMap<>();
        for (List<String> provList:alllist){
            log.info("-----------------当查询条件有"+provList.size()+"个省份,分别是："+provList+"----------");
            long startProvTime = System.currentTimeMillis();
            List<String> citiesList = new ArrayList<>();
            for (int i=0;i<provList.size();i++){
                String provId = provList.get(i);
                List<Map<String,Object>> singleProvCityList = testMapper.getAllProCities(provId);
                for (int j=0;j<singleProvCityList.size();j++){
                    citiesList.add(singleProvCityList.get(j).get("areaId").toString());
                }
            }
            whereMap.put(DataObject.DAY_PROV_ID,provList);
            whereMap.put(DataObject.DAY_AREA_NO,citiesList);
            //复合指标逻辑
            if("0".equals(paramMap.get("isSum")) && paramMap.get("relyKpiCodes") !=null && paramMap.get("relyKpiCodes") !=""){
                String relyKpiCodes = paramMap.get("relyKpiCodes").toString();
                whereMap.put(DataObject.DAY_KPI_CODE, Arrays.asList(convertRelyKpiCodes(relyKpiCodes)));
            }
            //普通指标逻辑
            if("1".equals(paramMap.get("isSum")) && paramMap.get("indexId") !=null && paramMap.get("indexId") !=""){
                whereMap.put(DataObject.DAY_KPI_CODE, Arrays.asList(new String[]{paramMap.get("indexId").toString()}));
            }
            dimensionProcess(paramMap, whereMap);
            dataObject.setWhereMap(whereMap);
            //调用计算引擎，返回结果
            List<Get> keyList = computeTaskService.generateKeyList(dataObject);
            log.info("-------------------生成keyList的长度"+keyList.size()+"-------------------");
            Map<String,String> logMap = new HashMap<>();
            int start = 0;
            int end = 1000;
            for (int i=1;end<keyList.size();i++){
                List<Get> singList = keyList.subList(start,end);
                long startTime = System.currentTimeMillis();   //获取开始时间
                List<String> hbaseResult = queryDataService.queryData(singList, dataObject);
                long endTime = System.currentTimeMillis(); //获取结束时间
                log.info("---------------------"+(end-start)+"条耗时"+(endTime-startTime)+"ms");
//                log.info("----------------------此次Hbase查询结果数为"+hbaseResult.size());
                logMap.put("查询"+(end-start)+"条耗时",(endTime-startTime)+"ms");
                start = end;
                end = i*1000+end;
            }
            long endProvTime = System.currentTimeMillis();
            log.info("---------------------"+provList.size()+"个省耗时"+(startProvTime-endProvTime)+"ms");
            resultMap.put("查询"+provList.size()+"个省",logMap);
        }
        return resultMap;
//        dataProcessSingle(whereMap, paramMap.get("provId"), DataObject.DAY_PROV_ID);
//        dataProcessSingle(whereMap, paramMap.get("cityId"), DataObject.DAY_AREA_NO);
//        //复合指标逻辑
//        if("0".equals(paramMap.get("isSum")) && paramMap.get("relyKpiCodes") !=null && paramMap.get("relyKpiCodes") !=""){
//            String relyKpiCodes = paramMap.get("relyKpiCodes").toString();
//            whereMap.put(DataObject.DAY_KPI_CODE, Arrays.asList(convertRelyKpiCodes(relyKpiCodes)));
//        }
//        //普通指标逻辑
//        if("1".equals(paramMap.get("isSum")) && paramMap.get("indexId") !=null && paramMap.get("indexId") !=""){
//            whereMap.put(DataObject.DAY_KPI_CODE, Arrays.asList(new String[]{paramMap.get("indexId").toString()}));
//        }
//        dimensionProcess(paramMap, whereMap);
//        dataObject.setWhereMap(whereMap);
//        log.info("拼接的结果集 "+dataObject.toString());
//        //调用计算引擎，返回结果
//        List<Map<String,Object>> result = computeTaskService.run(dataObject);

    }


    private boolean validateParams(Map<String, Object> paramMap) {
        if(CommonUtils.isBlank(paramMap.get("tableName"))){
            return false;
        }
        return true;
    }

    private void dataProcessSingle(Map<String, List<String>> whereMap, Object data, String fieldOfTable) {
        if (CommonUtils.isNotBlank(data)) {
            whereMap.put(fieldOfTable, Arrays.asList(data.toString()));
        }
    }

    public String[] convertRelyKpiCodes(String relyKpiCodes) {
        relyKpiCodes = relyKpiCodes.replace("'","");
        return relyKpiCodes.split(",");
    }

    /**
     * 维度动态处理
     * @param paramMap
     * @param whereMap
     */
    private void dimensionProcess(Map<String, Object> paramMap, Map<String, List<String>> whereMap) {
        if(null != paramMap.get("selectType")){
            List<Map<String, Object>> dimensions = (List<Map<String, Object>>) paramMap.get("selectType");
            for(Map<String, Object> dimension : dimensions){
                whereMap.put(DataObject.channelTypeMap.get(dimension.get("field")), (List<String>) dimension.get("dimensions"));
            }
        }
    }

    /**
     * 根据rowkey查询hbase
     * @param paramMap 参数对象
     * @return 结果
     */
    public String queryByRowkey(Map<String, Object> paramMap) throws Exception{
        String hashKey = paramMap.get("hashKey").toString();
        String rowKey = paramMap.get("rowKey").toString();
        String tableName = paramMap.get("tableName").toString();
        Get get = HbaseUtils.generateGet(hashKey.toString(), rowKey.toString());
        HbaseUtils.getCreate();
        return HbaseUtils.getResult(tableName.toString(), hashKey.toString(), rowKey.toString());
    }
}
