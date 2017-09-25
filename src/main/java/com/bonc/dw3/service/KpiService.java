package com.bonc.dw3.service;

import com.bonc.dw3.bean.DataObject;
import com.bonc.dw3.common.util.CommonUtils;
import com.bonc.dw3.common.util.DateUtils;
import com.bonc.dw3.mapper.KpiMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.CrossOrigin;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.*;

/**
 * 指标业务逻辑
 *
 * @author myj
 *         2017年7月13日
 */
@Service
@CrossOrigin(origins = "*")
public class KpiService implements EnvironmentAware {

    @Autowired
    @Qualifier("ComputeTaskServiceImpl")
    private ComputeTaskService computeTaskService;

    @Autowired
    @Qualifier("KpiTableComputeTaskService")
    private ComputeTaskService kpiTableComputeTaskService;

    @Autowired
    private KpiMapper kpiMapper;

    @Autowired
    private ParameterService parameterService;

    @Autowired
    private Environment env;

    @Override
    public void setEnvironment(Environment environment) {
        this.env = environment;
    }

    /**
     * 缓冲区
     */
//    public BlockingQueue<DataObject> queue = new LinkedBlockingQueue<DataObject>(60);

    private static Logger log = LoggerFactory.getLogger(KpiService.class);

    /**
     * 多线程执行类
     */
    private ExecutorService executor;

    /**
     * 每次提交给taskservice的任务数
     */
    private int everyThreadsNum;

    /**
     * 每一天构造一个
     */
    private int step;

    /**
     * 初始化系统变量
     */
    @PostConstruct
    public void init(){
        String everyThreadsNumStr =  env.getProperty("system.thread.everyThreadsNum");
        String stepStr =  env.getProperty("system.thread.step");
        if(null == everyThreadsNumStr){
            everyThreadsNumStr = "10";
        }
        if(null == stepStr){
            stepStr = "1";
        }
        everyThreadsNum = Integer.parseInt(everyThreadsNumStr);
        step = Integer.parseInt(stepStr);
        int cpuNum = Runtime.getRuntime().availableProcessors();
        executor = new ThreadPoolExecutor(cpuNum, cpuNum * 2, 60000L,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    }

    private boolean validateParams(Map<String, Object> paramMap) {
        log.info("接收参数" + paramMap);
        if(CommonUtils.isBlank(paramMap.get("tableName"))){
            return false;
        }
        return true;
    }

    /**
     * 参数传入单个值处理
     * @param whereMap
     * @param data
     * @param fieldOfTable
     */
    private void dataProcessSingle(Map<String, List<String>> whereMap, Object data, String fieldOfTable) {
        if (CommonUtils.isNotBlank(data)) {
            whereMap.put(fieldOfTable, Arrays.asList(data.toString()));
        }
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
                whereMap.put(DataObject.channelTypeMap.get(dimension.get("dimensionType")), (List<String>) dimension.get("dimensions"));
            }
        }
    }

    /**
     * 日趋势图
     * @param paramMap
     * @return
     */
    public List<Map<String,Object>> getDayTrend(Map<String, Object> paramMap) {
        int cpuNum = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = new ThreadPoolExecutor(cpuNum, cpuNum * 2, 60000L,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
        List<Callable<Object>> taskList = new LinkedList<Callable<Object>>();


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
        provCitieProcess(whereMap, paramMap);
        //dataProcessSingle(whereMap, paramMap.get("provId"), DataObject.DAY_PROV_ID);
        //dataProcessSingle(whereMap, paramMap.get("cityId"), DataObject.DAY_AREA_NO);
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
        log.info("拼接的结果集 "+dataObject.toString());
        //调用计算引擎，返回结果
        List<Map<String,Object>> result = computeTaskService.run(dataObject);
        return result;
    }

    public String[] convertRelyKpiCodes(String relyKpiCodes) {
        relyKpiCodes = relyKpiCodes.replace("'","");
        return relyKpiCodes.split(",");
    }

    /**
     * 月趋势图
     * @param paramMap
     * @return
     */
    public List<Map<String,Object>> getMonthTrend(Map<String, Object> paramMap) {
        if (!validateParams(paramMap)){
            return null;
        }

        //拆分原来sql逻辑，构造数据服务对象
        DataObject dataObject = new DataObject();
        dataObject.setDateType(paramMap.get("dateType").toString());
        Set<String> groupbySet = new LinkedHashSet<>();
        Map<String, List<String>> whereMap = new HashMap<>();
        //日指标
        if(SystemVariableService.acctTypeDay.equals(paramMap.get("dateType"))){
            dataObject.setQueryFieldList(Arrays.asList(new String[]{DataObject.DAY_KPI_CODE, DataObject.DAY_ACCT_DATE}));
            dataObject.setSumFieldList(Arrays.asList(new String[]{DataObject.DAY_M_TM_VALUE, DataObject.DAY_M_LM_VALUE, DataObject.DAY_M_LY_VALUE, null}));
            dataObject.setTableName(DataObject.DAY_TABLE_NAME);
            groupbySet.add(DataObject.DAY_ACCT_DATE);
            groupbySet.add(DataObject.DAY_KPI_CODE);
            whereMap.put(DataObject.DAY_ACCT_DATE, (List) paramMap.get("acctDate"));
        }else if(SystemVariableService.acctTypeMonth.equals(paramMap.get("dateType"))){//月指标
            dataObject.setQueryFieldList(Arrays.asList(new String[]{DataObject.MONTH_KPI_CODE, DataObject.MONTH_MONTH_ID}));
            dataObject.setSumFieldList(Arrays.asList(new String[]{DataObject.MONTH_KPI_VALUE, DataObject.MONTH_M_LM_VALUE, DataObject.MONTH_Y_LY_VALUE, DataObject.MONTH_M_LY12_VALUE}));
            dataObject.setTableName(DataObject.MONTH_TABLE_NAME);
            groupbySet.add(DataObject.MONTH_MONTH_ID);
            groupbySet.add(DataObject.MONTH_KPI_CODE);
            //开始时间 结束时间必须都不为空
            if(CommonUtils.isNotBlank(paramMap.get("startDate")) && CommonUtils.isNotBlank(paramMap.get("endDate"))){
                whereMap.put(DataObject.MONTH_MONTH_ID, DateUtils.dateBetweeWith(paramMap.get("startDate").toString(),
                        paramMap.get("endDate").toString(), "2"));
            }
        }
        dataObject.setQueryFieldAliasList(Arrays.asList(new String[]{"kpiCode", "acctDate"}));
        dataObject.setSumFieldAliasList(Arrays.asList(new String[]{"dValue", "lValue", "mlyValue", "lyValue"}));
        dataObject.setGroupbySet(groupbySet);

        provCitieProcess(whereMap, paramMap);
        //dataProcessSingle(whereMap, paramMap.get("provId"), DataObject.DAY_PROV_ID);
        //dataProcessSingle(whereMap, paramMap.get("cityId"), DataObject.DAY_AREA_NO);
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
        log.info("拼接的结果集 "+dataObject.toString());
        //调用计算引擎，返回结果
        List<Map<String,Object>> result = computeTaskService.run(dataObject);
        return result;
    }

    /**
     * 分省趋势图
     * @param paramMap
     * @return
     */
    public List<Map<String,Object>> getCityTrend(Map<String, Object> paramMap) {
        if (!validateParams(paramMap)){
            return null;
        }

        //拆分原来sql逻辑，构造数据服务对象
        DataObject dataObject = new DataObject();
        dataObject.setDateType(paramMap.get("dateType").toString());
        Map<String, List<String>> whereMap = new HashMap<>();
        //日指标
        dataObject.setQueryFieldList(Arrays.asList(new String[]{DataObject.DAY_PROV_ID, DataObject.DAY_KPI_CODE}));
        dataObject.setQueryFieldAliasList(Arrays.asList(new String[]{"id", "kpiCode"}));
        if(SystemVariableService.acctTypeDay.equals(paramMap.get("dateType"))){
            dataObject.setSumFieldList(Arrays.asList(new String[]{DataObject.DAY_M_TM_VALUE, DataObject.DAY_M_LM_VALUE, null}));
            dataObject.setTableName(DataObject.DAY_TABLE_NAME);
            dataProcessSingle(whereMap, paramMap.get("date"), DataObject.DAY_ACCT_DATE);
        }else if(SystemVariableService.acctTypeMonth.equals(paramMap.get("dateType"))){//月指标
            dataObject.setSumFieldList(Arrays.asList(new String[]{DataObject.MONTH_KPI_VALUE, DataObject.MONTH_M_LM_VALUE, DataObject.MONTH_M_LY12_VALUE}));
            dataObject.setTableName(DataObject.MONTH_TABLE_NAME);
            dataProcessSingle(whereMap, paramMap.get("date"), DataObject.MONTH_MONTH_ID);
        }
        dataObject.setSumFieldAliasList(Arrays.asList(new String[]{"dValue", "lValue", "lyValue"}));

        Set<String> groupbySet = new LinkedHashSet<>();
        groupbySet.add(DataObject.DAY_PROV_ID);
        groupbySet.add(DataObject.DAY_KPI_CODE);
        dataObject.setGroupbySet(groupbySet);

        List<String> provList = query31ProvList();
        whereMap.put(DataObject.DAY_PROV_ID, provList);
        dataProcessSingle(whereMap, "-1", DataObject.DAY_AREA_NO);
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
        log.info("拼接的结果集 "+dataObject.toString());
        //调用计算引擎，返回结果
        List<Map<String,Object>> result = computeTaskService.run(dataObject);
        return result;
    }

    /**
     * 分省趋势图
     * @param paramMap
     * @return
     */
    public List<Map<String,Object>> getCityTrendCities(Map<String, Object> paramMap) {
        if (!validateParams(paramMap)){
            return null;
        }

        //拆分原来sql逻辑，构造数据服务对象
        DataObject dataObject = new DataObject();
        dataObject.setDateType(paramMap.get("dateType").toString());
        Map<String, List<String>> whereMap = new HashMap<>();
        //日指标
        dataObject.setQueryFieldList(Arrays.asList(new String[]{DataObject.DAY_AREA_NO, DataObject.DAY_KPI_CODE}));
        dataObject.setQueryFieldAliasList(Arrays.asList(new String[]{"id", "kpiCode"}));
        if(SystemVariableService.acctTypeDay.equals(paramMap.get("dateType"))){
            dataObject.setSumFieldList(Arrays.asList(new String[]{DataObject.DAY_M_TM_VALUE, DataObject.DAY_M_LM_VALUE, null}));
            dataObject.setTableName(DataObject.DAY_TABLE_NAME);
            dataProcessSingle(whereMap, paramMap.get("date"), DataObject.DAY_ACCT_DATE);
        }else if(SystemVariableService.acctTypeMonth.equals(paramMap.get("dateType"))){//月指标
            dataObject.setSumFieldList(Arrays.asList(new String[]{DataObject.MONTH_KPI_VALUE, DataObject.MONTH_M_LM_VALUE, DataObject.MONTH_M_LY12_VALUE}));
            dataObject.setTableName(DataObject.MONTH_TABLE_NAME);
            dataProcessSingle(whereMap, paramMap.get("date"), DataObject.MONTH_MONTH_ID);
        }
        dataObject.setSumFieldAliasList(Arrays.asList(new String[]{"dValue", "lValue", "lyValue"}));

        Set<String> groupbySet = new LinkedHashSet<>();
        groupbySet.add(DataObject.DAY_AREA_NO);
        groupbySet.add(DataObject.DAY_KPI_CODE);
        dataObject.setGroupbySet(groupbySet);

        dataProcessSingle(whereMap, paramMap.get("provId"), DataObject.DAY_PROV_ID);
        List<String> cityList = queryCityListViaProvId(paramMap);
        whereMap.put(DataObject.DAY_AREA_NO, cityList);
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
        log.info("拼接的结果集 "+dataObject.toString());
        //调用计算引擎，返回结果
        List<Map<String,Object>> result = computeTaskService.run(dataObject);
        return result;
    }

    /**
     * 地市排名
     * @param paramMap
     * @return
     */
    public List<Map<String,Object>> getCityRank(Map<String, Object> paramMap) {
        if (!validateParams(paramMap)){
            return null;
        }

        //拆分sql，构造DataObject
        DataObject dataObject = new DataObject();
        dataObject.setDateType(paramMap.get("dateType").toString());
        //where条件
        Map<String, List<String>> whereMap = new HashMap<>();
        //select字段
        dataObject.setQueryFieldList(Arrays.asList(new String[]{DataObject.DAY_KPI_CODE, DataObject.DAY_AREA_NO}));
        dataObject.setQueryFieldAliasList(Arrays.asList(new String[]{"kpiCode", "id"}));
        //日指标
        if (SystemVariableService.acctTypeDay.equals(paramMap.get("dateType"))){
            //sum字段
            dataObject.setSumFieldList(Arrays.asList(new String[]{DataObject.DAY_M_TM_VALUE, DataObject.DAY_M_LM_VALUE, null}));
            dataObject.setSumFieldAliasList(Arrays.asList(new String[]{"dValue", "lValue", "lyValue"}));
            dataObject.setTableName(DataObject.DAY_TABLE_NAME);
            dataProcessSingle(whereMap, paramMap.get("date"), DataObject.DAY_ACCT_DATE);
        }else if (SystemVariableService.acctTypeMonth.equals(paramMap.get("dateType"))){
            //月指标
            //sum字段
            dataObject.setSumFieldList(Arrays.asList(new String[]{DataObject.MONTH_KPI_VALUE, DataObject.MONTH_M_LM_VALUE, DataObject.MONTH_M_LY12_VALUE}));
            dataObject.setSumFieldAliasList(Arrays.asList(new String[]{"dValue", "lValue", "lyValue"}));
            dataObject.setTableName(DataObject.MONTH_TABLE_NAME);
            dataProcessSingle(whereMap, paramMap.get("date"), DataObject.MONTH_MONTH_ID);
        }
        dataObject.setDecodeFieldList(Arrays.asList(new String[]{"dValue", "BYLJ", "ZR", "SYTQ"}));
        //group by字段
        Set<String> groupbySet = new LinkedHashSet<>();
        groupbySet.add(DataObject.DAY_AREA_NO);
        groupbySet.add(DataObject.DAY_KPI_CODE);
        dataObject.setGroupbySet(groupbySet);
        //省份地市
        provCitiesCombine(whereMap, paramMap);
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
        log.info("拼接的结果集 "+dataObject.toString());
        //调用计算引擎，返回结果
        List<Map<String,Object>> result = computeTaskService.run(dataObject);
        return result;
        //return null;
    }

    /**
     * 产品
     * @param paramMap
     * @return
     */
    public List<Map<String,Object>> getProductData(Map<String, Object> paramMap) {
        log.info("指标 getProductData方法 " + paramMap);
        if (!validateParams(paramMap)){
            return null;
        }
        //拆分原来sql逻辑，构造数据服务对象
        DataObject dataObject = new DataObject();
        dataObject.setQueryFieldAliasList(Arrays.asList(new String[]{"productId","kpiCode"}));
        dataObject.setQueryFieldList(Arrays.asList(new String[]{DataObject.DAY_PRODUCT_ID,DataObject.DAY_KPI_CODE}));
        Set<String> groupbySet = new LinkedHashSet<>();
        groupbySet.add(DataObject.DAY_PRODUCT_ID);
        groupbySet.add(DataObject.DAY_KPI_CODE);
        dataObject.setGroupbySet(groupbySet);
        //2、调用饼图统一处理方法
        List<Map<String,Object>> resultList = pieProcess(dataObject,paramMap);
        return resultList;
    }

    /**
     * 渠道
     * @param paramMap
     * @return
     */
    public List<Map<String,Object>> getChannelData(Map<String, Object> paramMap) {
        log.info("指标 getChannelData方法 " + paramMap);
        if (!validateParams(paramMap)){
            return null;
        }
        //拆分原来sql逻辑，构造数据服务对象
        DataObject dataObject = new DataObject();
        dataObject.setQueryFieldAliasList(Arrays.asList(new String[]{"channelId","kpiCode"}));
        dataObject.setQueryFieldList(Arrays.asList(new String[]{DataObject.DAY_CHANNEL_TYPE,DataObject.DAY_KPI_CODE}));
        Set<String> groupbySet = new LinkedHashSet<>();
        groupbySet.add(DataObject.DAY_CHANNEL_TYPE);
        groupbySet.add(DataObject.DAY_KPI_CODE);
        dataObject.setGroupbySet(groupbySet);
        //2、调用饼图统一处理方法
        List<Map<String,Object>> resultList = pieProcess(dataObject,paramMap);
        return resultList;
    }

    /**
     * 合约
     * @param paramMap
     * @return
     */
    public List<Map<String,Object>> getBusinessData(Map<String, Object> paramMap) {
        log.info("指标 getBusinessData方法 " + paramMap);
        if (!validateParams(paramMap)){
            return null;
        }
        //拆分原来sql逻辑，构造数据服务对象
        DataObject dataObject = new DataObject();
        //1、给DataObject赋值:查询字段、group by 字段
        dataObject.setQueryFieldAliasList(Arrays.asList(new String[]{"businessId","kpiCode"}));
        dataObject.setQueryFieldList(Arrays.asList(new String[]{DataObject.DAY_SERVICE_TYPE,DataObject.DAY_KPI_CODE}));
        Set<String> groupbySet = new LinkedHashSet<>();
        groupbySet.add(DataObject.DAY_SERVICE_TYPE);
        groupbySet.add(DataObject.DAY_KPI_CODE);
        dataObject.setGroupbySet(groupbySet);
        //2、调用饼图统一处理方法
        List<Map<String,Object>> resultList = pieProcess(dataObject,paramMap);
        return resultList;
    }


    /**
     * 饼图处理逻辑
     * @param dataObject
     * @param paramMap
     * @return
     */
    public List<Map<String,Object>> pieProcess(DataObject dataObject,Map<String,Object> paramMap){
        //给dataObject赋值
        if(paramMap.get("dateType").equals(SystemVariableService.acctTypeDay)){
            dataObject.setTableName(DataObject.DAY_TABLE_NAME);
            dataObject.setSumFieldList(Arrays.asList(new String[]{DataObject.DAY_KPI_VALUE}));
            dataObject.setSumFieldAliasList(Arrays.asList(new String[]{"dValue"}));
        }else{
            dataObject.setTableName(DataObject.MONTH_TABLE_NAME);
            dataObject.setSumFieldList(Arrays.asList(new String[]{DataObject.DAY_KPI_VALUE,DataObject.MONTH_M_LY12_VALUE}));
            dataObject.setSumFieldAliasList(Arrays.asList(new String[]{"dValue","lyValue"}));
        }
        dataObject.setDateType(paramMap.get("dateType").toString());

        Map<String, List<String>> whereMap = new HashMap<>();
        //时间必须不为空,分日月账期，直接接收传进来的参数值
        if(CommonUtils.isNotBlank(paramMap.get("date"))){
            whereMap.put(paramMap.get("rowName").toString(), Arrays.asList(new String[]{paramMap.get("date").toString()}));
        }else{
            return null;
        }
        provCitieProcess(whereMap, paramMap);
        //dataProcessSingle(whereMap, paramMap.get("provId"), DataObject.DAY_PROV_ID);
        //dataProcessSingle(whereMap, paramMap.get("cityId"), DataObject.DAY_AREA_NO);
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
        log.info("拼接的结果集 "+dataObject.toString());
        //调用计算引擎，返回结果
        List<Map<String,Object>> result = computeTaskService.run(dataObject);
        return result;
    }

    /**
     * 获取31省的list
     * @return
     */
    private List<String> query31ProvList() {
        List<String> result = new ArrayList<>();
        List<Map<String, Object>> list = kpiMapper.get31Provinces(null);
        for(Map<String, Object> map : list){
            result.add(map.get("ID").toString());
        }
        return result;
    }

    /**
     * 获取31省的list，包括全国、北十、南二十一
     * @return
     */
    private List<String> queryAllProvList() {
        List<String> result = new ArrayList<>();
        List<Map<String, Object>> list = kpiMapper.getAllProvinces(null);
        for(Map<String, Object> map : list){
            result.add(map.get("ID").toString());
        }
        return result;
    }

    /**
     * 根据provId获取地市
     *
     * @Author gp
     * @Date 2017/8/19
     */
    private List<String> queryCityListViaProvId(Map<String, Object> paramMap) {
        List<String> result = new ArrayList<>();
        List<Map<String, Object>> list = parameterService.getCities(paramMap);
        for(Map<String, Object> map : list){
            result.add(map.get("areaId").toString());
        }
        return result;
    }



    /**
     * 指标表格月接口
     * @param paramMap
     * @return
     */
    public List<Map<String,Object>> getKpiTableMonth(Map<String, Object> paramMap) {
        List<Map<String,Object>> multiThreadResult = new ArrayList<>();
        //开始时间 结束时间必须都不为空
        List<String> dateList = null;
        if(CommonUtils.isNotBlank(paramMap.get("startDate")) && CommonUtils.isNotBlank(paramMap.get("endDate"))){
            dateList = DateUtils.dateBetweeWith(paramMap.get("startDate").toString(),
                    paramMap.get("endDate").toString(), "2");
        }else{
            return null;
        }
        multiThreadProcess(paramMap, multiThreadResult, dateList, "2");
        return multiThreadResult;
    }

    private DataObject constructMonthDataObject(Map<String, Object> paramMap, List<String> monthList) {
        //拆分原来sql逻辑，构造数据服务对象
        DataObject dataObject = new DataObject();
        dataObject.setDateType(SystemVariableService.acctTypeMonth);
        Set<String> groupbySet = new LinkedHashSet<>();
        groupbySet.add(DataObject.MONTH_MONTH_ID);

        Map<String, List<String>> whereMap = new HashMap<>();
        //省份
        if("1".equals(paramMap.get("regionType"))){
            dataObject.setQueryFieldList(Arrays.asList(new String[]{DataObject.MONTH_MONTH_ID, DataObject.MONTH_PROV_ID}));
            groupbySet.add(DataObject.MONTH_PROV_ID);

            if(CommonUtils.isNotBlank(paramMap.get("provinceId"))){
                Map<String, Object> param = new HashMap<>();
                param.put("provId", paramMap.get("provinceId"));
                //省份地市
                whereMap.put(DataObject.MONTH_PROV_ID, parameterService.queryProvListViaProvId(param));
                dataProcessSingle(whereMap, "-1", DataObject.MONTH_AREA_NO);
            }
        }else{//地市
            dataObject.setQueryFieldList(Arrays.asList(new String[]{DataObject.MONTH_MONTH_ID, DataObject.MONTH_AREA_NO}));
            groupbySet.add(DataObject.MONTH_AREA_NO);

            if(CommonUtils.isNotBlank(paramMap.get("cityId"))){
                if(!"-1".equals(paramMap.get("cityId"))){
                    Map<String, Object> param = new HashMap<>();
                    param.put("provId", "");
                    param.put("areaNo", paramMap.get("cityId"));
                    List<Map<String, Object>> list = parameterService.getCities(param);
                    if(null != list && list.size() > 0){
                        dataProcessSingle(whereMap, list.get(0).get("provId"), DataObject.MONTH_PROV_ID);
                        dataProcessSingle(whereMap, list.get(0).get("areaId"), DataObject.MONTH_AREA_NO);
                    }else{
                        return null;
                    }
                }else{
                    //省份地市
                    /* Map<String, Object> param = new HashMap<>();
                    param.put("provId", paramMap.get("provinceId"));
                    whereMap.put(DataObject.DAY_PROV_ID, parameterService.queryProvListViaProvId(param));
                    dataProcessSingle(whereMap, "-1", DataObject.DAY_AREA_NO);*/
                    Map<String, Object> param = new HashMap<>();
                    param.put("provId", paramMap.get("provinceId"));
                    //省份地市
                    provCitiesCombineWithNoDistinguish(whereMap, param);
                }
            }
        }
        dataObject.setQueryFieldAliasList(Arrays.asList(new String[]{"acctDate", "place"}));
        dataObject.setSumFieldList(Arrays.asList(new String[]{DataObject.MONTH_KPI_VALUE}));
        dataObject.setSumFieldAliasList(Arrays.asList(new String[]{"kpiValue"}));
        dataObject.setDecodeFieldList(Arrays.asList(new String[]{"kpiValue"}));
        dataObject.setTableName(DataObject.MONTH_TABLE_NAME);
        dataObject.setGroupbySet(groupbySet);


        //开始时间 结束时间必须都不为空
        whereMap.put(DataObject.MONTH_MONTH_ID, monthList);

        dataProcessSingle(whereMap, paramMap.get("markType"), DataObject.MONTH_KPI_CODE);
        dimensionProcess(paramMap, whereMap);
        dataObject.setWhereMap(whereMap);
        //log.info("拼接的结果集 "+dataObject.toString());
        return dataObject;
    }

    /**
     * 指标表格日接口
     * @param paramMap
     * @return
     */
    public List<Map<String,Object>> getKpiTableDay(Map<String, Object> paramMap) {
        List<Map<String,Object>> multiThreadResult = new ArrayList<>();
        //开始时间 结束时间必须都不为空
        List<String> dateList = null;
        if(CommonUtils.isNotBlank(paramMap.get("startDate")) && CommonUtils.isNotBlank(paramMap.get("endDate"))){
            dateList = DateUtils.dateBetweeWith(paramMap.get("startDate").toString(),
                    paramMap.get("endDate").toString(), "1");
        }else{
            return null;
        }
        multiThreadProcess(paramMap, multiThreadResult, dateList, "1");
        return multiThreadResult;
    }

    private void multiThreadProcess(Map<String, Object> paramMap, List<Map<String, Object>> multiThreadResult,
                                    List<String> dateList, String dateType) {
        BlockingQueue<DataObject> queue = new LinkedBlockingQueue<DataObject>(60);
        int toatalOfferNum = 0;
        int toatalTakeNum = 0;
        new OfferDataThread(dateList, dateType, queue, paramMap).start();

        try {
            Thread.sleep(200);
            boolean run = true;
            while(run) {
                //List<Future<Object>> futureList = new ArrayList<>();
                final List<Callable<Object>> taskList = new LinkedList<Callable<Object>>();
                while (!queue.isEmpty()) {
                    //Future future = completionService.submit(new ComputeTask(queue.take()));
                    //futureList.add(future);
                    taskList.add(new ComputeTask(queue.take()));
                    if(taskList.size() <= everyThreadsNum || taskList.size() % everyThreadsNum == 0) {
                        try {
                            List<Future<Object>> futures = executor.invokeAll(taskList);
                            //futureList.addAll(futures);
                            for (Future<Object> future : futures) {
                                List<Map<String, Object>> result = (List<Map<String, Object>>) future.get();
                                if (null != result) {
                                    multiThreadResult.addAll(result);
                                }
                            }

                        } catch (InterruptedException e) {
                            log.error("", e);
                        } catch (Exception e) {
                            log.error("", e);
                        }
                        taskList.clear();
                    }
                    toatalTakeNum++;
                }
                /*while(toatalTakeNum > 0) {
                    for (Future<Object> future : futureList) {
                        List<Map<String, Object>> result = null;
                        try {
                            result = (List<Map<String, Object>>) future.get();
                            if (null != result) {
                                multiThreadResult.addAll(result);
                            }
                        } catch (InterruptedException e) {
                            log.error("", e);
                        } catch (Exception e) {
                            log.error("", e);
                        }
                        if(null != result){
                            futureList.remove(future);
                            toatalTakeNum--;
                            //此处必须break，否则会抛出并发修改异常。（也可以通过将futureList声明为CopyOnWriteArrayList类型解决）
                            break;
                        }
                    }
                }*/
                Thread.sleep(10);
                log.info("是否停止，queue是否为空"+queue.isEmpty()+",放入总数"+toatalOfferNum+"，取出总数"+toatalTakeNum);
                if(queue.isEmpty() && toatalTakeNum > 0){
                    run = false;
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            log.error("", e);
        }
    }

    private DataObject constructDayDataObject(Map<String, Object> paramMap, List<String> dateList) {
        //拆分原来sql逻辑，构造数据服务对象
        DataObject dataObject = new DataObject();
        dataObject.setDateType(SystemVariableService.acctTypeDay);
        Set<String> groupbySet = new LinkedHashSet<>();
        groupbySet.add(DataObject.DAY_ACCT_DATE);

        Map<String, List<String>> whereMap = new HashMap<>();
        //省份
        if("1".equals(paramMap.get("regionType"))){
            dataObject.setQueryFieldList(Arrays.asList(new String[]{DataObject.DAY_ACCT_DATE, DataObject.DAY_PROV_ID}));
            groupbySet.add(DataObject.DAY_PROV_ID);

            if(CommonUtils.isNotBlank(paramMap.get("provinceId"))){
                Map<String, Object> param = new HashMap<>();
                param.put("provId", paramMap.get("provinceId"));
                //省份地市
                whereMap.put(DataObject.DAY_PROV_ID, parameterService.queryProvListViaProvId(param));
                dataProcessSingle(whereMap, "-1", DataObject.DAY_AREA_NO);
            }
        }else{//地市
            dataObject.setQueryFieldList(Arrays.asList(new String[]{DataObject.DAY_ACCT_DATE, DataObject.DAY_AREA_NO}));
            groupbySet.add(DataObject.DAY_AREA_NO);

            if(CommonUtils.isNotBlank(paramMap.get("cityId"))){
                if(!"-1".equals(paramMap.get("cityId"))){
                    Map<String, Object> param = new HashMap<>();
                    param.put("provId", "");
                    param.put("areaNo", paramMap.get("cityId"));
                    List<Map<String, Object>> list = parameterService.getCities(param);
                    if(null != list && list.size() > 0){
                        dataProcessSingle(whereMap, list.get(0).get("provId"), DataObject.DAY_PROV_ID);
                        dataProcessSingle(whereMap, list.get(0).get("areaId"), DataObject.DAY_AREA_NO);
                    }else{
                        return null;
                    }
                }else{
                    //省份地市
                    /*Map<String, Object> param = new HashMap<>();
                    param.put("provId", paramMap.get("provinceId"));
                    whereMap.put(DataObject.DAY_PROV_ID, parameterService.queryProvListViaProvId(param));
                    dataProcessSingle(whereMap, "-1", DataObject.DAY_AREA_NO);*/
                    Map<String, Object> param = new HashMap<>();
                    param.put("provId", paramMap.get("provinceId"));
                    param.put("areaNo", "");
                    //省份地市
                    provCitiesCombineWithNoDistinguish(whereMap, param);
                }
            }
        }
        dataObject.setQueryFieldAliasList(Arrays.asList(new String[]{"acctDate", "place"}));
        dataObject.setSumFieldList(Arrays.asList(new String[]{DataObject.DAY_KPI_VALUE}));
        dataObject.setSumFieldAliasList(Arrays.asList(new String[]{"kpiValue"}));
        dataObject.setDecodeFieldList(Arrays.asList(new String[]{"kpiValue"}));
        dataObject.setTableName(DataObject.DAY_TABLE_NAME);
        dataObject.setGroupbySet(groupbySet);


        whereMap.put(DataObject.DAY_ACCT_DATE, dateList);

        dataProcessSingle(whereMap, paramMap.get("markType"), DataObject.DAY_KPI_CODE);
        dimensionProcess(paramMap, whereMap);
        dataObject.setWhereMap(whereMap);
        //log.info("拼接的结果集 "+dataObject.toString());
        return dataObject;
    }

    private void provCitieProcess(Map<String, List<String>> whereMap, Map<String, Object> param) {
        if(CommonUtils.isNotBlank(param.get("provId"))){
            dataProcessSingle(whereMap, param.get("provId"), DataObject.DAY_PROV_ID);
            dataProcessSingle(whereMap, param.get("cityId"), DataObject.DAY_AREA_NO);
        }else{//传入省份为空，表明只传了地市
            if(CommonUtils.isNotBlank(param.get("cityId"))){
                List<Map<String, Object>> list = parameterService.getCities(param);
                for(Map<String, Object> map : list){
                    dataProcessSingle(whereMap, map.get("provId"), DataObject.DAY_PROV_ID);
                    dataProcessSingle(whereMap, map.get("areaId"), DataObject.DAY_AREA_NO);
                }
            }
        }
    }

    private void provCitiesCombine(Map<String, List<String>> whereMap, Map<String, Object> param) {
        List<String> provCityList = new ArrayList<>();
        List<Map<String, Object>> list = parameterService.getCities(param);
        StringBuffer stringBuffer = new StringBuffer();
        for(Map<String, Object> map : list){
            stringBuffer.setLength(0);
            stringBuffer.append(map.get("provId").toString());
            stringBuffer.append("#");
            stringBuffer.append(map.get("areaId").toString());
            provCityList.add(stringBuffer.toString());
            //provList.add(map.get("provId").toString());
            //cityList.add(map.get("areaId").toString());
        }
        whereMap.put(DataObject.DAY_PROV_ID_AREA_NO, provCityList);
        //whereMap.put(DataObject.DAY_PROV_ID, provList);
        //whereMap.put(DataObject.DAY_AREA_NO, cityList);
    }

    private void provCitiesCombineWithNoDistinguish(Map<String, List<String>> whereMap, Map<String, Object> param) {
        List<String> provCityList = new ArrayList<>();
        List<Map<String, Object>> list = kpiMapper.getCitiesWithNoDistinguish(param);
        StringBuffer stringBuffer = new StringBuffer();
        for(Map<String, Object> map : list){
            stringBuffer.setLength(0);
            stringBuffer.append(map.get("provId").toString());
            stringBuffer.append("#");
            stringBuffer.append(map.get("areaId").toString());
            provCityList.add(stringBuffer.toString());
            //provList.add(map.get("provId").toString());
            //cityList.add(map.get("areaId").toString());
        }
        whereMap.put(DataObject.DAY_PROV_ID_AREA_NO, provCityList);
        //whereMap.put(DataObject.DAY_PROV_ID, provList);
        //whereMap.put(DataObject.DAY_AREA_NO, cityList);
    }

    /**
     * 复合指标并发任务类
     */
    class ComputeTask implements Callable {
        private DataObject dataObject;

        public ComputeTask(DataObject dataObject){
            this.dataObject = dataObject;
        }

        @Override
        public Object call() throws Exception {
            //调用计算引擎，返回结果
            return kpiTableComputeTaskService.run(dataObject);
        }
    }

    class OfferDataThread extends Thread{
        private List<String> dateList = null;
        private String dateType = SystemVariableService.acctTypeDay;
        private BlockingQueue<DataObject> queue = null;
        Map<String, Object> paramMap = null;
        int toatalOfferNum = 0;

        public OfferDataThread(List<String> dateList, String dateType, BlockingQueue<DataObject> queue, Map<String, Object> paramMap){
            this.dateList = dateList;
            this.dateType = dateType;
            this.queue = queue;
            this.paramMap = paramMap;
        }

        @Override
        public void run() {
            int dateListSize = dateList.size();
            //int step = 1;
            for(int i=0; i*step < dateListSize; i++){
                try {
                    boolean run = true;
                    while(run){
                        DataObject dataObject = null;
                        if(SystemVariableService.acctTypeDay.equals(dateType)){
                            dataObject = constructDayDataObject(paramMap, dateList.subList(i*step, (i*step + step) > dateListSize ? dateListSize : (i*step + step)));
                        }else{
                            dataObject = constructMonthDataObject(paramMap, dateList.subList(i*step, (i*step + step) > dateListSize ? dateListSize : (i*step + step)));
                        }
                        boolean result = this.queue.offer(dataObject);
                        if(result){
                            toatalOfferNum++;
                            //log.info(this.hashCode()+"放进去后，数据缓存区中的数据有"+queue.size());
                            break;
                        }else{
                            //log.info(this.hashCode()+"提交数据到缓冲区失败");
                        }
                        Thread.sleep(200);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    log.error("提交数据到缓冲区失败",e);
                }
            }
        }
    }
}


