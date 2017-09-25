package com.bonc.dw3.service;

import com.bonc.dw3.bean.DataObject;
import com.bonc.dw3.common.util.CommonUtils;
import com.bonc.dw3.common.util.DateUtils;
import com.bonc.dw3.mapper.SubjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.CrossOrigin;

import java.util.*;

/**
 * 专题日报、月报业务逻辑
 *
 * @author myj
 *         2017年7月13日
 */
@Service
@CrossOrigin(origins = "*")
public class SubjectService {

    @Autowired
    @Qualifier("ComputeTaskServiceImpl")
    private ComputeTaskService computeService;

    @Autowired
    private SubjectMapper subjectMapper;

    private static Logger log = LoggerFactory.getLogger(SubjectService.class);

    /**
     * 日报表格数据
     * @param paramMap
     * @return
     */
    public List<Map<String,Object>> selectData(Map<String, Object> paramMap) {
        log.info("专题 selectDay方法 " + paramMap);
        if (!validateParams(paramMap)){
            return null;
        }
        //拆分原来sql逻辑，构造数据服务对象
        DataObject dataObject = new DataObject();

        dataObject.setDateType(paramMap.get("dateType").toString());
        Set<String> groupbySet = new LinkedHashSet<>();

        Map<String, List<String>> whereMap = new HashMap<>();
        List<String> dateList = new ArrayList<>();

        if(!CommonUtils.isBlank(paramMap.get("dateType"))&&paramMap.get("dateType").equals(SystemVariableService.acctTypeDay)){
            dataObject.setQueryFieldList(Arrays.asList(new String[]{DataObject.DAY_KPI_CODE, DataObject.DAY_ACCT_DATE}));
            dataObject.setSumFieldAliasList(Arrays.asList(new String[]{"DR", "BYLJ", "ZR", "SYTQ"}));
            dataObject.setSumFieldList(Arrays.asList(new String[]{DataObject.DAY_KPI_VALUE, DataObject.DAY_M_TM_VALUE, DataObject.DAY_M_LM_VALUE, DataObject.DAY_M_LY_VALUE}));
            dataObject.setDecodeFieldList(Arrays.asList(new String[]{"DR", "BYLJ", "ZR", "SYTQ"}));

            groupbySet.add(DataObject.DAY_KPI_CODE);
            groupbySet.add(DataObject.DAY_ACCT_DATE);
            dataObject.setTableName(DataObject.DAY_TABLE_NAME);
            //开始时间 结束时间必须都不为空
            if(CommonUtils.isNotBlank(paramMap.get("minDate")) && CommonUtils.isNotBlank(paramMap.get("date"))){
                whereMap.put(DataObject.DAY_ACCT_DATE, DateUtils.dateBetweeWith(paramMap.get("minDate").toString(), paramMap.get("date").toString(), "1"));
            }else{
                return null;
            }
        }else if(paramMap.get("dateType").equals(SystemVariableService.acctTypeMonth)){
            dataObject.setQueryFieldList(Arrays.asList(new String[]{DataObject.MONTH_KPI_CODE, DataObject.MONTH_MONTH_ID}));
            dataObject.setQueryFieldAliasList(Arrays.asList(new String[]{"KPI_CODE","ACCT_DATE"}));
            dataObject.setSumFieldAliasList(Arrays.asList(new String[]{"DR", "BYLJ", "ZR", "SYTQ","END_VALUE"}));
            dataObject.setSumFieldList(Arrays.asList(new String[]{DataObject.MONTH_KPI_VALUE, DataObject.MONTH_Y_TY_VALUE, DataObject.MONTH_M_LM_VALUE, DataObject.MONTH_Y_LY_VALUE, DataObject.MONTH_M_LY12_VALUE}));
            dataObject.setDecodeFieldList(Arrays.asList(new String[]{"DR", "BYLJ", "ZR", "SYTQ","END_VALUE"}));

            groupbySet.add(DataObject.MONTH_KPI_CODE);
            groupbySet.add(DataObject.MONTH_MONTH_ID);

            dataObject.setTableName(DataObject.MONTH_TABLE_NAME);
            //开始时间 结束时间必须都不为空
            if(CommonUtils.isNotBlank(paramMap.get("minDate")) && CommonUtils.isNotBlank(paramMap.get("date"))){
                whereMap.put(DataObject.MONTH_MONTH_ID, DateUtils.dateBetweeWith(paramMap.get("minDate").toString(), paramMap.get("date").toString(), "2"));
            }else{
                return null;
            }
        }
        dataObject.setDateType(paramMap.get("dateType").toString());
        dataObject.setGroupbySet(groupbySet);
        //indexList为空，是普通指标，需要关联dw_newquery_module_kpi_rel限制指标范围
        if(null == paramMap.get("indexList") && null != paramMap.get("moduleId")){
            List<String> moduleKpiRel = queryModuleKpiRelByModuleId(paramMap.get("moduleId").toString());
            whereMap.put(DataObject.DAY_KPI_CODE, moduleKpiRel);
        }else{//indexList不为空，是复合指标，直接用复合指标的id
            whereMap.put(DataObject.DAY_KPI_CODE, (List<String>) paramMap.get("indexList"));
        }
        if(CommonUtils.isNotBlank(paramMap.get("prov"))){
            whereMap.put(DataObject.DAY_PROV_ID, Arrays.asList(paramMap.get("prov").toString()));
        }
        if(CommonUtils.isNotBlank(paramMap.get("city"))){
            whereMap.put(DataObject.DAY_AREA_NO, Arrays.asList(paramMap.get("city").toString()));
        }
        if(null != paramMap.get("dimensions")){
            List<Map<String, Object>> dimensions = (List<Map<String, Object>>) paramMap.get("dimensions");
            for(Map<String, Object> dimension : dimensions){
                whereMap.put(DataObject.channelTypeMap.get(dimension.get("dimensionType")), (List<String>) dimension.get("dimensions"));
            }
        }
        dataObject.setWhereMap(whereMap);
        log.info("拼接的结果集 "+dataObject.toString());
        //调用计算引擎，返回结果
        List<Map<String,Object>> result = computeService.run(dataObject);
        return result;
    }

    private List<String> queryModuleKpiRelByModuleId(String moduleId) {
        List<String> result = new ArrayList<>();
        List<Map<String, String>> list = subjectMapper.getModuleKpiRelByModuleId(moduleId);
        for(Map<String, String> map : list){
            result.add(map.get("kpiCode"));
        }
        return result;
    }

    private boolean validateParams(Map<String, Object> paramMap) {
        log.info("接收参数" + paramMap);
        if(CommonUtils.isBlank(paramMap.get("table"))){
            return false;
        }
        return true;
    }

}


