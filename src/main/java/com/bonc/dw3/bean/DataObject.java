package com.bonc.dw3.bean;

import com.bonc.dw3.service.SystemVariableService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by zg on 2017/8/14.
 */
public class DataObject {

    public static final String NULL_FLAG = "#NULL#";
    public static final String DAY_PROV_ID_AREA_NO = "PROV_ID#AREA_NO";
    //日表字段
    public static final String DAY_MONTH_ID = "MONTH_ID";
    public static final String DAY_DAY_ID = "DAY_ID";
    public static final String DAY_ACCT_DATE = "ACCT_DATE";
    public static final String DAY_PROV_ID = "PROV_ID";
    public static final String DAY_AREA_NO = "AREA_NO";
    public static final String DAY_CITY_NO = "CITY_NO";
    public static final String DAY_SERVICE_TYPE = "SERVICE_TYPE";
    public static final String DAY_CHANNEL_TYPE = "CHANNEL_TYPE";
    public static final String DAY_PRODUCT_ID = "PRODUCT_ID";
    public static final String DAY_KPI_CODE = "KPI_CODE";
    public static final String DAY_KPI_VALUE = "KPI_VALUE";
    public static final String DAY_D_LD_VALUE = "D_LD_VALUE";
    public static final String DAY_M_TM_VALUE = "M_TM_VALUE";
    public static final String DAY_D_LM_VALUE = "D_LM_VALUE";
    public static final String DAY_M_LM_VALUE = "M_LM_VALUE";
    public static final String DAY_M_LY_VALUE = "M_LY_VALUE";
    public static final String DAY_M_DA_VALUE = "M_DA_VALUE";
    public static final String DAY_M_LDA_VALUE = "M_LDA_VALUE";

    //月表字段
    public static final String MONTH_MONTH_ID = "MONTH_ID";
    public static final String MONTH_PROV_ID = "PROV_ID";
    public static final String MONTH_AREA_NO = "AREA_NO";
    public static final String MONTH_SERVICE_TYPE = "SERVICE_TYPE";
    public static final String MONTH_CHANNEL_TYPE = "CHANNEL_TYPE";
    public static final String MONTH_PRODUCT_ID = "PRODUCT_ID";
    public static final String MONTH_KPI_CODE = "KPI_CODE";
    public static final String MONTH_KPI_VALUE = "KPI_VALUE";
    public static final String MONTH_M_LM_VALUE = "M_LM_VALUE";
    public static final String MONTH_Y_TY_VALUE = "Y_TY_VALUE";
    public static final String MONTH_M_LY_VALUE = "M_LY_VALUE";
    public static final String MONTH_Y_LY_VALUE = "Y_LY_VALUE";
    public static final String MONTH_M_LY12_VALUE = "M_LY12_VALUE";

//    public static final String DAY_TABLE_NAME = "DM_KPI_DATA_GJ_D_V_MAPPING_ID_0831";
//    public static final String MONTH_TABLE_NAME = "DM_KPI_DATA_GJ_M_V_MAPPING_ID_0831";

    public static final String DAY_TABLE_NAME = SystemVariableService.dayHbaseTable;
    public static final String MONTH_TABLE_NAME = SystemVariableService.monthHbaseTable;

    public static final String[] dayKeyArray = new String[]{DAY_MONTH_ID, DAY_DAY_ID, DAY_ACCT_DATE, DAY_PROV_ID,
            DAY_AREA_NO, DAY_CITY_NO, DAY_SERVICE_TYPE, DAY_CHANNEL_TYPE, DAY_PRODUCT_ID, DAY_KPI_CODE, DAY_KPI_VALUE,
            DAY_D_LD_VALUE, DAY_M_TM_VALUE, DAY_D_LM_VALUE, DAY_M_LM_VALUE, DAY_M_LY_VALUE, DAY_M_DA_VALUE, DAY_M_LDA_VALUE};

    public static final String[] monthKeyArray = new String[]{DAY_MONTH_ID, DAY_PROV_ID, DAY_AREA_NO,
            DAY_SERVICE_TYPE, DAY_CHANNEL_TYPE, DAY_PRODUCT_ID, DAY_KPI_CODE, DAY_KPI_VALUE,
            MONTH_M_LM_VALUE, MONTH_Y_TY_VALUE, MONTH_M_LY_VALUE, MONTH_Y_LY_VALUE, MONTH_M_LY12_VALUE};

    public static Map<String, String> channelTypeMap = new HashMap<>();
    static{
        channelTypeMap.put("3", DAY_SERVICE_TYPE);
        channelTypeMap.put("1", DAY_CHANNEL_TYPE);
        channelTypeMap.put("2", DAY_PRODUCT_ID);
    }

    public static Map<String, String> channelMap = new HashMap<>();
    static{
        channelMap.put("1030500", "10AA");
        channelMap.put("1030100", "10AA");
        channelMap.put("1030200", "10AA");
        channelMap.put("1030300", "10AA");
        channelMap.put("1030400", "10AA");
        channelMap.put("2030100", "10AA");
        channelMap.put("2030200", "10AA");
        channelMap.put("2030300", "10AA");

        channelMap.put("1010500", "20AA");
        channelMap.put("1020200", "20AA");
        channelMap.put("2020200", "20AA");
        channelMap.put("2050400", "20AA");

        channelMap.put("1010100", "30AA");
        channelMap.put("1010200", "30AA");
        channelMap.put("1010300", "30AA");
        channelMap.put("1010400", "30AA");
        channelMap.put("1020100", "30AA");
        channelMap.put("2010100", "30AA");
        channelMap.put("2010200", "30AA");
        channelMap.put("2010300", "30AA");
        channelMap.put("2020100", "30AA");
        channelMap.put("2040100", "30AA");
        channelMap.put("2040200", "30AA");
        channelMap.put("2040300", "30AA");
        channelMap.put("2040400", "30AA");
        channelMap.put("2050100", "30AA");
        channelMap.put("2050200", "30AA");
        channelMap.put("2050300", "30AA");

        channelMap.put("1010600", "99AA");
        channelMap.put("1020300", "99AA");
        channelMap.put("99AAAAA", "99AA");
        channelMap.put("**", "**");
    }


    /**
     * 账期类型
     */
    private String dateType;
    /**
     * 查询字段列表，查数据用
     */
    private List<String> queryFieldList;

    /**
     * 查询字段别名列表，可为空，结果集用
     */
    private List<String> queryFieldAliasList;

    /**
     * 汇总字段，查数据用，计算用
     */
    private List<String> sumFieldList;

    /**
     * 汇总字段别名列表，可为空，结果集用
     */
    private List<String> sumFieldAliasList;

    /**
     * 需要空判断的字段别名列表，可为空，结果集用
     */
    private List<String> decodeFieldList;

    /**
     * 表名，查数据用
     */
    private String tableName;

    /**
     * groupby的字段集合，计算用
     */
    private Set<String> groupbySet;

    /**
     * where列表，查数据用
     */
    private Map<String, List<String>> whereMap;

    public String getDateType() {
        return dateType;
    }

    public void setDateType(String dateType) {
        this.dateType = dateType;
    }

    public List<String> getQueryFieldList() {
        return queryFieldList;
    }

    public void setQueryFieldList(List<String> queryFieldList) {
        this.queryFieldList = queryFieldList;
    }

    public List<String> getSumFieldList() {
        return sumFieldList;
    }

    public void setSumFieldList(List<String> sumFieldList) {
        this.sumFieldList = sumFieldList;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Set<String> getGroupbySet() {
        return groupbySet;
    }

    public void setGroupbySet(Set<String> groupbySet) {
        this.groupbySet = groupbySet;
    }

    public Map<String, List<String>> getWhereMap() {
        return whereMap;
    }

    public void setWhereMap(Map<String, List<String>> whereMap) {
        this.whereMap = whereMap;
    }

    public List<String> getQueryFieldAliasList() {
        return queryFieldAliasList;
    }

    public void setQueryFieldAliasList(List<String> queryFieldAliasList) {
        this.queryFieldAliasList = queryFieldAliasList;
    }

    public List<String> getSumFieldAliasList() {
        return sumFieldAliasList;
    }

    public void setSumFieldAliasList(List<String> sumFieldAliasList) {
        this.sumFieldAliasList = sumFieldAliasList;
    }

    public List<String> getDecodeFieldList() {
        return decodeFieldList;
    }

    public void setDecodeFieldList(List<String> decodeFieldList) {
        this.decodeFieldList = decodeFieldList;
    }

    @Override
    public String toString() {
        return "DataObject{" +
                "queryFieldList=" + queryFieldList +
                ", queryFieldAliasList=" + queryFieldAliasList +
                ", sumFieldList=" + sumFieldList +
                ", sumFieldAliasList=" + sumFieldAliasList +
                ", decodeFieldList=" + decodeFieldList +
                ", tableName='" + tableName + '\'' +
                ", groupbySet=" + groupbySet +
                ", whereMap=" + whereMap +
                '}';
    }
}
