package com.bonc.dw3.service;


import com.bonc.dw3.mapper.SystemVariablesMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 初始化系统变量
 * @author myj
 * 2017年7月13日
 */
@Component
public class SystemVariableService {
	
	@Autowired
	private SystemVariablesMapper systemVariablesMapper;

	//hbase日表
	public static final String DAYHBASETABLE = "code_1030";
	public static String dayHbaseTable = "DM_KPI_DATA_GJ_D_V_MAPPING_ID_0831";

	//hbase日表
	public static final String MONTHHBASETABLE = "code_1031";
	public static String monthHbaseTable = "DM_KPI_DATA_GJ_M_V_MAPPING_ID_0831";

	//日账期
	public static final String DATEDAYCODE = "code_1003";
	public static String acctTypeDay = "1";
	//月账期
	public static final String DATEMONTHCODE = "code_1004";
	public static String acctTypeMonth = "2";
	
    //最大账期日表
    public static final String DAYTABLE = "code_1007";
    public static String dayTable = "V_DM_KPI_D_001001";
    //最大账期月表
    public static final String MONTHTABLE = "code_1008";
    public static String monthTable = "V_DM_KPI_DATA_GJ_MONTH";
     
    //日全量表
    public static final String ALLDAYTABLE = "code_1009";
    public static String allDayTable = "V_DM_KPI_D_001001";
    //月全量表
    public static final String ALLMONTHTABLE = "code_1010";
    public static String allMonthTable = "V_DM_KPI_DATA_GJ_MONTH";

	//渠道编码
	public static final String CHANNEL_TYPE = "code_1011";
	public static String channel_type = "1";
	//产品编码
	public static final String PRODUCT_TYPE = "code_1012";
	public static String product_type = "2";
	
	//业务编码
	public static final String BUSI_TYPE = "code_1013";
	public static String busi_type = "3";

	
	List<Map<String,Object>> systemVariablesList=new LinkedList<>();
	
	/**
	 * 初始化系统变量
	 */
	@PostConstruct
	public void init(){
		//系统参数
		systemVariablesList =systemVariablesMapper.getSystemVariables();		
		if(systemVariablesList != null){			
			for(Map<String,Object> variables:systemVariablesList){
				if(DATEDAYCODE.equals(variables.get("SYS_CODE"))){
					acctTypeDay = variables.get("CODE_VALUE").toString();
				}
				
				if(DATEMONTHCODE.equals(variables.get("SYS_CODE"))){
					acctTypeMonth = variables.get("CODE_VALUE").toString();
				}
				
				if(DAYTABLE.equals(variables.get("SYS_CODE"))){
					dayTable = variables.get("CODE_VALUE").toString();
				}
				
				if(MONTHTABLE.equals(variables.get("SYS_CODE"))){
					monthTable = variables.get("CODE_VALUE").toString();
				}
				
				if(ALLDAYTABLE.equals(variables.get("SYS_CODE"))){
					allDayTable = variables.get("CODE_VALUE").toString();
				}
				
				if(ALLMONTHTABLE.equals(variables.get("SYS_CODE"))){
					allMonthTable = variables.get("CODE_VALUE").toString();
				}
				
				if(CHANNEL_TYPE.equals(variables.get("SYS_CODE"))){
					channel_type = variables.get("CODE_VALUE").toString();
				}
				
				if(PRODUCT_TYPE.equals(variables.get("SYS_CODE"))){
					product_type = variables.get("CODE_VALUE").toString();
				}
				
				if(BUSI_TYPE.equals(variables.get("SYS_CODE"))){
					busi_type = variables.get("CODE_VALUE").toString();
				}

				if(DAYHBASETABLE.equals(variables.get("SYS_CODE"))){
					dayHbaseTable = variables.get("CODE_VALUE").toString();
				}

				if(MONTHHBASETABLE.equals(variables.get("SYS_CODE"))){
					monthHbaseTable = variables.get("CODE_VALUE").toString();
				}
			}
		}
    }
	
	 
}
