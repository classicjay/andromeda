package com.bonc.dw3.common.util;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * 复合指标计算公式
 * @author Administrator
 *
 */
public class CalculateUtils {
	
	private static Logger log = LoggerFactory.getLogger(CalculateUtils.class);
      
    public static Map<String,Object> formulaCalculat(String formulaStr, Map<String, Map<String, Object>> valuesMap) {
    	Map<String,Object> resMap = new HashMap<>();
    	try {
			ScriptEngine jse = new ScriptEngineManager().getEngineByName("JavaScript");
			Set<String> keys = valuesMap.keySet();
			for(String key:keys){
				String formular = formulaStr;
				Map<String,Object> dataMap = (Map<String, Object>) valuesMap.get(key);
				for (Map.Entry<String, Object> entry : dataMap.entrySet()) {
					if (!StringUtils.isEmpty(entry.getKey()) && null != entry.getValue()&&!"-".equals(entry.getValue())) {
						//kap版取数
						String value = entry.getValue().toString();
						formular = formular.replaceFirst(entry.getKey(), value);
					}else{
						formular = null;
						break;
					}
				}
				String str = null;
				if(!StringUtils.isBlank(formular)){
					str = jse.eval(formular).toString();
				}
				if(null!=str&&!str.equals("NaN")&&!str.equals("Infinity")){
					resMap.put(key, str);
				}else{
					resMap.put(key, "-");
				}
			}
			return resMap;
		} catch (Exception e) {
			log.error("", e);
		}
    	return resMap;
	}
}
