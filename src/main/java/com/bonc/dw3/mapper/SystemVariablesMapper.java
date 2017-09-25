package com.bonc.dw3.mapper;

import org.apache.ibatis.annotations.Mapper;

import java.util.List;
import java.util.Map;

/**
 * 系统变量
 * @author myj
 * 2017年7月13日
 */
@Mapper
public interface SystemVariablesMapper {

	/**
	 * 获取系统变量
	 * @return
	 */
	List<Map<String,Object>> getSystemVariables();
}
