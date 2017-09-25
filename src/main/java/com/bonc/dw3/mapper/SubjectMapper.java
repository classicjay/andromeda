package com.bonc.dw3.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.springframework.web.bind.annotation.CrossOrigin;

import java.util.List;
import java.util.Map;

/**
 * 专题日报、月报、首页请求专题
 * @author myj
 * 2017年7月13日
 */
@Mapper
@CrossOrigin( origins="*")
public interface SubjectMapper {

	/**
	 * 查询模块下包含的指标
	 * @param moduleId 模块id
	 * @return 结果
	 */
	List<Map<String,String>> getModuleKpiRelByModuleId(@Param("moduleId") String moduleId);
}

