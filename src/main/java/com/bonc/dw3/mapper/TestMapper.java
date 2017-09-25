package com.bonc.dw3.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.springframework.web.bind.annotation.CrossOrigin;

import java.util.List;
import java.util.Map;

/**
 * <p>Title: BONC -  TestMapper</p>
 * <p>Description:  </p>
 * <p>Copyright: Copyright BONC(c) 2013 - 2025 </p>
 * <p>Company: 北京东方国信科技股份有限公司 </p>
 *
 * @author zhaojie
 * @version 1.0.0
 */
@Mapper
@CrossOrigin( origins="*")
public interface TestMapper {

    /**
     * 所有省份地市
     * @return
     */
    List<Map<String, Object>> getAllProCities(@Param(value = "provId") String provId);
}
