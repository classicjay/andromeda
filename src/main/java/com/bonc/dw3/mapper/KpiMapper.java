package com.bonc.dw3.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.springframework.web.bind.annotation.CrossOrigin;

import java.util.List;
import java.util.Map;

@Mapper
@CrossOrigin( origins="*")
public interface KpiMapper {

    /**
     * 31省
     * @param paramMap
     * @return
     */
    List<Map<String, Object>>  get31Provinces(Map<String, Object> paramMap);

    /**
     * 全国、北十、南二十一
     * @param paramMap
     * @return
     */
    List<Map<String, Object>>  getAllProvinces(Map<String, Object> paramMap);

    /**
     * 地市
     * @param paramMap
     * @return
     */
    List<Map<String, Object>>  getCities(Map<String, Object> paramMap);

    /**
     * 地市
     * @param paramMap
     * @return
     */
    List<Map<String, Object>>  getCitiesWithNoDistinguish(Map<String, Object> paramMap);

    /**
     * 根据前端传参得到所有省份
     * @param paramMap
     * @return
     */
    List<Map<String, Object>>  getProvsViaProvId(Map<String, Object> paramMap);


}