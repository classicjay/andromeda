package com.bonc.dw3.service;

import com.bonc.dw3.common.util.CommonUtils;
import com.bonc.dw3.mapper.KpiMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.CrossOrigin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zg on 2017/8/14.
 */
@Service
@CrossOrigin(origins = "*")
public class ParameterService {

    private static Logger log = LoggerFactory.getLogger(KpiService.class);

    @Autowired
    private KpiMapper kpiMapper;

    /**
     * 根据provId获取所有省份
     *
     * @Author gp
     * @Date 2017/8/19
     */
    @Cacheable(cacheNames = "queryProvListViaProvId",key = "#paramMap.get('provId')", condition = "#root.target.validParamsNotNull(#paramMap)")
    public List<String> queryProvListViaProvId(Map<String, Object> paramMap) {
        log.info("根据provId获取所有省份"+paramMap.get("provId"));
        List<String> result = new ArrayList<>();
        List<Map<String, Object>> list = kpiMapper.getProvsViaProvId(paramMap);
        for(Map<String, Object> map : list){
            result.add(map.get("id").toString());
        }
        return result;
    }
    /**
     * 根据provId获取所有省份
     *
     * @Author gp
     * @Date 2017/8/19
     */
    @Cacheable(cacheNames = "getCities")
    public List<Map<String, Object>> getCities(Map<String, Object> paramMap) {
        log.info("根据provId获取所有省份"+paramMap.get("provId") + "地市"+paramMap.get("areaNo"));
        return kpiMapper.getCities(paramMap);
    }

    public boolean validParamsNotNull(HashMap<String, Object> params){
        return CommonUtils.isNotBlank(params.get("provId"));
    }

}
