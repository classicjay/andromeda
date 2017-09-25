package com.bonc.dw3.controller;

import com.alibaba.fastjson.JSONObject;
import com.bonc.dw3.service.HbaseEfficiencyService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * <p>Title: BONC -  HbaseEfficiencyController</p>
 * <p>Description:  </p>
 * <p>Copyright: Copyright BONC(c) 2013 - 2025 </p>
 * <p>Company: 北京东方国信科技股份有限公司 </p>
 *
 * @author zhaojie
 * @version 1.0.0
 */
@Api(tags = "DW3.0-数据服务 指标", description ="Hbase查询效率测试")
@CrossOrigin(origins ="*")
@RestController
@RequestMapping("/hbasetest")
public class HbaseEfficiencyController {

    @Autowired
    HbaseEfficiencyService hbaseEfficiencyService;


    @ApiOperation("Hbase查询效率测试，以200条递增")
    @PostMapping("/queryHbaseEff")
    public Map<String,Map<String,String>> test(@ApiParam("请求参数对象") @RequestBody String params) {
        //1、将json参数转成object,参数map对象
        //2.压力大的话，考虑队列处理
        Map<String,Object> paramMap = JSONObject.parseObject(params);
        return hbaseEfficiencyService.test(paramMap);
    }

    @ApiOperation("传入rowkey查询")
    @PostMapping("/queryByRowkey")
    public String queryByRowkey(@ApiParam("请求参数对象") @RequestBody String params) throws Exception{
        //1、将json参数转成object,参数map对象
        //2.压力大的话，考虑队列处理
        Map<String,Object> paramMap = JSONObject.parseObject(params);
        return hbaseEfficiencyService.queryByRowkey(paramMap);
    }
}
