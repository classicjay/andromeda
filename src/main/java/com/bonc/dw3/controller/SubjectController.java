package com.bonc.dw3.controller;

import com.alibaba.fastjson.JSONObject;
import com.bonc.dw3.service.SubjectService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@Api(tags = "DW3.0-数据服务", description ="专题")
@CrossOrigin(origins ="*")
@RestController
@RequestMapping("/specialReport")
public class SubjectController {
    
	@Autowired
	private SubjectService subjectService;

	/**
	 * 7、日报表格数据
	 * @return
	 */
	@ApiOperation("日报表格数据接口")
	@PostMapping("/selectDay")
	public List<Map<String,Object>> selectDay(@ApiParam("请求参数对象") @RequestBody String paramStr){
		//1、将json参数转成object,参数map对象
		//2.压力大的话，考虑队列处理
		Map<String,Object> paramMap = JSONObject.parseObject(paramStr);
		return subjectService.selectData(paramMap);
	}

	/**
	 * 8、月报表格数据
	 * @return
	 */
	@ApiOperation("月报表格数据接口")
	@PostMapping("/selectMonth")
	public List<Map<String,Object>> selectMonth(@ApiParam("请求参数对象") @RequestBody String paramStr){
		Map<String,Object> paramMap = JSONObject.parseObject(paramStr);
		return subjectService.selectData(paramMap);
	}
}