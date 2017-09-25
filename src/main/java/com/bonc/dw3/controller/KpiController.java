package com.bonc.dw3.controller;

import com.alibaba.fastjson.JSONObject;
import com.bonc.dw3.service.KpiService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@Api(tags = "DW3.0-数据服务 指标", description ="指标")
@CrossOrigin(origins ="*")
@RestController
@RequestMapping("/indexDetails")
public class KpiController {
    
	@Autowired
	private KpiService kpiService;

	/**
	 * 1、日趋势图
	 * @return
	 */
	@ApiOperation("日趋势图")
	@PostMapping("/getDayTrend")
	public List<Map<String, Object>> getDayTrend(@ApiParam("请求参数对象") @RequestBody String params) {
		//1、将json参数转成object,参数map对象
		//2.压力大的话，考虑队列处理
		Map<String,Object> paramMap = JSONObject.parseObject(params);
		return kpiService.getDayTrend(paramMap);
	}

	/**
	 * 2、月累计图
	 * @return
	 */
	@ApiOperation("月累计图")
	@PostMapping("/getMonthTrend")
	public List<Map<String, Object>> getMonthTrend(@ApiParam("请求参数对象") @RequestBody String params) {
		Map<String,Object> paramMap = JSONObject.parseObject(params);
		return kpiService.getMonthTrend(paramMap);
	}

	/**
	 * 3、省份趋势图
	 * @return
	 */
	@ApiOperation("省份趋势图")
	@PostMapping("/getCityTrend")
	public List<Map<String, Object>> getCityTrend(@ApiParam("请求参数对象") @RequestBody String params) {
		Map<String,Object> paramMap = JSONObject.parseObject(params);
		return kpiService.getCityTrend(paramMap);
	}

	/**
	 * 3.1、省份趋势图所有地市
	 * @return
	 */
	@ApiOperation("省份趋势图，所有地市")
	@PostMapping("/getCityTrendCities")
	public List<Map<String, Object>> getCityTrendCities(@ApiParam("请求参数对象") @RequestBody String params) {
		Map<String,Object> paramMap = JSONObject.parseObject(params);
		return kpiService.getCityTrendCities(paramMap);
	}

	/**
	 * 4、地市排名
	 * @return
	 */
	@ApiOperation("地市排名")
	@PostMapping("/getCityRank")
	public List<Map<String, Object>> getCityRank(@ApiParam("请求参数对象") @RequestBody String params) {
		Map<String,Object> paramMap = JSONObject.parseObject(params);
		return kpiService.getCityRank(paramMap);
	}

	/**
	 * 5、产品类型数据
	 * @return
	 */
	@ApiOperation("产品类型数据")
	@PostMapping("/getProductData")
	public List<Map<String, Object>> getProductData(@ApiParam("请求参数对象") @RequestBody String params) {
		Map<String,Object> paramMap = JSONObject.parseObject(params);
		return kpiService.getProductData(paramMap);
	}

	/**
	 * 6、渠道类型数据
	 * @return
	 */
	@ApiOperation("渠道类型数据")
	@PostMapping("/getChannelData")
	public List<Map<String, Object>> getChannelData(@ApiParam("请求参数对象") @RequestBody String params) {
		Map<String,Object> paramMap = JSONObject.parseObject(params);
		return kpiService.getChannelData(paramMap);
	}

	/**
	 * 7、业务类型数据
	 * @return
	 */
	@ApiOperation("合约")
	@PostMapping("/getBusinessData")
	public List<Map<String, Object>> getBusinessData(@ApiParam("请求参数对象") @RequestBody String params) {
		Map<String,Object> paramMap = JSONObject.parseObject(params);
		return kpiService.getBusinessData(paramMap);
	}

	/**
	 * 8、指标表格月接口
	 * @return
	 */
	@ApiOperation("指标表格月表格")
	@PostMapping("/kpiTableMonth")
	public List<Map<String, Object>> getKpiTableMonth(@ApiParam("请求参数对象") @RequestBody String params) {
		Map<String,Object> paramMap = JSONObject.parseObject(params);
		return kpiService.getKpiTableMonth(paramMap);
	}

	/**
	 * 9、指标表格日接口
	 * @return
	 */
	@ApiOperation("指标表格日表格")
	@PostMapping("/kpiTableDay")
	public List<Map<String, Object>> getKpiTableDay(@ApiParam("请求参数对象") @RequestBody String params) {
		Map<String,Object> paramMap = JSONObject.parseObject(params);
		return kpiService.getKpiTableDay(paramMap);
	}

}