package com.bonc.dw3.controller;

import com.bonc.dw3.service.QueryDataFromFileServiceImpl;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * 首页请求专题
 * @author myj
 * 2017年7月13日
 */

@Api(tags = "DW3.0-查询hbase", description ="测试")
@CrossOrigin(origins ="*")
@RequestMapping("/queryHbaseTest")
//@Controller
@RestController
public class QueryTestController {
	
	@Autowired
    QueryDataFromFileServiceImpl queryDataFromFileService;

	/**
	 * 查询hbase测试
	 * @param specialId 专题id
	 * @return
	 */
	@ApiOperation("查询hbase接口")
	@PostMapping("/test")
	public List<String> queryHbaseTest(@ApiParam("请输入“gp great！！！”") @RequestBody String specialId){

	    return null;
	}
	
}
