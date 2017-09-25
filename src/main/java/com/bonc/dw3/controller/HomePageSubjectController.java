package com.bonc.dw3.controller;

import com.bonc.dw3.service.SubjectService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.Map;

/**
 * 首页请求专题
 * @author myj
 * 2017年7月13日
 */

@Api(tags = "DW3.0", description ="专题")
@CrossOrigin(origins ="*")
@RequestMapping("/specialForHomepage")
//@RestController
@Controller
public class HomePageSubjectController {
	
	@Autowired
	SubjectService service;


	/**
	 * 专题图标接口,给首页提供专题图标
	 * @param specialId 专题id
	 * @return
	 */
	@ApiOperation("专题图标接口")
	@PostMapping("/icon")	
	public Map<String,String> icon(@ApiParam("专题编码") @RequestBody String specialId){
		return null;//service.getIcons(specialId);
	}
	
}
