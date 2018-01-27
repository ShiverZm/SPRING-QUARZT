package org.dreams.fly.controller;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
/**
 * @author 艾国梁
 * 动态发布job和执行job 控制器
 */
@Controller
public class DynamicReleaseJobController {
	
	private static final Logger LOG = LoggerFactory.getLogger(DynamicReleaseJobController.class);
	
	/**
	 * @return展示所有的job
	 */
    @RequestMapping("listJob")	
    public Map<String,Object> listJob(){
      LOG.info("=====> listJob invoked.");	
      Map<String,Object> jobs = new HashMap<String, Object>();
    	//未完待续
      return jobs;
    }
	
    @RequestMapping(value = "addJob", name="动态添加-job", produces = "application/json;charset=utf-8", consumes = "application/json", method = RequestMethod.POST)	
    public Map<String,Object> addJob(){
      LOG.info("=====> addJob invoked.");	
      Map<String,Object> jobs = new HashMap<String, Object>();
    	//未完待续
      return jobs;
    }
	
	
	
}
