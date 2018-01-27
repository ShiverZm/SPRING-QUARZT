package org.dreams.fly.controller;

import org.dreams.fly.dao.domain.Example;
import org.dreams.fly.service.ExampleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
@RequestMapping(value = "/app",name="app-log")
public class App {
   
	private static final Logger LOG = LoggerFactory.getLogger(App.class);
	   
	@Autowired    
	private ExampleService exampleService;
	
	@RequestMapping(path = "test",name="测试-测试")
	public String test(@RequestParam(name="age") int age){
		LOG.info("test invoked.");
        for(int i =  0; i < 100; i++){  
        	Example example = new Example();
        	example.setName("张三"+i);
        	exampleService.saveExample(example);
        	
        } 
        
		return "ok";
	}
	
	public static void main(String[] args) {
		
		System.out.println("hello quartz");
		
		
	}
	
	
}
