package org.dreams.fly.controller;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.dreams.fly.cache.CacheOperator;
import org.dreams.fly.cache.impl.AbsRedisTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import redis.clients.jedis.Jedis;


@Controller
@RequestMapping(value = "/redis", name = "redis - test")
public class RedisExampleController {

	private static final Logger LOG = LoggerFactory.getLogger(RedisExampleController.class);
	
	private static final String SKEY = "key";

	/***
	 * @return 
	 */
	@RequestMapping(value = "add",name="redis-add", produces = "application/json;charset=utf-8")
	public Map<String,Object> addSortSet(){
		LOG.info("=====> addSortSet invoked .");
		 Map<String,Object> result = new HashMap<String, Object>();
		 result.put("code", 200);
		 result.put("msg", "请求成功");
		 for(int i = 0; i < 50; i++){
			 
			 double d = new Random().nextInt(100);
			 String name =  "张三"+i;
			 LOG.info("=====> 添加到集合的数据 : {},{}",d,name);
			 CacheOperator.getOptZSet().add(SKEY, d,name);
		 }
		 return result;
	}
	
	
	
	@RequestMapping(value = "getsortset",name="redis-get", produces = "application/json;charset=utf-8")
	public Map<String,Object> getSortSet(){
		LOG.info("=====> addSortSet invoked .");
		 Map<String,Object> result = new HashMap<String, Object>();
		 result.put("code", 200);
		 result.put("msg", "请求成功"); 
		Set<String> s =  CacheOperator.getOptZSet().reverseRange(SKEY, 0, 100);
	    for(String s1:s){
	    	LOG.info("=====> 排序打印:{}",s1);
	    }
		 long rank = CacheOperator.getOptZSet().rank(SKEY,"张三2");
		 LOG.info("=====>张三2 排名:{}",rank);
		 return result;
	}
	
	
	public static void main(String[] args) {
		
		/*
		for(int i = 0; i < 10; i++){
			 
			 double d = new Random().nextInt(100);
			 String name =  "张三"+i;
			 LOG.info("=====> 添加到集合的数据 : {},{}",d,name);
			 CacheOperator.getOptZSet().add(SKEY, d,name);
		 }
		*/
		Jedis jedis = AbsRedisTemplate.JEDIS_POOL.getResource();
		
		Set<String> s2 = jedis.zrevrange(SKEY, 0, -1);
		
		Set<String> s1 = CacheOperator.getOptZSet().zrevrangeStore(SKEY,0,200,0,3);
		
		 Iterator<String>  iter =  s1.iterator();
		 while(iter.hasNext()){
			 String t = iter.next();
			 LOG.info("=====>:{}",t);
		 }
		 
		 /*
	     Iterator<String>  iter2 =  s2.iterator();
		 while(iter2.hasNext()){
			 String t = iter2.next();
			 LOG.info("2=====>:{}",t);
		 }
		 */
		
		 long rank = CacheOperator.getOptZSet().reverseRank(SKEY,"张三5");
		 LOG.info("=====> 前张三5 排名:{}",rank);	
		 /*
		 //incrByScore
		 double surce =  CacheOperator.getOptZSet().incrByScore(SKEY,100,"张三0");
		 long rank2 = CacheOperator.getOptZSet().reverseRank(SKEY,"张三0");
		 LOG.info("=====> 后张三0 排名:{}，{}",rank2,surce);	
		*/
		 
	}
	
 	
}
