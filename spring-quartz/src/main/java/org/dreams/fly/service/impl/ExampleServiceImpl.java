package org.dreams.fly.service.impl;


import org.dreams.fly.dao.ExampleMapper;
import org.dreams.fly.dao.domain.Example;
import org.dreams.fly.service.ExampleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


@Service("exampleService")
public class ExampleServiceImpl implements ExampleService {

	private static final Logger LOG = LoggerFactory.getLogger(ExampleServiceImpl.class);
	
	@Autowired
	private ExampleMapper exampleMapper;
	
	public boolean saveExample(Example example){
		LOG.info("saveExample invoked");
		return exampleMapper.insert(example) > 0;
	}

}
