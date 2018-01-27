package org.dreams.fly.controller.interceptor;

import java.text.SimpleDateFormat;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.NamedThreadLocal;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;



/**
 * 日志拦截器
*/
public class LogInterceptor extends HandlerInterceptorAdapter {

	private static final ThreadLocal<Long> START_TIME_THREADLOCAL = new NamedThreadLocal<Long>("ThreadLocal StartTime");

	private static final Logger LOG = LoggerFactory.getLogger(LogInterceptor.class);

	@Override
	public boolean preHandle(HttpServletRequest request, HttpServletResponse response,
			Object handler) throws Exception {
		LOG.info("preHandle invoked .");
		if (LOG.isDebugEnabled()){ //预判断
			long beginTime = System.currentTimeMillis();//1、开始时间
			START_TIME_THREADLOCAL.set(beginTime);		//线程绑定变量（该数据只有当前请求的线程可见）
	        LOG.debug("开始计时: {}  URI: {}", new SimpleDateFormat("hh:mm:ss.SSS")
	        	.format(beginTime), request.getRequestURI());
		}
		return true;
	}

	@Override
	public void postHandle(HttpServletRequest request, HttpServletResponse response, Object handler,
			ModelAndView modelAndView) throws Exception {
		LOG.info("postHandle invoked .");
		if (modelAndView != null){
			LOG.info("viewName: " + modelAndView.getViewName());
		}
	}

	@Override
	public void afterCompletion(HttpServletRequest request, HttpServletResponse response,
			Object handler, Exception ex) throws Exception {
		LOG.info("afterCompletion invoked .");
		//获取日志标题
		String title = null;
		if(handler instanceof HandlerMethod){
			String className = "";
			String methodName = "";
			HandlerMethod handlerMethod = (HandlerMethod)handler;
			RequestMapping classMapping = handlerMethod.getBeanType().getAnnotation(RequestMapping.class);
			if(classMapping != null){
				className = classMapping.name();
			}
			RequestMapping methodMapping = handlerMethod.getMethodAnnotation(RequestMapping.class);
			if(methodMapping != null){
				methodName = methodMapping.name();
			}
			title = methodName;
			if(!(Strings.isNullOrEmpty(className)) && !(Strings.isNullOrEmpty(methodName))){
				title = Joiner.on(" - ").join(new String[]{className,methodName});
			}
		}

		// 如果无标题，则不保存信息 
		if(Strings.isNullOrEmpty(title)){
			return;
		}
		// 保存日志
		//LogUtils.saveLog(request, handler, ex, title); import cn.creditease.wechat.op.util.system.LogUtils;
	}

}
