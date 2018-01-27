留个后来者的话：

  1.核心依赖
   
   1).本项目核心依赖jkd1.7,spring 4.3.0.RELEAS, mybatis 3.4.1,quartz 2.2.1,日志logback 1.1.7

   2).quartz 2.2.1模板表结构，见quartz-2.2.1.sql, 官网地址 http://www.quartz-scheduler.org/
  
   3).项目的表结构以及字段详细描述见 sp-*.sql。
   
   
   2.原型环境tomcat7，编译工具mavean, 开发工具eclipse，idea后续支持 
   
   
   3.参考质料
     1） 博客：刘亚壮的专栏 http://blog.csdn.net/l1028386804/article/details/49150267
     2)  Quartz应用与集群原理分析：http://tech.meituan.com/mt-crm-quartz.html
         浅析Quartz的集群配置:http://blog.csdn.net/tayanxunhua/article/details/19345733
        
        
     
     
   Remarks：
      笔者最终目标是将该项目做成高可用，横向扩展，job透明，监控透明，线程透明，0.1版以及实现集群部署，job动态加载
   