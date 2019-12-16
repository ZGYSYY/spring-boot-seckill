package com.itstyle.seckill.common.interceptor;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.format.FormatterRegistry;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.validation.MessageCodesResolver;
import org.springframework.validation.Validator;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.HandlerMethodReturnValueHandler;
import org.springframework.web.servlet.HandlerExceptionResolver;
import org.springframework.web.servlet.config.annotation.*;

import java.util.List;

/*
# WebMvcConfigurerAdapter是什么?
Spring内部的一种配置方式, 采用JavaBean的形式来代替传统的xml配置文件形式进行针对框架个性化定制。

# WebMvcConfigurerAdapter常用的方法
- 解决跨域问题：public void addCorsMappings(CorsRegistry registry);
- 添加拦截器：void addInterceptors(InterceptorRegistry registry);
- 配置视图解析器：void configureViewResolvers(ViewResolverRegistry registry);
- 配置内容裁决的一些选项：void configureContentNegotiation(ContentNegotiationConfigurer configurer);
- 视图跳转控制器：void addViewControllers(ViewControllerRegistry registry);
- 静态资源处理：void addResourceHandlers(ResourceHandlerRegistry registry);
- 默认静态资源处理器：void configureDefaultServletHandling(DefaultServletHandlerConfigurer configurer);

# 知识点参考链接
- [SpringBoot WebMvcConfigurerAdapter详解](https://blog.csdn.net/weixin_43453386/article/details/83623242)
 */

/**
 * WebMvcConfigurerAdapter SpringMVC 配置适配器
 * @author ZGY
 */
@Configuration
public class MyAdapter extends WebMvcConfigurerAdapter {
    @Override
    public void addViewControllers( ViewControllerRegistry registry ) {
        // 当访问 "/" 路径时，要访问的视图为 index.shtml
        registry.addViewController( "/" ).setViewName( "forward:/index.shtml" );
        // 就算是其他 controller 中的路径为 `/` 也会先记载这个配置
        registry.setOrder(Ordered.HIGHEST_PRECEDENCE);
        super.addViewControllers(registry);
    }


}
