package com.itstyle.seckill.common.aop;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.itstyle.seckill.common.exception.RrException;
import com.itstyle.seckill.common.utils.IPUtils;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import com.google.common.util.concurrent.RateLimiter;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * 限流 AOP
 * 创建者	张志朋
 * 创建时间	2015年6月3日
 */
@Aspect
@Configuration
public class LimitAspect {

	private static final Logger LOGGER = LoggerFactory.getLogger(LimitAspect.class);

	/**
	 * 根据IP分不同的令牌桶, 每天自动清理缓存
	 */
	private static LoadingCache<String, RateLimiter> caches = CacheBuilder.newBuilder()
			.maximumSize(1000)
			.expireAfterWrite(1, TimeUnit.DAYS)
			.build(new CacheLoader<String, RateLimiter>() {
				@Override
				public RateLimiter load(String key) {
					LOGGER.info("缓存中没有数据，这时令牌桶中每秒钟向令牌桶中生成5个令牌");
					// 新的IP初始化 每秒只发出5个令牌
					return RateLimiter.create(20);
				}
			});
	
	//Service层切点  限流
	@Pointcut("@annotation(com.itstyle.seckill.common.aop.ServiceLimit)")  
	public void ServiceAspect() {
		
	}
	
    @Around("ServiceAspect()")
    public  Object around(ProceedingJoinPoint joinPoint) {
		MethodSignature signature = (MethodSignature) joinPoint.getSignature();
		Method method = signature.getMethod();
		ServiceLimit limitAnnotation = method.getAnnotation(ServiceLimit.class);
		ServiceLimit.LimitType limitType = limitAnnotation.limitType();
		String key = limitAnnotation.key();
		Object obj = null;
		if(limitType.equals(ServiceLimit.LimitType.IP)){
			key = IPUtils.getIpAddr();
		}
		RateLimiter rateLimiter = null;
		try {
			rateLimiter = caches.get(key);
		} catch (ExecutionException e) {
			LOGGER.error("获取 rateLimiter 对象发生异常！", e);
		}
		Boolean flag = rateLimiter.tryAcquire();
		if(flag){
			try {
				obj = joinPoint.proceed();
			} catch (Throwable throwable) {
				LOGGER.error("执行 service 发生了异常！", throwable);
			}
		}else{
			LOGGER.info("做了用户限流，该 ip 地址不能及时获取令牌桶中的令牌。");
		}
		return obj;
    } 
}
