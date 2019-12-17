package com.itstyle.seckill.queue.jvm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import com.itstyle.seckill.common.entity.SuccessKilled;
import com.itstyle.seckill.service.ISeckillService;
/**
 * 消费秒杀队列
 * 创建者 科帮网
 * 创建时间	2018年4月3日
 */
@Component
public class TaskRunner implements ApplicationRunner{

	private static final Logger LOGGER = LoggerFactory.getLogger(TaskRunner.class);
	
	@Autowired
	private ISeckillService seckillService;
	
	@Override
    public void run(ApplicationArguments var) throws Exception{
		while(true){
			//进程内队列
			SuccessKilled kill = SeckillQueue.getMailQueue().consume();
			if(kill!=null){
				LOGGER.info("开始操作数据库");
				seckillService.seckill7(kill.getSeckillId(), kill.getUserId());
			}
		}
    }
}