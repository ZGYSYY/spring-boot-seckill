package com.itstyle.seckill.queue.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.itstyle.seckill.common.entity.Result;
import com.itstyle.seckill.common.enums.SeckillStatEnum;
import com.itstyle.seckill.common.redis.RedisUtil;
import com.itstyle.seckill.common.webSocket.WebSocketServer;
import com.itstyle.seckill.service.ISeckillService;

/**
 * 消费者
 * @author ZGY
 */
@Service
public class RedisConsumer {

	private static final Logger LOGGER = LoggerFactory.getLogger(RedisConsumer.class);
	
	@Autowired
	private ISeckillService seckillService;

	@Autowired
	private RedisUtil redisUtil;
	
    public void receiveMessage(String message) {
    	LOGGER.info("消费消息，message: [{}]", message);
		/*Thread th=Thread.currentThread();
		LOGGER.info("线程名称：{}", th.getName());
        //收到通道的消息之后执行秒杀操作(超卖)
    	String[] array = message.split(";");
		//control层已经判断了，其实这里不需要再判断了
    	if(redisUtil.getValue(array[0])==null){
    		Result result = seckillService.startSeckilDBPCC_TWO(Long.parseLong(array[0]), Long.parseLong(array[1]));
    		if(result.equals(Result.ok(SeckillStatEnum.SUCCESS))){
				//推送给前台
    			WebSocketServer.sendInfo(array[0], "秒杀成功");
    		}else{
				//推送给前台
    			WebSocketServer.sendInfo(array[0], "秒杀失败");
				//秒杀结束
    			redisUtil.cacheValue(array[0], "ok");
    		}
    	}else{
			//推送给前台
    		WebSocketServer.sendInfo(array[0], "秒杀失败");
    	}*/
    }
}