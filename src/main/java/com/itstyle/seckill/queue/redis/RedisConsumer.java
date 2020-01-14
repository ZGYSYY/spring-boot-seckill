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

	/**
	 * 在 RedisSubListenerConfig 中配置的方法，当发布方发送消息到通道后，订阅方就会获取该消息执行相应的业务逻辑
	 * @param message
	 */
    public void receiveMessage(String message) {
		Thread thread = Thread.currentThread();
		LOGGER.info("当前线程名称：{}，发送方发送的消息为：{}", thread.getName(), message);

    	String[] array = message.split(";");

    	// 如果 redis 中 key 存在，但是没有值，说明可以访问数据库
    	if (redisUtil.getValue(array[0]) == null) {
			Result result = seckillService.startSeckilDBPCC_TWO(Long.parseLong(array[0]), Long.parseLong(array[1]));
			if(result.equals(Result.ok(SeckillStatEnum.SUCCESS))){
				//推送给前台
				WebSocketServer.sendInfo("秒杀成功", array[1]);
			}else{
				//推送给前台
				WebSocketServer.sendInfo("秒杀失败", array[1]);
				redisUtil.cacheValue(array[0], "end");
			}
		} else { // 如果 redis 中 key 存在，并且有值，说明就不可以访问数据库了，应为没有数据可以使用了，这样就减轻了数据库的访问压力
			WebSocketServer.sendInfo("秒杀失败", array[1]);
		}
    }
}