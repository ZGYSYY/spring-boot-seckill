package com.itstyle.seckill.queue.disruptor;

import java.util.concurrent.ThreadFactory;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

/**
 * @author ZGY
 * 使用 disruptor 框架实现生产/消费。
 */
public class DisruptorUtil {

	private final static Disruptor<SeckillEvent> disruptor;

	/**
	 * 类加载后初始化动作
	 */
	static{
		// 创建事件工厂对象
		EventFactory<SeckillEvent> factory = () -> new SeckillEvent();
		// 环形队列大小
		int ringBufferSize = 1024;
		// 创建线程工厂对象
		ThreadFactory threadFactory = runnable -> new Thread(runnable);
		// 创建 Disruptor 对象
		disruptor = new Disruptor<>(factory, ringBufferSize, threadFactory);
		// 关联事件消费者
		disruptor.handleEventsWith(new SeckillEventConsumer());
		// 启动框架
		disruptor.start();
	}

	/**
	 * 发布事件
	 * @param kill
	 */
	public static void producer(SeckillEvent kill){
		RingBuffer<SeckillEvent> ringBuffer = disruptor.getRingBuffer();
		EventTranslator<SeckillEvent>  translator = (event, sequence) -> {
			event.setSeckillId(kill.getSeckillId());
			event.setUserId(kill.getUserId());
		};
		ringBuffer.publishEvent(translator);
	}
}
