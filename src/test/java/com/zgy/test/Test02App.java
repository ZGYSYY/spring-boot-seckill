package com.zgy.test;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author ZGY
 * @date 2019/12/17 17:31
 * @description Test02App, Disruptor 使用案例
 */
public class Test02App {

    private static final Logger LOGGER = LoggerFactory.getLogger(Test02App.class);

    @Test
    public void test() {
        MyEventFactory factory = new MyEventFactory();
        Executor executor = Executors.newCachedThreadPool();
        int ringBufferSize = 32;

        Disruptor<MyEvent> disruptor = new Disruptor<MyEvent>(factory, ringBufferSize, executor, ProducerType.SINGLE, new BlockingWaitStrategy());

        EventHandler<MyEvent> a = new MyEventHandlerA();
        EventHandler<MyEvent> b = new MyEventHandlerB();
        EventHandler<MyEvent> c = new MyEventHandlerC();

        SequenceBarrier sequenceBarrier = disruptor.handleEventsWith(a, b).asSequenceBarrier();
        BatchEventProcessor processor = new BatchEventProcessor(disruptor.getRingBuffer(), sequenceBarrier, c);
        disruptor.handleEventsWith(processor);

        RingBuffer<MyEvent> ringBuffer = disruptor.start();

        for (int i = 0; i < 10; i++) {
            long sequence = ringBuffer.next();
            try {
                MyEvent myEvent = ringBuffer.get(sequence);
                myEvent.setValue(i);
            } finally {
                ringBuffer.publish(sequence);
            }
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                LOGGER.error("系统异常！", e);
            }
        }
        disruptor.shutdown();
    }

    /**
     * 定义用户事件
     */
    private class MyEvent {

        private long value;

        public long getValue() {
            return value;
        }

        public void setValue(long value) {
            this.value = value;
        }
    }

    /**
     * 定义事件工厂，这是实例化 Disruptor 所需要的
     */
    private class MyEventFactory implements EventFactory<MyEvent> {
        @Override
        public MyEvent newInstance() {
            LOGGER.info("执行了事件工厂的重写方法");
            return new MyEvent();
        }
    }

    /**
     * 定义事件消费者A
     */
    private class MyEventHandlerA implements EventHandler<MyEvent> {
        @Override
        public void onEvent(MyEvent myEvent, long l, boolean b) throws Exception {
            LOGGER.info("执行了事件消费者A的重写方法，消费者A的值为：{}", myEvent.getValue());
        }
    }

    /**
     * 定义事件消费者A
     */
    private class MyEventHandlerB implements EventHandler<MyEvent> {
        @Override
        public void onEvent(MyEvent myEvent, long l, boolean b) throws Exception {
            LOGGER.info("执行了事件消费者B的重写方法，消费者B的值为：{}", myEvent.getValue());
        }
    }

    /**
     * 定义事件消费者A
     */
    private class MyEventHandlerC implements EventHandler<MyEvent> {
        @Override
        public void onEvent(MyEvent myEvent, long l, boolean b) throws Exception {
            LOGGER.info("执行了事件消费者C的重写方法，消费者C的值为：{}", myEvent.getValue());
        }
    }
}
