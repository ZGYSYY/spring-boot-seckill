package com.zgy.test;


import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ZGY
 * @date 2019/12/18 10:14
 * @description Test03App，Disruptor 使用案例，单线程生产者
 */
public class Test03App {

    private static final Logger LOGGER = LoggerFactory.getLogger(Test03App.class);

    @Test
    public void test() {

        /**
         * 构造函数参数含义如下：
         * public Disruptor(EventFactory<T> eventFactory, int ringBufferSize, ThreadFactory threadFactory, ProducerType producerType, WaitStrategy waitStrategy){...}。
         * eventFactory: 事件工厂，作用是返回事件对象，这里的事件对象就是 TradBO。
         * ringBufferSize: ringBuffer 可以看作为一个环形队列，ringBufferSize 的作用就是设置这个队列的大小，该值必须是 2^n 的值。
         * threadFactory: 线程工厂，作用是返回一个线程对象。
         * producerType: 生产者策略，ProducerType.SINGLE和ProducerType.MULTI 单个生产者还是多个生产者。
         * waitStrategy: 等待策略，用来平衡事件发布者和事件处理者之间的处理效率，提供了八种策略，默认是BlockingWaitStrategy。
         */
        // 创建 Disruptor 对象。
        Disruptor<TradBO> disruptor = new Disruptor<TradBO>(() -> new TradBO(), 2, (x) -> {
            Thread thread = new Thread(x);
            thread.setName("实战单线程生产者");
            return thread;
        }, ProducerType.SINGLE, new BlockingWaitStrategy());

        // 关联事件处理者。
        disruptor.handleEventsWith(new ConsumerA());
        disruptor.handleEventsWith(new ConsumerB());
        // 启动消费者线程。
        disruptor.start();

        // 发布事件。
        for (int i = 0; i < 10; i++) {
            // 创建 EventTranslator 对象，该对象是发布事件方法的参数。
            final int value = i;
            EventTranslator<TradBO> eventTranslator = (tradBO, l) -> {
                tradBO.setId(value);
                tradBO.setPrice(value);
            };
            // 发布事件。
            disruptor.publishEvent(eventTranslator);
        }

        // 销毁 Disruptor 对象。
        disruptor.shutdown();
    }

    /**
     * 消费者A
     */
    private class ConsumerA implements EventHandler<TradBO> {
        /**
         * 消费者在消费时要做的事情
         * @param tradBO 参与消费的实体对象
         * @param l 序列
         * @param b TODO
         * @throws Exception
         */
        @Override
        public void onEvent(TradBO tradBO, long l, boolean b) throws Exception {
            LOGGER.info("ConsumerA id={}, price={}", tradBO.getId(), tradBO.getPrice());
        }
    }

    /**
     * 消费者B
     */
    private class ConsumerB implements EventHandler<TradBO> {
        /**
         * 同消费者A中的方法作用一样
         * @param tradBO
         * @param l
         * @param b
         * @throws Exception
         */
        @Override
        public void onEvent(TradBO tradBO, long l, boolean b) throws Exception {
            LOGGER.info("ConsumerB id={}, price={}", tradBO.getId(), tradBO.getPrice());
        }
    }

    /**
     * 事件对象
     */
    private class TradBO {

        private int id;
        private double price;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public double getPrice() {
            return price;
        }

        public void setPrice(double price) {
            this.price = price;
        }
    }
}
