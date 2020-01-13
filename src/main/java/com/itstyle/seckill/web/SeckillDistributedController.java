package com.itstyle.seckill.web;

import com.itstyle.seckill.queue.redis.RedisSubListenerConfig;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import java.util.concurrent.*;
import javax.jms.Destination;
import org.apache.activemq.command.ActiveMQQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.itstyle.seckill.common.entity.Result;
import com.itstyle.seckill.common.redis.RedisUtil;
import com.itstyle.seckill.queue.activemq.ActiveMQSender;
import com.itstyle.seckill.queue.kafka.KafkaSender;
import com.itstyle.seckill.queue.redis.RedisSender;
import com.itstyle.seckill.service.ISeckillDistributedService;
import com.itstyle.seckill.service.ISeckillService;

/**
 * @author ZGY
 */
@Api(tags = "分布式秒杀 API 测试")
@RestController
@RequestMapping("/seckillDistributed")
public class SeckillDistributedController {

    private final static Logger LOGGER = LoggerFactory.getLogger(SeckillDistributedController.class);

    private static int corePoolSize = Runtime.getRuntime().availableProcessors();

    /**
     * 调整队列数 拒绝服务
     */
    private static ThreadPoolExecutor executor = new ThreadPoolExecutor(corePoolSize, corePoolSize + 1, 10L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(10000));

    @Autowired
    private ISeckillService seckillService;

    @Autowired
    private ISeckillDistributedService seckillDistributedService;

    @Autowired
    private RedisSender redisSender;

    @Autowired
    private KafkaSender kafkaSender;

    @Autowired
    private ActiveMQSender activeMQSender;

    @Autowired
    private RedisUtil redisUtil;

    /**
     * 这种方式的秒杀会出现超卖的现象，出现超卖的原因是因为事务和锁冲突。
     * 解决方案有 3 种：
     * 第一种，利用 AOP 对 service 方法进行进行加锁。
     * 第二种，去除声明式事务，改用手动提交事务。
     * 第三种，在 controller 中使用锁将业务放在锁中执行。
     * @param seckillId
     * @return
     */
    @ApiOperation(value = "秒杀一(使用 redisson 框架实现分布式锁)")
    @PostMapping("/startRedisLock")
    public Result startRedisLock(long seckillId) {
        // 恢复数据
        seckillService.resetData(100, seckillId);
        final long killId = seckillId;
        LOGGER.info("开始秒杀一");

        int threadNumber = 100;
        CountDownLatch latch = new CountDownLatch(threadNumber);
        for (int i = 0; i < threadNumber; i++) {
            final long userId = i;
            Runnable task = () -> {
                Result result = seckillDistributedService.startSeckilRedisLock(killId, userId);
                LOGGER.info("用户:{}{}", userId, result.get("msg"));
                latch.countDown();
            };
            executor.execute(task);
        }

        try {
            latch.await();
            LOGGER.info("秒杀已经结束，正在计算秒杀结果，请稍等。。。");
            Thread.sleep(5);
            Long seckillCount = seckillService.getSeckillCount(seckillId);
            LOGGER.info("一共秒杀出{}件商品", seckillCount);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return Result.ok();
    }

    @ApiOperation(value = "秒杀二(zookeeper分布式锁)")
    @PostMapping("/startZkLock")
    public Result startZkLock(final long seckillId) {
        // 恢复数据
        seckillService.resetData(100, seckillId);

        // 模拟多人同时秒杀
        LOGGER.info("开始秒杀二");
        int threadNumber = 500;
        CountDownLatch latch = new CountDownLatch(threadNumber);
        for (int i = 0; i < threadNumber; i++) {
            final long userId = i;
            Runnable task = () -> {
                Result result = seckillDistributedService.startSeckilZksLock(seckillId, userId);
                LOGGER.info("用户:{}{}", userId, result.get("msg"));
                latch.countDown();
            };
            executor.execute(task);
        }
        try {
            latch.await();
            LOGGER.info("秒杀模拟结束，正在获取秒杀结果，请等待。。。");
            Long seckillCount = seckillService.getSeckillCount(seckillId);
            LOGGER.info("一共秒杀出{}件商品", seckillCount);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return Result.ok();
    }

    @ApiOperation(value = "秒杀三(Redis分布式队列-订阅监听)")
    @PostMapping("/startRedisQueue")
    public Result startRedisQueue(final long seckillId) {
        // 在 Redis 中创建一个没有值的 Key
        redisUtil.cacheValue(seckillId + "", null);
        // 恢复数据
        seckillService.resetData(100, seckillId);

        LOGGER.info("开始秒杀三");
        for (int i = 0; i < 5; i++) {
            final int userId = i;
            Runnable task = () -> {
                if (redisUtil.getValue(seckillId + "") == null) {
                    //思考如何返回给用户信息ws
                    redisSender.sendChannelMess("seckill", seckillId + ";" + userId);
                } else {
                    //秒杀结束
                }
            };
            executor.submit(task);
        }

        try {
            TimeUnit.SECONDS.sleep(20);
            redisUtil.cacheValue(seckillId + "", null);
            Long seckillCount = seckillService.getSeckillCount(seckillId);
            LOGGER.info("一共秒杀出{}件商品", seckillCount);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return Result.ok();
    }

    @ApiOperation(value = "秒杀四(Kafka分布式队列)", nickname = "科帮网")
    @PostMapping("/startKafkaQueue")
    public Result startKafkaQueue(long seckillId) {
        seckillService.deleteSeckill(seckillId);
        final long killId = seckillId;
        LOGGER.info("开始秒杀四");
        for (int i = 0; i < 1000; i++) {
            final long userId = i;
            Runnable task = () -> {
                if (redisUtil.getValue(killId + "") == null) {
                    //思考如何返回给用户信息ws
                    kafkaSender.sendChannelMess("seckill", killId + ";" + userId);
                } else {
                    //秒杀结束
                }
            };
            executor.execute(task);
        }
        try {
            Thread.sleep(10000);
            redisUtil.cacheValue(killId + "", null);
            Long seckillCount = seckillService.getSeckillCount(seckillId);
            LOGGER.info("一共秒杀出{}件商品", seckillCount);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return Result.ok();
    }

    @ApiOperation(value = "秒杀五(ActiveMQ分布式队列)", nickname = "科帮网")
    @PostMapping("/startActiveMQQueue")
    public Result startActiveMQQueue(long seckillId) {
        seckillService.deleteSeckill(seckillId);
        final long killId = seckillId;
        LOGGER.info("开始秒杀五");
        for (int i = 0; i < 1000; i++) {
            final long userId = i;
            Runnable task = () -> {
                if (redisUtil.getValue(killId + "") == null) {
                    Destination destination = new ActiveMQQueue("seckill.queue");
                    //思考如何返回给用户信息ws
                    activeMQSender.sendChannelMess(destination, killId + ";" + userId);
                } else {
                    //秒杀结束
                }
            };
            executor.execute(task);
        }
        try {
            Thread.sleep(10000);
            redisUtil.cacheValue(killId + "", null);
            Long seckillCount = seckillService.getSeckillCount(seckillId);
            LOGGER.info("一共秒杀出{}件商品", seckillCount);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return Result.ok();
    }
}
