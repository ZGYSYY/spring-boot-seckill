package com.itstyle.seckill.service.impl;

import java.sql.Timestamp;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.Transactional;
import com.itstyle.seckill.common.dynamicquery.DynamicQuery;
import com.itstyle.seckill.common.entity.Result;
import com.itstyle.seckill.common.entity.SuccessKilled;
import com.itstyle.seckill.common.enums.SeckillStatEnum;
import com.itstyle.seckill.distributedlock.redis.RedissLockUtil;
import com.itstyle.seckill.distributedlock.zookeeper.ZkLockUtil;
import com.itstyle.seckill.service.ISeckillDistributedService;

@Service
public class SeckillDistributedServiceImpl implements ISeckillDistributedService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SeckillDistributedServiceImpl.class);

    @Autowired
    private DynamicQuery dynamicQuery;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private TransactionDefinition transactionDefinition;

    @Override
    public Result startSeckilRedisLock(long seckillId, long userId) {
        boolean res = false;
        try {
            /**
             * 尝试获取锁，最多等待3秒，上锁以后20秒自动解锁（实际项目中推荐这种，以防出现死锁）、这里根据预估秒杀人数，设定自动释放锁时间.
             * 看过博客的朋友可能会知道(Lcok锁与事物冲突的问题)：https://blog.52itstyle.com/archives/2952/
             * 分布式锁的使用和Lock锁的实现方式是一样的，但是测试了多次分布式锁就是没有问题，当时就留了个坑
             * 闲来咨询了《静儿1986》，推荐下博客：https://www.cnblogs.com/xiexj/p/9119017.html
             * 先说明下之前的配置情况：Mysql在本地，而Redis是在外网。
             * 回复是这样的：
             * 这是因为分布式锁的开销是很大的。要和锁的服务器进行通信，它虽然是先发起了锁释放命令，涉及网络IO，延时肯定会远远大于方法结束后的事务提交。
             * ==========================================================================================
             * 分布式锁内部都是Runtime.exe命令调用外部，肯定是异步的。分布式锁的释放只是发了一个锁释放命令就算完活了。真正其作用的是下次获取锁的时候，要确保上次是释放了的。
             * 就是说获取锁的时候耗时比较长，那时候事务肯定提交了就是说获取锁的时候耗时比较长，那时候事务肯定提交了。
             * ==========================================================================================
             * 周末测试了一下，把redis配置在了本地，果然出现了超卖的情况；或者还是使用外网并发数增加在10000+也是会有问题的，之前自己没有细测，我的锅。
             * 所以这钟实现也是错误的，事物和锁会有冲突，建议AOP实现。
             */
            res = RedissLockUtil.tryLock(seckillId + "", TimeUnit.SECONDS, 3, 20);
            if (res) {
                LOGGER.info("用户：{}尝试加锁成功，开始对数据库进行操作", userId);
                String nativeSql = "SELECT number FROM seckill WHERE seckill_id=?";
                Object object = dynamicQuery.nativeQueryObject(nativeSql, new Object[]{seckillId});
                Long number = ((Number) object).longValue();
                if (number > 0) {
					SuccessKilled killed = new SuccessKilled();
                    killed.setSeckillId(seckillId);
                    killed.setUserId(userId);
                    killed.setState((short) 0);
                    killed.setCreateTime(new Timestamp(System.currentTimeMillis()));
					// 开启事务
					TransactionStatus transactionStatus = transactionManager.getTransaction(transactionDefinition);
					try {
					    // 模拟复杂业务，需要持锁很长时间
                        TimeUnit.SECONDS.sleep(3);

						dynamicQuery.save(killed);
						nativeSql = "UPDATE seckill  SET number=number-1 WHERE seckill_id=? AND number>0";
						dynamicQuery.nativeExecuteUpdate(nativeSql, new Object[]{seckillId});
						transactionManager.commit(transactionStatus);
					} catch (Exception e) {
						transactionManager.rollback(transactionStatus);
						LOGGER.error("操作数据库发生异常!", e);
						return Result.error(SeckillStatEnum.END);
					}
                } else {
                    return Result.error(SeckillStatEnum.END);
                }
            } else {
                LOGGER.info("该秒杀线程没有获加锁失败，该锁正被其他线程使用中！");
                return Result.error(SeckillStatEnum.MUCH);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //释放锁
            if (res) {
                RedissLockUtil.unlock(seckillId + "");
            }
        }
        return Result.ok(SeckillStatEnum.SUCCESS);
    }

    @Override
    @Transactional
    public Result startSeckilZksLock(long seckillId, long userId) {
        boolean res = false;
        try {
            //基于redis分布式锁 基本就是上面这个解释 但是 使用zk分布式锁 使用本地zk服务 并发到10000+还是没有问题，谁的锅？
            res = ZkLockUtil.acquire(3, TimeUnit.SECONDS);
            if (res) {
                String nativeSql = "SELECT number FROM seckill WHERE seckill_id=?";
                Object object = dynamicQuery.nativeQueryObject(nativeSql, new Object[]{seckillId});
                Long number = ((Number) object).longValue();
                if (number > 0) {
                    SuccessKilled killed = new SuccessKilled();
                    killed.setSeckillId(seckillId);
                    killed.setUserId(userId);
                    killed.setState((short) 0);
                    killed.setCreateTime(new Timestamp(new Date().getTime()));
                    dynamicQuery.save(killed);
                    nativeSql = "UPDATE seckill  SET number=number-1 WHERE seckill_id=? AND number>0";
                    dynamicQuery.nativeExecuteUpdate(nativeSql, new Object[]{seckillId});
                } else {
                    return Result.error(SeckillStatEnum.END);
                }
            } else {
                return Result.error(SeckillStatEnum.MUCH);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (res) {//释放锁
                ZkLockUtil.release();
            }
        }
        return Result.ok(SeckillStatEnum.SUCCESS);
    }

    @Override
    @Transactional
    public Result startSeckilLock(long seckillId, long userId, long number) {
        boolean res = false;
        try {
            //尝试获取锁，最多等待3秒，上锁以后10秒自动解锁（实际项目中推荐这种，以防出现死锁）
            res = RedissLockUtil.tryLock(seckillId + "", TimeUnit.SECONDS, 3, 10);
            if (res) {
                String nativeSql = "SELECT number FROM seckill WHERE seckill_id=?";
                Object object = dynamicQuery.nativeQueryObject(nativeSql, new Object[]{seckillId});
                Long count = ((Number) object).longValue();
                if (count >= number) {
                    SuccessKilled killed = new SuccessKilled();
                    killed.setSeckillId(seckillId);
                    killed.setUserId(userId);
                    killed.setState((short) 0);
                    killed.setCreateTime(new Timestamp(new Date().getTime()));
                    dynamicQuery.save(killed);
                    nativeSql = "UPDATE seckill  SET number=number-? WHERE seckill_id=? AND number>0";
                    dynamicQuery.nativeExecuteUpdate(nativeSql, new Object[]{number, seckillId});
                } else {
                    return Result.error(SeckillStatEnum.END);
                }
            } else {
                return Result.error(SeckillStatEnum.MUCH);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (res) {//释放锁
                RedissLockUtil.unlock(seckillId + "");
            }
        }
        return Result.ok(SeckillStatEnum.SUCCESS);
    }

}
