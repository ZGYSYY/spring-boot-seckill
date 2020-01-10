package com.itstyle.seckill.service.impl;

import java.sql.Timestamp;
import java.util.Date;
import java.util.Random;
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
            // 尝试获取锁，如果在1秒内能获取锁返回 true，锁在 redis 中保存2秒后删除。否则返回 false 。
            res = RedissLockUtil.tryLock(seckillId + "", TimeUnit.MILLISECONDS, 1000, 2000);
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
                        TimeUnit.SECONDS.sleep(1);

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
    public Result startSeckilZksLock(long seckillId, long userId) {
        // 从 Zookeeper 中获取锁，最多等待3秒，三秒没有获取到锁就返回 false。
        boolean res = ZkLockUtil.acquire(3, TimeUnit.SECONDS);
        try {
            if (res) {
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
                        Random random = new Random();
                        // 模拟复杂业务
                        TimeUnit.SECONDS.sleep(random.nextInt(5));
                        dynamicQuery.save(killed);
                        nativeSql = "UPDATE seckill  SET number=number-1 WHERE seckill_id=? AND number>0";
                        dynamicQuery.nativeExecuteUpdate(nativeSql, new Object[]{seckillId});
                        // 提交事务
                        transactionManager.commit(transactionStatus);
                    } catch (Exception e) {
                        // 事务回滚
                        transactionManager.rollback(transactionStatus);
                        LOGGER.error("操作数据库发生异常!", e);
                        return Result.error(SeckillStatEnum.END);
                    }
                } else {
                    return Result.error(SeckillStatEnum.END);
                }
            } else {
                return Result.error(SeckillStatEnum.MUCH);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //释放锁
            if (res) {
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
