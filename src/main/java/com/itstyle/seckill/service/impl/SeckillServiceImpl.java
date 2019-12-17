package com.itstyle.seckill.service.impl;

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.itstyle.seckill.common.aop.ServiceLimit;
import com.itstyle.seckill.common.aop.Servicelock;
import com.itstyle.seckill.common.dynamicquery.DynamicQuery;
import com.itstyle.seckill.common.entity.Result;
import com.itstyle.seckill.common.entity.Seckill;
import com.itstyle.seckill.common.entity.SuccessKilled;
import com.itstyle.seckill.common.enums.SeckillStatEnum;
import com.itstyle.seckill.repository.SeckillRepository;
import com.itstyle.seckill.service.ISeckillService;
@Service("seckillService")
public class SeckillServiceImpl implements ISeckillService {
	
	@Autowired
	private DynamicQuery dynamicQuery;
	@Autowired
	private SeckillRepository seckillRepository;
	
	@Override
	public List<Seckill> getSeckillList() {
		return seckillRepository.findAll();
	}

	@Override
	public Seckill getById(long seckillId) {
		return seckillRepository.findOne(seckillId);
	}

	@Override
	public Long getSeckillCount(long seckillId) {
		String nativeSql = "SELECT count(*) FROM success_killed WHERE seckill_id=?";
		Object object =  dynamicQuery.nativeQueryObject(nativeSql, new Object[]{seckillId});
		return ((Number) object).longValue();
	}
	@Override
	@Transactional
	public void deleteSeckill(long seckillId) {
		// 删除上次测试插入的数据
		String nativeSql = "DELETE FROM success_killed WHERE seckill_id=?";
		dynamicQuery.nativeExecuteUpdate(nativeSql, new Object[]{seckillId});
		// 恢复初始数据
		nativeSql = "UPDATE seckill SET number = 5 WHERE seckill_id=?";
		dynamicQuery.nativeExecuteUpdate(nativeSql, new Object[]{seckillId});
	}

	@Override
	@ServiceLimit(limitType= ServiceLimit.LimitType.IP)
	@Transactional
	public Result startSeckil(long seckillId,long userId) {
		//校验库存
		String nativeSql = "SELECT number FROM seckill WHERE seckill_id=?";
		Object object =  dynamicQuery.nativeQueryObject(nativeSql, new Object[]{seckillId});
		Long number =  ((Number) object).longValue();
		if(number>0){
			//扣库存
			nativeSql = "UPDATE seckill  SET number=number-1 WHERE seckill_id=?";
			dynamicQuery.nativeExecuteUpdate(nativeSql, new Object[]{seckillId});
			//创建订单
			SuccessKilled killed = new SuccessKilled();
			killed.setSeckillId(seckillId);
			killed.setUserId(userId);
			killed.setState((short)0);
			Timestamp createTime = new Timestamp(System.currentTimeMillis());
			killed.setCreateTime(createTime);
			dynamicQuery.save(killed);
            /**
             * 这里仅仅是分表而已，提供一种思路，供参考，测试的时候自行建表
             * 按照用户 ID 来做 hash分散订单数据。
             * 要扩容的时候，为了减少迁移的数据量，一般扩容是以倍数的形式增加。
             * 比如原来是8个库，扩容的时候，就要增加到16个库，再次扩容，就增加到32个库。
             * 这样迁移的数据量，就小很多了。
             * 这个问题不算很大问题，毕竟一次扩容，可以保证比较长的时间，而且使用倍数增加的方式，已经减少了数据迁移量。
             */
            /*String table = "success_killed_"+userId%2;
            nativeSql = "INSERT INTO "+table+" (seckill_id, user_id,state,create_time)VALUES(?,?,?,?)";
            Object[] params = new Object[]{seckillId,userId,(short)0,createTime};
            dynamicQuery.nativeExecuteUpdate(nativeSql,params);*/
			//支付
			return Result.ok(SeckillStatEnum.SUCCESS);
		}else{
			return Result.error(SeckillStatEnum.END);
		}
	}

	/**
	 * 切忌不要在 @Transactional 修饰的方法中加锁或者在其方法声明上加 synchronized 关键字，这样做并不能保证数据的准确，
	 * 因为事务代码是在方法执行完后才执行的，在上述情况下，会出现锁被释放了，但是事务还没有提交，试想一下在此时，另一个线程进入该方法中，
	 * 执行了一些与数据库相关的代码，实际上是在同一个事务中。并没有达到事务的隔离性，所以导致数据不准确，出现超卖的情况。
	 * @param seckillId
	 * @param userId
	 * @return
	 */
	@Override
	@Transactional
	public Result startSeckilLock(long seckillId, long userId) {
		String nativeSql = "SELECT number FROM seckill WHERE seckill_id=?";
		Object object = dynamicQuery.nativeQueryObject(nativeSql, new Object[]{seckillId});
		Long number = ((Number) object).longValue();
		if (number > 0) {
			nativeSql = "UPDATE seckill  SET number=number-1 WHERE seckill_id=?";
			dynamicQuery.nativeExecuteUpdate(nativeSql, new Object[]{seckillId});
			SuccessKilled killed = new SuccessKilled();
			killed.setSeckillId(seckillId);
			killed.setUserId(userId);
			killed.setState(Short.valueOf("0"));
			killed.setCreateTime(new Timestamp(System.currentTimeMillis()));
			dynamicQuery.save(killed);
		} else {
			return Result.error(SeckillStatEnum.END);
		}
		return Result.ok(SeckillStatEnum.SUCCESS);
	}

	@Override
	@Servicelock
	@Transactional
	public Result startSeckilAopLock(long seckillId, long userId) {
		//来自码云码友<马丁的早晨>的建议 使用AOP + 锁实现
		String nativeSql = "SELECT number FROM seckill WHERE seckill_id=?";
		Object object =  dynamicQuery.nativeQueryObject(nativeSql, new Object[]{seckillId});
		Long number =  ((Number) object).longValue();
		if(number>0){
			nativeSql = "UPDATE seckill  SET number=number-1 WHERE seckill_id=?";
			dynamicQuery.nativeExecuteUpdate(nativeSql, new Object[]{seckillId});
			SuccessKilled killed = new SuccessKilled();
			killed.setSeckillId(seckillId);
			killed.setUserId(userId);
			killed.setState(Short.valueOf("0"));
			killed.setCreateTime(new Timestamp(System.currentTimeMillis()));
			dynamicQuery.save(killed);
		}else{
			return Result.error(SeckillStatEnum.END);
		}
		return Result.ok(SeckillStatEnum.SUCCESS);
	}
	//注意这里 限流注解 可能会出现少买 自行调整
	@Override
	@ServiceLimit(limitType= ServiceLimit.LimitType.IP)
	@Transactional
	public Result startSeckilDBPCC_ONE(long seckillId, long userId) {
		//单用户抢购一件商品或者多件都没有问题
		String nativeSql = "SELECT number FROM seckill WHERE seckill_id=? FOR UPDATE";
		Object object =  dynamicQuery.nativeQueryObject(nativeSql, new Object[]{seckillId});
		Long number =  ((Number) object).longValue();
		if(number>0){
			nativeSql = "UPDATE seckill  SET number=number-1 WHERE seckill_id=?";
			dynamicQuery.nativeExecuteUpdate(nativeSql, new Object[]{seckillId});
			SuccessKilled killed = new SuccessKilled();
			killed.setSeckillId(seckillId);
			killed.setUserId(userId);
			killed.setState((short)0);
			killed.setCreateTime(new Timestamp(System.currentTimeMillis()));
			dynamicQuery.save(killed);
			return Result.ok(SeckillStatEnum.SUCCESS);
		}else{
			return Result.error(SeckillStatEnum.END);
		}
	}
    /**
     * SHOW STATUS LIKE 'innodb_row_lock%'; 
     * 如果发现锁争用比较严重，如InnoDB_row_lock_waits和InnoDB_row_lock_time_avg的值比较高
     */
	@Override
	@Transactional
	public Result startSeckilDBPCC_TWO(long seckillId, long userId) {
		//单用户抢购一件商品没有问题、但是抢购多件商品不建议这种写法
		String nativeSql = "UPDATE seckill  SET number=number-1 WHERE seckill_id=? AND number>0";//UPDATE锁表
		int count = dynamicQuery.nativeExecuteUpdate(nativeSql, new Object[]{seckillId});
		if(count>0){
			SuccessKilled killed = new SuccessKilled();
			killed.setSeckillId(seckillId);
			killed.setUserId(userId);
			killed.setState((short)0);
			killed.setCreateTime(new Timestamp(System.currentTimeMillis()));
			dynamicQuery.save(killed);
			return Result.ok(SeckillStatEnum.SUCCESS);
		}else{
			return Result.error(SeckillStatEnum.END);
		}
	}
	@Override
	@Transactional
	public Result startSeckilDBOCC(long seckillId, long userId, long number) {
		Seckill kill = seckillRepository.findOne(seckillId);
		//if(kill.getNumber()>0){
		if(kill.getNumber()>=number){//剩余的数量应该要大于等于秒杀的数量
			//乐观锁
			String nativeSql = "UPDATE seckill  SET number=number-?,version=version+1 WHERE seckill_id=? AND version = ?";
			int count = dynamicQuery.nativeExecuteUpdate(nativeSql, new Object[]{number,seckillId,kill.getVersion()});
			if(count>0){
				SuccessKilled killed = new SuccessKilled();
				killed.setSeckillId(seckillId);
				killed.setUserId(userId);
				killed.setState((short)0);
				killed.setCreateTime(new Timestamp(System.currentTimeMillis()));
				dynamicQuery.save(killed);
				return Result.ok(SeckillStatEnum.SUCCESS);
			}else{
				return Result.error(SeckillStatEnum.END);
			}
		}else{
			return Result.error(SeckillStatEnum.END);
		}
	}

	@Override
	public Result startSeckilTemplate(long seckillId, long userId, long number) {
		return null;
	}

	@Override
	@Transactional
	public Result seckill7(long seckillId, long userId) {
		//校验库存
		String nativeSql = "SELECT number FROM seckill WHERE seckill_id=?";
		Object object =  dynamicQuery.nativeQueryObject(nativeSql, new Object[]{seckillId});
		Long number =  ((Number) object).longValue();
		if(number>0) {
			//扣库存
			nativeSql = "UPDATE seckill  SET number=number-1 WHERE seckill_id=?";
			dynamicQuery.nativeExecuteUpdate(nativeSql, new Object[]{seckillId});
			//创建订单
			SuccessKilled killed = new SuccessKilled();
			killed.setSeckillId(seckillId);
			killed.setUserId(userId);
			killed.setState((short) 0);
			Timestamp createTime = new Timestamp(System.currentTimeMillis());
			killed.setCreateTime(createTime);
			dynamicQuery.save(killed);
			return Result.ok(SeckillStatEnum.SUCCESS);
		} else {
			return Result.ok(SeckillStatEnum.END);
		}
	}

	@Transactional
	@Override
	public void resetData(int number, long id) {
		// 删除上次测试插入的数据
		String nativeSql = "DELETE FROM success_killed WHERE seckill_id=?";
		dynamicQuery.nativeExecuteUpdate(nativeSql, new Object[]{id});
		// 恢复初始数据
		nativeSql = "UPDATE seckill SET number = " + number + " WHERE seckill_id=?";
		dynamicQuery.nativeExecuteUpdate(nativeSql, new Object[]{id});
	}

}
