package com.itstyle.seckill.web;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.itstyle.seckill.common.entity.Result;
import com.itstyle.seckill.common.entity.SuccessKilled;
import com.itstyle.seckill.queue.disruptor.DisruptorUtil;
import com.itstyle.seckill.queue.disruptor.SeckillEvent;
import com.itstyle.seckill.queue.jvm.SeckillQueue;
import com.itstyle.seckill.service.ISeckillService;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

/**
 * @author ZGY
 */
@Api(tags ="秒杀 API 测试")
@RestController
@RequestMapping("/seckill")
public class SeckillController {
	private final static Logger LOGGER = LoggerFactory.getLogger(SeckillController.class);
	
	private static int corePoolSize = Runtime.getRuntime().availableProcessors();

	/**
	 * 创建线程池  调整队列数 拒绝服务。
	 */
	private static ThreadPoolExecutor executor  = new ThreadPoolExecutor(corePoolSize, corePoolSize+1, 10L, TimeUnit.SECONDS,
			new LinkedBlockingQueue<>(1000));

	/**
	 * 定义一个可重入锁，默认为非公平锁，这里定义为公平锁。
	 */
	private final ReentrantLock lock = new ReentrantLock(true);
	
	@Autowired
	private ISeckillService seckillService;
	
	@ApiOperation(value="秒杀一(最low实现)",nickname="科帮网")
	@PostMapping("/start")
	public Result start(long seckillId){
		int skillNum = 10;
		//N个购买者
		final CountDownLatch latch = new CountDownLatch(skillNum);
		seckillService.deleteSeckill(seckillId);
		final long killId =  seckillId;
		LOGGER.info("开始秒杀一(会出现超卖)");
		/**
		 * 开启新线程之前，将RequestAttributes对象设置为子线程共享
		 * 这里仅仅是为了测试，否则 IPUtils 中获取不到 request 对象
		 * 用到限流注解的测试用例，都需要加一下两行代码
		 */
		ServletRequestAttributes sra = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
		RequestContextHolder.setRequestAttributes(sra, true);
		for(int i=0;i<skillNum;i++){
			final long userId = i;
			Runnable task = () -> {
				Result result = seckillService.startSeckil(killId, userId);
				if(result!=null){
					LOGGER.info("用户:{}，执行方法成功，方法返回值为：{}",userId,result.get("msg"));
				}else{
					LOGGER.info("用户:{}，没有获取到令牌桶中的令牌，所以并没有执行对应的方法。",userId);
				}
				latch.countDown();
			};
			executor.execute(task);
		}
		try {
			// 等待所有人任务结束
			latch.await();
			Long  seckillCount = seckillService.getSeckillCount(seckillId);
			LOGGER.info("一共秒杀出{}件商品",seckillCount);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return Result.ok();
	}

	@ApiOperation(value="秒杀二(程序锁)",nickname="科帮网")
	@PostMapping("/startLock")
	public Result startLock(long seckillId){
		int skillNum = 1000;

		// 恢复数据
		seckillService.resetData(50, seckillId);
		//N个购买者
		final CountDownLatch latch = new CountDownLatch(skillNum);
		final long killId =  seckillId;
		LOGGER.info("开始秒杀二(正常)");
		for(int i=0;i<skillNum;i++){
			final long userId = i;
			Runnable task = () -> {
				Result result = null;
				try {
					lock.lock();
					result = seckillService.startSeckilLock(killId, userId);
				} catch (Exception e) {
					LOGGER.error("执行SQL出错！");
				} finally {
					lock.unlock();
				}
				LOGGER.info("用户:{}{}",userId,result.get("msg"));
				latch.countDown();
			};
			executor.execute(task);
		}
		try {
			// 等待所有人任务结束
			latch.await();
			Long  seckillCount = seckillService.getSeckillCount(seckillId);
			LOGGER.info("一共秒杀出{}件商品",seckillCount);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return Result.ok();
	}

	@ApiOperation(value="秒杀三(AOP程序锁)",nickname="科帮网")
	@PostMapping("/startAopLock")
	public Result startAopLock(long seckillId){
		int skillNum = 1000;
		//N个购买者
		final CountDownLatch latch = new CountDownLatch(skillNum);
		seckillService.resetData(100, seckillId);
		final long killId =  seckillId;
		LOGGER.info("开始秒杀三(正常)");
		for(int i=0;i<1000;i++){
			final long userId = i;
			Runnable task = () -> {
				Result result = seckillService.startSeckilAopLock(killId, userId);
				LOGGER.info("用户:{}{}",userId,result.get("msg"));
				latch.countDown();
			};
			executor.execute(task);
		}
		try {
			latch.await();// 等待所有人任务结束
			Long  seckillCount = seckillService.getSeckillCount(seckillId);
			LOGGER.info("一共秒杀出{}件商品",seckillCount);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return Result.ok();
	}


	@ApiOperation(value="秒杀四(数据库悲观锁)",nickname="科帮网")
	@PostMapping("/startDBPCC_ONE")
	public Result startDBPCC_ONE(long seckillId){
		ServletRequestAttributes sra = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
		RequestContextHolder.setRequestAttributes(sra, true);
		int skillNum = 1000;
		//N个购买者
		final CountDownLatch latch = new CountDownLatch(skillNum);
		seckillService.resetData(50 ,seckillId);
		final long killId =  seckillId;
		LOGGER.info("开始秒杀四(正常)");
		for(int i=0;i<1000;i++){
			final long userId = i;
			Runnable task = () -> {
				Result result = seckillService.startSeckilDBPCC_ONE(killId, userId);
				if (result != null) {
					LOGGER.info("用户:{}{}",userId,result.get("msg"));
				}
				latch.countDown();
			};
			executor.execute(task);
		}
		try {
			latch.await();// 等待所有人任务结束
			Long  seckillCount = seckillService.getSeckillCount(seckillId);
			LOGGER.info("一共秒杀出{}件商品",seckillCount);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return Result.ok();
	}

	@ApiOperation(value="秒杀五(数据库悲观锁)",nickname="科帮网")
	@PostMapping("/startDPCC_TWO")
	public Result startDPCC_TWO(long seckillId){
		int skillNum = 1000;
		final CountDownLatch latch = new CountDownLatch(skillNum);//N个购买者
		seckillService.resetData(20, seckillId);
		final long killId =  seckillId;
		LOGGER.info("开始秒杀五(正常、数据库锁最优实现)");
		for(int i=0;i<skillNum;i++){
			final long userId = i;
			Runnable task = () -> {
				Result result = seckillService.startSeckilDBPCC_TWO(killId, userId);
				if (result != null) {
					LOGGER.info("用户:{}{}",userId,result.get("msg"));
				}
				latch.countDown();
			};
			executor.execute(task);
		}
		try {
			latch.await();// 等待所有人任务结束
			Long  seckillCount = seckillService.getSeckillCount(seckillId);
			LOGGER.info("一共秒杀出{}件商品",seckillCount);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return Result.ok();
	}

	@ApiOperation(value="秒杀六(数据库乐观锁)",nickname="科帮网")
	@PostMapping("/startDBOCC")
	public Result startDBOCC(long seckillId){
		int skillNum = 1000;
		final CountDownLatch latch = new CountDownLatch(skillNum);//N个购买者
		seckillService.resetData(20, seckillId);
		final long killId =  seckillId;
		LOGGER.info("开始秒杀六(正常、数据库锁最优实现)");
		for(int i=0;i<1000;i++){
			final long userId = i;
			Runnable task = () -> {
				//这里使用的乐观锁、可以自定义抢购数量、如果配置的抢购人数比较少、比如120:100(人数:商品) 会出现少买的情况
				//用户同时进入会出现更新失败的情况
				Result result = seckillService.startSeckilDBOCC(killId, userId,1);
				LOGGER.info("用户:{}{}",userId,result.get("msg"));
				latch.countDown();
			};
			executor.execute(task);
		}
		try {
			latch.await();// 等待所有人任务结束
			Long  seckillCount = seckillService.getSeckillCount(seckillId);
			LOGGER.info("一共秒杀出{}件商品",seckillCount);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return Result.ok();
	}

	@ApiOperation(value="秒杀柒(进程内队列)",nickname="科帮网")
	@PostMapping("/startQueue")
	public Result startQueue(long seckillId){
		seckillService.resetData(20, seckillId);
		final long killId =  seckillId;
		LOGGER.info("开始秒杀柒(正常)");
		for(int i=0;i<1000;i++){
			final long userId = i;
			Runnable task = () -> {
				SuccessKilled kill = new SuccessKilled();
				kill.setSeckillId(killId);
				kill.setUserId(userId);
				try {
					Boolean flag = SeckillQueue.getMailQueue().produce(kill);
					if(flag){
						LOGGER.info("用户:{}{}",kill.getUserId(),"有秒杀成功");
					}else{
						LOGGER.info("用户:{}{}",userId,"秒杀失败");
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
					LOGGER.info("用户:{}{}",userId,"秒杀失败");
				}
			};
			executor.execute(task);
		}
		try {
			// 当前线程等待 8 秒钟后查看有多少用户秒杀成功
			TimeUnit.SECONDS.sleep(8);
			Long  seckillCount = seckillService.getSeckillCount(seckillId);
			LOGGER.info("一共秒杀出{}件商品",seckillCount);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return Result.ok();
	}

	@ApiOperation(value = "秒杀七，使用 Disruptor 高性能队列来实现秒杀功能。当大量请求访问该接口时，该接口会将所有请求交给 Disruptor 来异步处理，而秒杀这个业务是不会出现阻塞情况的，接口会直接返回值。)")
	@PostMapping("/startDisruptorQueue")
	public Result startDisruptorQueue(long seckillId) {
		seckillService.resetData(100, seckillId);
		final long killId = seckillId;
		LOGGER.info("开始秒杀八(正常)");
		// 模拟 1000 个用户同时开始请求
		for (int i = 0; i < 1000; i++) {
			final long userId = i;
			Runnable task = () -> {
				SeckillEvent kill = new SeckillEvent();
				kill.setSeckillId(killId);
				kill.setUserId(userId);
				DisruptorUtil.producer(kill);
			};
			executor.execute(task);
		}

		LOGGER.info("秒杀程序已经结束了，请等待15秒钟查看秒杀结果！");

		// 获取结果
		try {
			TimeUnit.SECONDS.sleep(15);
			Long seckillCount = seckillService.getSeckillCount(seckillId);
			LOGGER.info("一共秒杀出{}件商品", seckillCount);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return Result.ok();
	}
}
