package com.shirc.redis.delay.queue.threads;

import com.shirc.redis.delay.queue.common.Args;
import com.shirc.redis.delay.queue.iface.impl.AbstractTopicRegister;
import com.shirc.redis.delay.queue.redis.RedisOperation;
import com.shirc.redis.delay.queue.utils.RedisKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * @Description 失败重试线程
 * @Author shirenchuang
 * @Date 2019/8/4 10:02 AM
 **/
public class RetryOutTimesThread {
    private static final Logger logger = LoggerFactory.getLogger(RetryOutTimesThread.class);


    private static RetryOutTimesThread instance = new RetryOutTimesThread();
    public static RetryOutTimesThread getInstance(){
        return instance;
    }

    /**异步回调 重试2次仍然失败 通知**/
    private static ExecutorService NOTIFY_RETRY_OUT_TIME = Executors.newCachedThreadPool();



    public void callBackExceptionTryRetry(AbstractTopicRegister register, Args args, RedisOperation redisOperation) {
        if(args.getRetryCount()>1){
            //重试了2次了,不再重试了; 异步执行回调通知接口;
            NOTIFY_RETRY_OUT_TIME.execute(()->register.retryOutTimes(args));
        }else if(args.getRetryCount()>=0){
            args.setRetryCount(args.getRetryCount()+1);
            redisOperation.retryJob(register.getTopic(),args.getId(),args);
            logger.warn("失败任务第{}次放入重试:{}:topicId:{},Args:{}",args.getRetryCount(),
                    RedisKeyUtil.getTopicId(register.getTopic(),args.getId()),args);
        }
    }

    //停机
    public void toStop(){
        ShutdownThread.closeExecutor(NOTIFY_RETRY_OUT_TIME,"重试任然失败通知线程池");
    }
}
