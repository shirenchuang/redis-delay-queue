package com.shirc.redis.delay.queue.utils;

import java.util.concurrent.*;
import java.util.function.Function;

/**
 * @Description 超时机制
 * @Author shirenchuang
 * @Date 2019/8/4 2:48 PM
 **/
public class TimeoutUtil {

    /**执行用户回调接口的 线程池;    计算回调接口的超时时间           **/
    private static ExecutorService executorService = Executors.newCachedThreadPool();

    /**
     * 有超时时间的方法
     * @param timeout 时间秒
     * @return
     */
    public static void timeoutMethod(long timeout, Function function) throws InterruptedException, ExecutionException, TimeoutException {
        FutureTask futureTask = new FutureTask(()->(function.apply("")));
        executorService.execute(futureTask);
        //new Thread(futureTask).start();
        try {
            futureTask.get(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            //e.printStackTrace();
            futureTask.cancel(true);
            throw e;
        }

    }
}
