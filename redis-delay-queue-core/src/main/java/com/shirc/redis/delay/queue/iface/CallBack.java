package com.shirc.redis.delay.queue.iface;

import com.shirc.redis.delay.queue.common.Args;

/**
 * @Description 回调接口
 * @Author shirenchuang
 * @Date 2019/7/31 8:57 AM
 **/
public interface CallBack<T extends Args> {

    /**
     * 执行回调接口
     * @param t
     */
    public void execute(T t);


    /**
     * 重试超过2次(总共3次)回调接口;
     * 消费者可以在这个方法里面发送钉钉警告邮件警告等等
     * 回调这个接口是一个单独的线程
     *  @param t
     */
    public void retryOutTimes(T t);


}
