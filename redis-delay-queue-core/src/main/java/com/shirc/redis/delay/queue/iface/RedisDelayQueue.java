package com.shirc.redis.delay.queue.iface;

import com.shirc.redis.delay.queue.common.Args;
import com.shirc.redis.delay.queue.common.RunTypeEnum;


/**
 * @Description 提供给客户端使用的 延迟队列操作
 * @Author shirenchuang
 * @Date 2019/7/30 4:58 PM
 **/
public interface RedisDelayQueue {


    /**
     * 异步新增一个延迟任务
     * @param args
     * @param topic
     * @param delayTimeMillis
     */
    void addAsync(Args args, String topic, long delayTimeMillis);

    /**
     * 新增一个延迟任务
     * @param args  用户入参
     * @param topic
     * @param runTimeMillis  执行时间 单位: 毫秒
     */
    public  void add(Args args, String topic, long runTimeMillis,RunTypeEnum runTypeEnum);


    /**
     * 新增一个延迟任务
     * @param args
     * @param delayTimeMillis   需要延迟的时间:  单位: 毫秒
     * @param topic
     */
    public void add(Args args, long delayTimeMillis, String topic,RunTypeEnum runTypeEnum);

    /**
     * 删除一个延迟队列
     * @param topic
     * @param id
     */
    public void delete(String topic, String id,RunTypeEnum runTypeEnum);

    /**
     * 异步删除一个延迟队列
     * @param topic
     * @param id
     */
    public void deleteAsync(String topic, String id);








}
