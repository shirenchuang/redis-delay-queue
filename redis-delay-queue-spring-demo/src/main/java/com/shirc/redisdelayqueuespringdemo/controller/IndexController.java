package com.shirc.redisdelayqueuespringdemo.controller;

import com.shirc.redis.delay.queue.common.RunTypeEnum;
import com.shirc.redis.delay.queue.core.RedisDelayQueueContext;
import com.shirc.redis.delay.queue.iface.RedisDelayQueue;
import com.shirc.redisdelayqueuespringdemo.bo.MyArgs;
import com.shirc.redisdelayqueuespringdemo.delayqueues.DelayQueueDemo2;
import com.shirc.redisdelayqueuespringdemo.delayqueues.TopicEnums;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Date;
import java.util.UUID;

/**
 * @Description TODO
 * @Author shirenchuang
 * @Date 2019/8/1 9:40 AM
 **/
@Controller
@ResponseBody
public class IndexController {


    @Autowired
    RedisDelayQueue redisDelayQueue;


    @Autowired
    RedisDelayQueueContext redisDelayQueueContext;

    @Autowired
    DelayQueueDemo2 delayQueueDemo2;



    /**
     *
     */
    @GetMapping("/addJob")
    public void addJob(Long rt,Integer type ){
        if(rt ==null){
            rt = System.currentTimeMillis()+30000;
        }
        MyArgs myArgs = new MyArgs();
        String id = UUID.randomUUID().toString();
        myArgs.setId(id);
        myArgs.setPutTime(new Date());
        myArgs.setShoudRunTime(new Date(rt));
        myArgs.setContent("lalalalala");
        redisDelayQueue.add(myArgs,TopicEnums.DEMO_TOPIC.getTopic(),rt,type==null?RunTypeEnum.ASYNC:RunTypeEnum.SYNC);
    }

    @GetMapping("/addJob2")
    public void addJob2(Long delayTime,String userId ){
        delayQueueDemo2.addDemo2DelayQueue(userId,delayTime);
    }

    @GetMapping("/delJob2")
    public void delJob2(Long delayTime,String userId ){
        delayQueueDemo2.delDemo2Queue(userId);
    }











    private Date getDate(long millis){
        Date date = new Date();
        date.setTime(millis);
        return date;
    }



}
