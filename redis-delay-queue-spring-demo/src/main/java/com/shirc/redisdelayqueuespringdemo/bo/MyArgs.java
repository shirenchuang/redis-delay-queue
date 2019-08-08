package com.shirc.redisdelayqueuespringdemo.bo;

import com.shirc.redis.delay.queue.common.Args;

import java.util.Date;

/**
 * @Description TODO
 * @Author shirenchuang
 * @Date 2019/8/1 2:05 PM
 **/
public class MyArgs extends Args {


    private Date shoudRunTime;

    private Date putTime;

    private String content;

    public Date getShoudRunTime() {
        return shoudRunTime;
    }

    public void setShoudRunTime(Date shoudRunTime) {
        this.shoudRunTime = shoudRunTime;
    }

    public Date getPutTime() {
        return putTime;
    }

    public void setPutTime(Date putTime) {
        this.putTime = putTime;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }


    @Override
    public String toString() {
        return "MyArgs{" +
                "id='" + getId() + '\'' +
                ", retryCount=" + getRetryCount() +
                ", reentry=" + getRetryCount() +
                ", shoudRunTime=" + shoudRunTime +
                ", putTime=" + putTime +
                ", content='" + content + '\'' +
                '}';
    }
}
