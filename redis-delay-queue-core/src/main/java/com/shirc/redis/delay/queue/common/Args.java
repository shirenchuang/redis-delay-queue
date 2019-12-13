package com.shirc.redis.delay.queue.common;


import java.io.Serializable;

/**
 * @Description 入参
 * @Author shirenchuang
 * @Date 2019/8/1 2:01 PM
 **/
public class Args implements Serializable {

    private static final long serialVersionUID = 66666L;

    /**唯一键 不能为空**/
    private String id;

    /**
     * 已经重试的次数:
     * 重试机制: 默认重试2次; 总共最多执行3次
     * 添加任务的时候可以设置为<0 的值;则表示不希望重试;
     * 回调接口自己做好幂等
     ***/
    private int retryCount;


    /**
     * 重入次数:
     * 这里标记的是当前Job某些异常情况导致并没有真正消费到,然后重新放入待消费池的次数;
     * 比如: BLPOP出来了之后,在去获取Job的时候redis超时了,导致没有正常消费掉;
     * 重入次数最大 3次; 避免某些不可控因素出现,超过3次则丢弃
     */
    private int reentry;

    public Args() {
    }

    public Args(String id) {
        this.id = id;
    }

    public Args(String id, int retryCount) {
        this.id = id;
        this.retryCount = retryCount;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public int getReentry() {
        return reentry;
    }

    public void setReentry(int reentry) {
        this.reentry = reentry;
    }

    @Override
    public String toString() {
        return "Args{" +
                "id='" + id + '\'' +
                ", retryCount=" + retryCount +
                ", reentry=" + reentry +
                '}';
    }



}
