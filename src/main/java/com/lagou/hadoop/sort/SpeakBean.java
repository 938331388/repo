package com.lagou.hadoop.sort;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SpeakBean implements WritableComparable<SpeakBean> {

    private Long selfDrutation;
    private Long thirdPartDuration;
    private String deviceId;
    private Long sumDruation;

    public SpeakBean() {

    }

    public SpeakBean(Long selfDrutation, Long thirdPartDuration, String deviceId, Long sumDruation) {
        this.selfDrutation = selfDrutation;
        this.thirdPartDuration = thirdPartDuration;
        this.deviceId = deviceId;
        this.sumDruation = sumDruation;
    }

    @Override
    public int compareTo(SpeakBean o) {

        //返回值3种， 0相等， 1小于 -1大于
        //指定按照bean对象的总时长字段的值进行比较
        if(this.sumDruation > o.sumDruation) {
            return -1;
        } else if(this.sumDruation < o.sumDruation) {
            return 1;
        } else {
            return 0;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(selfDrutation);
        out.writeLong(thirdPartDuration);
        out.writeUTF(deviceId);
        out.writeLong(sumDruation);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.selfDrutation=in.readLong();
        this.thirdPartDuration=in.readLong();
        this.deviceId=in.readUTF();
        this.sumDruation=in.readLong();
    }

    @Override
    public String toString() {
        return selfDrutation + "\t"
            + thirdPartDuration +"\t"
            + deviceId +"\t"
            + sumDruation
            ;
    }

    public Long getSelfDrutation() {
        return selfDrutation;
    }

    public void setSelfDrutation(Long selfDrutation) {
        this.selfDrutation = selfDrutation;
    }

    public Long getThirdPartDuration() {
        return thirdPartDuration;
    }

    public void setThirdPartDuration(Long thirdPartDuration) {
        this.thirdPartDuration = thirdPartDuration;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public Long getSumDruation() {
        return sumDruation;
    }

    public void setSumDruation(Long sumDruation) {
        this.sumDruation = sumDruation;
    }
}
