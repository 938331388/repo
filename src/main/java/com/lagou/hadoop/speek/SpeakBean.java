package com.lagou.hadoop.speek;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SpeakBean implements Writable {

    private Long selfDuration;//自有内容时长

    private Long thirdPartDuration;//第三方内容时长

    private String deviceID;

    private Long sumDuration;//总时长

    public SpeakBean() {

    }

    public SpeakBean(Long selfDuration, Long thirdPartDuration, String deviceID) {
        this.selfDuration = selfDuration;
        this.thirdPartDuration = thirdPartDuration;
        this.deviceID = deviceID;
        this.sumDuration = selfDuration + thirdPartDuration;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(selfDuration);
        out.writeLong(thirdPartDuration);
        out.writeUTF(deviceID);
        out.writeLong(sumDuration);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.selfDuration = in.readLong();//自有时长
        this.thirdPartDuration = in.readLong();//第三方时长
        this.deviceID = in.readUTF();//设备id
        this.sumDuration = in.readLong();//总时长

    }

    public String getDeviceID() {
        return deviceID;
    }

    public void setDeviceID(String deviceID) {
        this.deviceID = deviceID;
    }

    public Long getSelfDuration() {
        return selfDuration;
    }

    public void setSelfDuration(Long selfDuration) {
        this.selfDuration = selfDuration;
    }

    public Long getThirdPartDuration() {
        return thirdPartDuration;
    }

    public void setThirdPartDuration(Long thirdPartDuration) {
        this.thirdPartDuration = thirdPartDuration;
    }

    public Long getSumDuration() {
        return sumDuration;
    }

    public void setSumDuration(Long sumDuration) {
        this.sumDuration = sumDuration;
    }

    @Override
    public String toString() {
        return selfDuration + "\t" + thirdPartDuration + "\t" + deviceID + "\t" + sumDuration;
    }
}

