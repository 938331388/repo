package com.lagou.hadoop.partition;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PartitionBean implements Writable {

    public PartitionBean() {

    }


    public PartitionBean(String id, String deviceId, String appKey, String ip, Long selfDuration, Long thirdPartDuration, String status) {
        this.id = id;
        this.deviceId = deviceId;
        this.appKey = appKey;
        this.ip = ip;
        this.selfDuration = selfDuration;
        this.thirdPartDuration = thirdPartDuration;
        this.status = status;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(deviceId);
        out.writeUTF(appKey);
        out.writeUTF(ip);
        out.writeLong(selfDuration);
        out.writeLong(thirdPartDuration);
        out.writeUTF(status);
    }

    //序列化和反序列化顺序保持一致
    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.deviceId = in.readUTF();
        this.appKey = in.readUTF();
        this.ip = in.readUTF();
        this.selfDuration = in.readLong();
        this.thirdPartDuration = in.readLong();
        this.status = in.readUTF();

    }

    private String id;//日志id
    private String deviceId;
    private String appKey;
    private String ip;
    private Long selfDuration;//自有时长
    private Long thirdPartDuration;//第三方内容时长
    private String status;//状态码

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getAppKey() {
        return appKey;
    }

    public void setAppKey(String appKey) {
        this.appKey = appKey;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
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

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return id + '\t' +
            deviceId + '\t' +
            appKey + '\t' +
            ip + '\t' +
            selfDuration + '\t' +
            thirdPartDuration + '\t' +
            status;
    }
}
