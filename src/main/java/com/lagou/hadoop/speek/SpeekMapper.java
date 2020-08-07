package com.lagou.hadoop.speek;

//四个参数：氛围两队kv
//第一对kv：map输入的kv类型；k--> 一行文本偏移量，v-->

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * 001 001577c3 kar_890809 120.196.100.99 1116 954
 * 200
 * ⽇日志id 设备id appkey(合作硬件⼚厂商) ⽹网络ip ⾃自有内容时⻓长(秒) 第三⽅方内容时⻓长 (秒) ⽹网络状态码
 */

public class SpeekMapper extends Mapper<LongWritable, Text, Text, SpeakBean> {

    Text devID = new Text();

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        final String line = value.toString();
        String[] fields = line.split("\t");
        //自有时长
        String selfDuration = fields[4];
        //第三方时长
        String thirdPartDuration = fields[5];
        //设备id
        String deviceID = fields[1];

        SpeakBean speakBean = new SpeakBean(Long.parseLong(selfDuration), Long.parseLong(thirdPartDuration), deviceID);
        devID.set(deviceID);
        context.write(devID, speakBean);
    }

}
