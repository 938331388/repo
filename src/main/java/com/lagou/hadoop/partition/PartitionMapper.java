package com.lagou.hadoop.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * 1.读取一行文本，按照制表符切分
 * 2.解析出appkey字段，其余数据封装为PartitionBean对象（实现writerable接口）
 * 3.设计map()输出的kv，key-->appkey（依靠该字段完成分区），partitionBean对象作为输出
 *
 */



public class PartitionMapper extends Mapper<LongWritable, Text, Text, PartitionBean> {

    /**
     *
     * 001 001577c3 kar_890809 120.196.100.99 1116 954
     * 200
     * ⽇日志id 设备id appkey(合作硬件⼚厂商) ⽹网络ip ⾃自有内容时⻓长(秒) 第三⽅方内容时⻓长 (秒) ⽹网络状态码
     */
    PartitionBean bean = new PartitionBean();
    Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        final String[] fields = value.toString().split("\t");
        String appKey = fields[2];
        bean.setId(fields[0]);
        bean.setDeviceId(fields[1]);
        bean.setAppKey(fields[2]);
        bean.setIp(fields[3]);
        bean.setSelfDuration(Long.parseLong(fields[4]));
        bean.setThirdPartDuration(Long.parseLong(fields[5]));
        bean.setStatus(fields[6]);
        k.set(appKey);
        context.write(k, bean);
    }

}
