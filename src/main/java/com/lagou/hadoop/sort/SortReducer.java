package com.lagou.hadoop.sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<SpeakBean, NullWritable, SpeakBean, NullWritable> {
    //reduce方法调用是相同的key的value的集合组成的集合调用一次

    /**
     * java中如何判断两个对象是否相等
     * 根据equals，比较的还是地址值
     */
    @Override
    protected void reduce(SpeakBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for (NullWritable value : values) {
            context.write(key, value);
        }

    }
}
