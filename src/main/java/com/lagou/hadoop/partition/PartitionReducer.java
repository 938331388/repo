package com.lagou.hadoop.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PartitionReducer extends Reducer<Text, PartitionBean, Text, PartitionBean> {
    @Override
    protected void reduce(Text key, Iterable<PartitionBean> values, Context context) throws IOException, InterruptedException {
        for (PartitionBean bean : values) {
            context.write(key, bean);
        }
    }
}
