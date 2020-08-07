package com.lagou.hadoop.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class CustomPartitioner extends Partitioner<Text, PartitionBean> {
    @Override
    public int getPartition(Text text, PartitionBean partitionBean, int i) {
        int partition=0;
        if(text.toString().equals("kar")) {
            //只需要保证此if条件的数据获得同一个分区编号即可
            partition=0;
        } else if(text.toString().equals("pandora")) {
            partition=1;
        } else {
            partition=2;
        }
        return partition;
    }
}
