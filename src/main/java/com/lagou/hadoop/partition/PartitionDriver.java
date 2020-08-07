package com.lagou.hadoop.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PartitionDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(PartitionDriver.class);
        job.setMapperClass(PartitionMapper.class);
        job.setReducerClass(PartitionReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PartitionBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PartitionBean.class);

        job.setPartitionerClass(CustomPartitioner.class);
        //reduceTask不设置，默认是一个
        job.setNumReduceTasks(2);

        FileInputFormat.setInputPaths(job, new Path("/Users/jianfeng.wen/online/worldcount/src/main/resources/data/mr-writable/speak.data"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/jianfeng.wen/online/worldcount/wenjf2"));

        boolean flag = job.waitForCompletion(true);

        System.exit(flag ? 0: 1);
    }

}
