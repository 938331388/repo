package com.lagou.hadoop.group;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class GroupDrver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(GroupDrver.class);
        job.setMapperClass(GroupMapper.class);
        job.setReducerClass(GroupReducer.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setPartitionerClass(CostomPartitioner.class);
        job.setGroupingComparatorClass(CustomGroupingComparator.class);
        job.setNumReduceTasks(2);

        FileInputFormat.setInputPaths(job, new Path("/Users/jianfeng.wen/online/worldcount/src/main/resources/data/GroupingComparator/groupingComparator.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/jianfeng.wen/online/worldcount/wenjf4"));

        boolean flag = job.waitForCompletion(true);

        System.exit(flag ? 0: 1);
    }
}
