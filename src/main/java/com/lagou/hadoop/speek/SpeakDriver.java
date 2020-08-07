package com.lagou.hadoop.speek;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SpeakDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        final Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "speekDriver");

        job.setJarByClass(SpeakDriver.class);
        job.setMapperClass(SpeekMapper.class);
        job.setReducerClass(SpeekReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SpeakBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SpeakBean.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean flag = job.waitForCompletion(true);

        System.exit(flag? 0:1);
    }
}
