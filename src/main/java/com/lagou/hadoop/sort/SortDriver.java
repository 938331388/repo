package com.lagou.hadoop.sort;

import com.lagou.hadoop.wordcount.WordCountDriver;
import com.lagou.hadoop.wordcount.WordCountMapper;
import com.lagou.hadoop.wordcount.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SortDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取配置文件的对象，获取job对象实例
        Configuration conf = new Configuration();
        Job job= Job.getInstance(conf, "SortDriver");

        //知道程序jar包的本地路径
        job.setJarByClass(SortDriver.class);

        //指定map类和reduce类
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        //指定Mapper输出的kv数据类型
        job.setMapOutputKeyClass(SpeakBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        //指定最终输出的kv数据类型
        job.setOutputKeyClass(SpeakBean.class);
        job.setOutputValueClass(NullWritable.class);

        //指定job处理的原始数据路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //指定输出数据路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //提交作业
        boolean flag = job.waitForCompletion(true);
        //jvm退出，正常退出0， 非0则是错误退出
        System.exit(flag ? 0: 1);

    }

}
