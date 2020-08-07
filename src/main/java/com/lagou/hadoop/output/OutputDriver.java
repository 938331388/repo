package com.lagou.hadoop.output;

import com.lagou.hadoop.wordcount.WordCountDriver;
import com.lagou.hadoop.wordcount.WordCountMapper;
import com.lagou.hadoop.wordcount.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OutputDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //1.获取配置文件的对象，获取job对象实例
        Configuration conf = new Configuration();
        Job job= Job.getInstance(conf, "OutputDriver");

        //知道程序jar包的本地路径
        job.setJarByClass(OutputDriver.class);

        //指定map类和reduce类
        job.setMapperClass(OutputMapper.class);
        job.setReducerClass(OutputReducer.class);

        //指定Mapper输出的kv数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //指定最终输出的kv数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(CustomOutputFormat.class);

        //指定job处理的原始数据路径
        FileInputFormat.setInputPaths(job, new Path("/Users/jianfeng.wen/online/worldcount/src/main/resources/data/click_log"));
        //指定输出数据路径
        FileOutputFormat.setOutputPath(job, new Path("/Users/jianfeng.wen/online/worldcount/out"));
        //提交作业
        boolean flag = job.waitForCompletion(true);
        //jvm退出，正常退出0， 非0则是错误退出
        System.exit(flag ? 0: 1);

    }
}
