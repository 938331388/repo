package com.lagou.hadoop.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

//封装任务并提交运行
public class WordCountDriver {
    /**
     * 1.获取配置文件对象
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //1.获取配置文件的对象，获取job对象实例
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.fileoutputformat.compress","true");
        conf.set("mapreduce.output.fileoutputformat.compress.type","RECORD");
        conf.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");

        Job job= Job.getInstance(conf, "WordCountDriver");

        //知道程序jar包的本地路径
        job.setJarByClass(WordCountDriver.class);

        //指定map类和reduce类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //指定Mapper输出的kv数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //指定最终输出的kv数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setCombinerClass(WordCountReducer.class);

        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 419430400);

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
