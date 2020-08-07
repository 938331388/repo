package com.lagou.hadoop.rownum;


import com.lagou.hadoop.sequence.CustomInputFormat;
import com.lagou.hadoop.sequence.SequcenDriver;
import com.lagou.hadoop.sequence.SequcenMapper;
import com.lagou.hadoop.sequence.SequcenReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class RowNumDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取配置文件的对象，获取job对象实例
        Configuration conf = new Configuration();
        Job job= Job.getInstance(conf, "RowNumDriver");

        //知道程序jar包的本地路径
        job.setJarByClass(RowNumDriver.class);

        //指定map类和reduce类
        job.setMapperClass(RowNumMapper.class);
        job.setReducerClass(RowNumReducer.class);

        //指定Mapper输出的kv数据类型
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        //指定最终输出的kv数据类型
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        //指定job处理的原始数据路径
        FileInputFormat.setInputPaths(job, new Path("/Users/jianfeng.wen/online/worldcount/src/main/resources/data/rownum"));
        //指定输出数据路径
        FileOutputFormat.setOutputPath(job, new Path("/Users/jianfeng.wen/online/worldcount/wenjf22"));
        //提交作业
        boolean flag = job.waitForCompletion(true);
        //jvm退出，正常退出0， 非0则是错误退出
        System.exit(flag ? 0: 1);

    }
}
