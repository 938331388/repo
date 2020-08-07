package com.lagou.hadoop.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CustomOutputFormat extends FileOutputFormat<Text, NullWritable> {
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        //定义写出路径信息，并获取到输出流传入writer对象中
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream lagou = fs.create(new Path("/Users/jianfeng.wen/online/worldcount/lagou.log"));
        FSDataOutputStream other = fs.create(new Path("/Users/jianfeng.wen/online/worldcount/other.log"));
        CustomWriter customWriter = new CustomWriter(lagou, other);

        return customWriter;

    }
}
