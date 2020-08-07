package com.lagou.hadoop.sequence;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.jute.Record;

import java.io.IOException;

//自定义inputFormat->文件路径+名称,value->整个文件内容
public class CustomInputFormat extends FileInputFormat<Text, BytesWritable> {
    //重写是否可切分

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        //对于当前需求，不需要把文件切分,保证一个切片就是一个文件
        return false;
    }

    //读取数据的对象
    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        CustomRecordReader recordReader = new CustomRecordReader();
        recordReader.initialize(inputSplit, taskAttemptContext);
        return recordReader;
    }
}
