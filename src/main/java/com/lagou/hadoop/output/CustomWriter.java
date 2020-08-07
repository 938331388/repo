package com.lagou.hadoop.output;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class CustomWriter extends RecordWriter<Text, NullWritable> {
    //写出数据的逻辑
    private FSDataOutputStream lagouOut;
    private FSDataOutputStream otherOut;

    public CustomWriter(FSDataOutputStream lagouOut, FSDataOutputStream otherOut) {
        this.lagouOut = lagouOut;
        this.otherOut = otherOut;
    }

    public CustomWriter(){

    }

    @Override
    public void write(Text key, NullWritable nullWritable) throws IOException, InterruptedException {
        //写数据写到输出流
        String line = key.toString();
        if(line.contains("lagou")) {
            lagouOut.write(line.getBytes());
            lagouOut.write("\r\n".getBytes());
        } else {
            otherOut.write(line.getBytes());
            otherOut.write("\r\n".getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(lagouOut);
        IOUtils.closeStream(otherOut);
    }
}
