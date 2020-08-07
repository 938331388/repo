package com.lagou.hadoop.sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class CustomRecordReader extends RecordReader<Text, BytesWritable> {
    //初始化方法,把切片以及上下文提为全局
    //把切片以及上下文提升为全局
    private FileSplit split;
    //hadoop配置文件对象
    private Configuration conf;

    //定义key，value的成员变量
    private Text key=new Text();
    private BytesWritable value = new BytesWritable();

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        split = (FileSplit) inputSplit;
        conf=taskAttemptContext.getConfiguration();
    }

    private Boolean flag=true;
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if(flag) {
            //对于当前split来说，只需要读取一次即可，因为一次就把整个文件全部读取了
            //准备一个数组存放读取到的数据，数据大小是多少？
            byte[] content = new byte[(int) split.getLength()];
            final Path path = split.getPath();//获取切片的path信息
            FileSystem fs = path.getFileSystem(conf);//获取文件系统对象
            FSDataInputStream fis = fs.open(path);//获取输入流
            IOUtils.readFully(fis, content, 0, content.length);

            key.set(path.toString());
            value.set(content, 0, content.length);
            IOUtils.closeStream(fis);
            //把再次读取的开关置为false
            flag=false;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
