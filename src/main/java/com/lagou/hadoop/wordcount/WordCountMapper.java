package com.lagou.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//1.继承Mapper类
//2.Mapper类的泛形编:共4个参数，2对KV
//3.第一对KV：map输入参数类型
//4.第二对KV：map输出参数类型
//LongWritable文本偏移量（后面不会用到）
//Text行文本内容
//Text, IntWritable 单词,1

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    /**
     * 1.接收到文本内容，转为String内容
     * 2.按照空格进行切分
     * 3。输出<单词,1>
     */

    final IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1.接收到文本内容，转为String类型
        final String str = value.toString();

        //2.按照空格进行切分
        String[] words = str.split(" ");

        //3.输出<单词, 1>
        final Text word = new Text();

        for (String s : words) {
            word.set(s);
            context.write(word, one);

        }
    }
}
