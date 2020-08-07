package com.lagou.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

//继承的Reducer类有4个泛型参数，2对KV
//第一对kv，类型要与Mapper输出保持一致 Text， IntWritable
//第二对KV：自己设计决定输出的结果数据是什么类型

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    //1.重写Reduce方法

    //Text key:map方法输出的key，本案例中就是单词
    //Iterable<IntWritable> values:一组key相同的kv的value组成的集合

    /*
    假设map方法: hello 1, hello 2
    reduce的key和value
    key hello
    value <1, 2>
    reduce的key和value是什么
     */
    IntWritable total = new IntWritable();

    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        //遍历key对应的values，然后累加结果
        for (IntWritable value : values) {
            sum += value.get();
        }

        //直接输出当前的key对应的sum值，结果就是单词出现的次数
        total.set(sum);
        context.write(key, total);
    }

}
