package com.lagou.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WorldCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable total = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //进行局部汇总

        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        total.set(sum);
        context.write(key, total);
    }
}
