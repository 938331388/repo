package com.lagou.hadoop.rownum;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RowNumReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    IntWritable rowNum = new IntWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int i = 1;
        for (Text value : values) {
            rowNum.set(i++);
            context.write(rowNum, value);
        }
    }
}
