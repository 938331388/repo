package com.lagou.hadoop.speek;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SpeekReducer extends Reducer<Text, SpeakBean, Text, SpeakBean> {

    protected void reduce(Text key, Iterable<SpeakBean> values, Context context) throws IOException, InterruptedException {

        Long sumSelfDuration = 0L;
        Long sumThirdPartDuration = 0L;

        for (SpeakBean bean : values) {
            Long thirdPartDuration = bean.getThirdPartDuration();
            Long selfDuration = bean.getSelfDuration();
            sumSelfDuration += selfDuration;
            sumThirdPartDuration += thirdPartDuration;
        }

        final SpeakBean bean = new SpeakBean(sumSelfDuration, sumThirdPartDuration, key.toString());
        context.write(key, bean);
    }
}