package com.lagou.hadoop.group;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CustomGroupingComparator extends WritableComparator {

    public CustomGroupingComparator() {
        super(OrderBean.class, true);//注册自定义的GroupingComparator接受OrderBean对象
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        //a和b是orderBean的对象
        OrderBean o1 = (OrderBean)a;
        OrderBean o2 = (OrderBean)b;

        return o1.getOrderId().compareTo(o2.getOrderId());
    }
}
