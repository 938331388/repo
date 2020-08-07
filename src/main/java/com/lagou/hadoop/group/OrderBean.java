package com.lagou.hadoop.group;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {
    //指定排序规则，先按照订单id比较，再按照金额比较
    private String orderId;
    private Double price;

    public OrderBean() {

    }

    public OrderBean(String orderId, Double price) {
        this.orderId = orderId;
        this.price = price;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    //按照金额降序排
    @Override
    public int compareTo(OrderBean o) {
        int res = this.orderId.compareTo(o.orderId);
        if(res==0) {
            //订单id相同,比较金额
            res = -this.price.compareTo(o.price);
        }

        return res;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId=in.readUTF();
        this.price=in.readDouble();
    }

    @Override
    public String toString() {
        return "OrderBean{" +
            "orderId='" + orderId + '\'' +
            ", price=" + price +
            '}';
    }
}
