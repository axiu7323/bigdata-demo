package com.demo.pojo;

public class ItemEventCount {
    public String productId;  //商品ID
    public String eventId;  //事件ID
    public String categoryId;  //分类ID
    public long count;  //商品点击量
    public long windowStart;  //窗口开始时间
    public long windowEnd;  //窗口结束时间

    public ItemEventCount(String productId, String eventId, String categoryId, long count, long windowStart, long windowEnd) {
        this.productId = productId;
        this.eventId = eventId;
        this.categoryId = categoryId;
        this.count = count;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    public ItemEventCount() {
    }

    @Override
    public String toString() {
        return "ItemEventCount{" +
                "productId='" + productId + '\'' +
                ", eventId='" + eventId + '\'' +
                ", categoryId='" + categoryId + '\'' +
                ", count=" + count +
                ", windowStart=" + windowStart +
                ", windowEnd=" + windowEnd +
                '}';
    }
}
