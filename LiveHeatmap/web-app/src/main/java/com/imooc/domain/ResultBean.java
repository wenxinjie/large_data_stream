package com.imooc.domain;

public class ResultBean {

    private double lng;

    private double lat;

    private long count;

    private double sent;

    public double getSent() {return sent; }

    public void setSent(double sent) {this.sent = sent; }

    public double getLng() {
        return lng;
    }

    public void setLng(double lng) {
        this.lng = lng;
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
