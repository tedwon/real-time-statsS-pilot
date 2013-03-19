package com.realtimecep.storm.starter.trident;

/**
 * Created with IntelliJ IDEA.
 * User: ted
 * Date: 3/13/13
 * Time: 4:19 PM
 * To change this template use File | Settings | File Templates.
 */
public class MyEvent {

    private String data1;
    private String data2;
    private String data3;
    private String data4;
    private String data5;

    public MyEvent() {
    }

    public MyEvent(String data1, String data2, String data3, String data4, String data5) {
        this.data1 = data1;
        this.data2 = data2;
        this.data3 = data3;
        this.data4 = data4;
        this.data5 = data5;
    }

    public MyEvent(String line) {
        String[] strings = line.split(",");
        this.data1 = strings[0];
        this.data2 = strings[1];
        this.data3 = strings[2];
        this.data4 = strings[3];
        this.data5 = strings[4];
    }

    public String getData1() {
        return data1;
    }

    public void setData1(String data1) {
        this.data1 = data1;
    }

    public String getData2() {
        return data2;
    }

    public void setData2(String data2) {
        this.data2 = data2;
    }

    public String getData3() {
        return data3;
    }

    public void setData3(String data3) {
        this.data3 = data3;
    }

    public String getData4() {
        return data4;
    }

    public void setData4(String data4) {
        this.data4 = data4;
    }

    public String getData5() {
        return data5;
    }

    public void setData5(String data5) {
        this.data5 = data5;
    }
}
