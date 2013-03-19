package com.realtimecep.storm.starter.trident;

import backtype.storm.utils.DRPCClient;

public class DRPCClientTest {

    public static void main(String[] args) throws Exception {

        DRPCClient client = new DRPCClient("daisy02", 50010);

        while (true) {
            System.out.println("DRPC RESULT: " + client.execute("words", "ted trinity"));
            Thread.sleep(1000);
        }
    }
}