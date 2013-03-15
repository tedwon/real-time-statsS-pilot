package com.realtimecep.storm.starter.drpc;

import backtype.storm.utils.DRPCClient;

public class DRPCClientTest {

    public static void main(String[] args) throws Exception {

        DRPCClient client = new DRPCClient("daisy02", 50010);

        for (String word : new String[]{"hello", "goodbye"}) {
            System.out.println("Result for \"" + word + "\": "
                    + client.execute("exclamation", word));
        }
    }
}