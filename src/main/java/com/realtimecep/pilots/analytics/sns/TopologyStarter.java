package com.realtimecep.pilots.analytics.sns;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import com.realtimecep.pilots.analytics.sns.spouts.twitter.TwitterApiStreamingSpout;

/**
 * Topology Starter Class.
 * <p/>
 *
 * @author <a href="iamtedwon@gmail.com">Ted Won</a>
 * @version 0.1.0
 */
public class TopologyStarter {

    public static void main(String[] args) {

        // Step1. Define Topology
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("twitter-stream-reader", new TwitterApiStreamingSpout());


        // Step2. Define Configuration
        Config conf = new Config();

        conf.put("user", args[0]);
        conf.put("password", args[1]);
        conf.put("track", "Barack,Obama");

        conf.setDebug(false);

        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

        // Step3. Run Topology
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("statss-analytics-topology", conf, builder.createTopology());

    }
}
