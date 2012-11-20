package com.realtimecep.pilots.analytics.sns;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import com.realtimecep.pilots.analytics.sns.spouts.twitter.httpclient.TwitterApiStreamingSpout;
import com.realtimecep.pilots.analytics.sns.spouts.twitter.twitter4j.TwitterFilterStreamSpout;

/**
 * Topology Starter Class.
 * <p/>
 * java -cp rt-statss-pilot-0.1.0-SNAPSHOT-jar-with-dependencies -Dlog4j.configuration=log4j.xml com.realtimecep.pilots.analytics.sns.LocalTopologyStarter
 * <p/>
 * storm jar rt-statss-pilot-0.1.0-SNAPSHOT-jar-with-dependencies com.realtimecep.pilots.analytics.sns.ClusterTopologyStarter
 * storm kill statss-analytics-topology
 *
 * @author <a href="iamtedwon@gmail.com">Ted Won</a>
 * @version 0.1.0
 */
public class LocalTopologyStarter {

    public static void main(String[] args) {

        // Step1. Define Topology
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("twitter-stream-reader", new TwitterFilterStreamSpout());


        // Step2. Define Configuration
        Config conf = new Config();

        conf.put("user", args[0]);
        conf.put("password", args[1]);
        conf.put("track", args[2]);

        conf.setDebug(false);


        // Step3. Run Topology
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("statss-analytics-topology", conf, builder.createTopology());

    }
}
