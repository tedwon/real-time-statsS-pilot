package com.realtimecep.pilots.analytics.sns;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import com.realtimecep.pilots.analytics.sns.bolts.TwitterDataExtractorBolt;
import com.realtimecep.pilots.analytics.sns.spouts.twitter.twitter4j.TwitterFilterStreamSpout;

/**
 * Topology Starter Class.
 * <p/>
 * java -cp rt-statss-pilot-0.1.0-SNAPSHOT-jar-with-dependencies.jar -Dlog4j.configuration=log4j.xml com.realtimecep.pilots.analytics.sns.LocalTopologyStarter
 * <p/>
 * storm jar rt-statss-pilot-0.1.0-SNAPSHOT-jar-with-dependencies.jar com.realtimecep.pilots.analytics.sns.ClusterTopologyStarter
 * storm kill statss-analytics-topology
 *
 * @author <a href="iamtedwon@gmail.com">Ted Won</a>
 * @version 0.1.0
 */
public class ClusterTopologyStarter {

    public static void main(String[] args) throws Exception {

        // Step1. Define Topology
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("twitter-stream-reader", new TwitterFilterStreamSpout(), 1);

        builder.setBolt("data-extractor", new TwitterDataExtractorBolt(), 1)
                .shuffleGrouping("twitter-stream-reader");


        // Step2. Define Configuration
        Config conf = new Config();

        conf.put("user", args[0]);
        conf.put("password", args[1]);
        conf.put("track", args[2]);

        conf.setDebug(false);
        conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0);
        conf.setNumWorkers(2);
        conf.setOptimize(false);


        // Step3. Run Topology
        StormSubmitter.submitTopology("statss-analytics-topology", conf, builder.createTopology());

    }
}
