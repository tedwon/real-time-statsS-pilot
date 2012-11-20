package com.realtimecep.pilots.analytics.sns;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.realtimecep.pilots.analytics.sns.bolts.TwitterDataExtractorBolt;
import com.realtimecep.pilots.analytics.sns.bolts.WordCountSaverBolt;
import com.realtimecep.pilots.analytics.sns.bolts.WordCounterBolt;
import com.realtimecep.pilots.analytics.sns.spouts.twitter.twitter4j.TwitterFilterStreamSpout;

/**
 * Topology Starter Class.
 * <p/>
 * java -cp rt-statss-pilot-0.1.0-SNAPSHOT-jar-with-dependencies.jar -Dlog4j.configuration=log4j.xml com.realtimecep.pilots.analytics.sns.LocalTopologyStarter <twitter id> <twitter pwd> <track(comma separated filter terms)> localhost 6379
 * <p/>
 * storm jar rt-statss-pilot-0.1.0-SNAPSHOT-jar-with-dependencies com.realtimecep.pilots.analytics.sns.ClusterTopologyStarter <twitter id> <twitter pwd> <track(comma separated filter terms)> localhost 6379
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

        builder.setBolt("data-extractor", new TwitterDataExtractorBolt())
                .shuffleGrouping("twitter-stream-reader");

        builder.setBolt("", new WordCountSaverBolt())
                .fieldsGrouping("data-extractor", new Fields("word"));


        // Step2. Define Configuration
        Config conf = new Config();

        conf.put("user", args[0]);
        conf.put("password", args[1]);
        conf.put("track", args[2]);
        conf.put("redisHost", args[3]);
        conf.put("redisPort", args[4]);

        conf.setDebug(false);


        // Step3. Run Topology
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("statss-analytics-topology", conf, builder.createTopology());

    }
}
