package com.realtimecep.pilots.analytics.sns;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.realtimecep.pilots.analytics.sns.bolts.TwitterDataExtractorBolt;
import com.realtimecep.pilots.analytics.sns.bolts.WordCountSaverBolt;
import com.realtimecep.pilots.analytics.sns.spouts.twitter.twitter4j.TwitterFilterStreamSpout;
import redis.clients.jedis.Jedis;

import java.util.Timer;
import java.util.TimerTask;

/**
 * Topology Starter Class.
 * <p/>
 * java -cp rt-statss-pilot-0.1.0-SNAPSHOT-jar-with-dependencies.jar -Dlog4j.configuration=log4j.xml com.realtimecep.pilots.analytics.sns.LocalTopologyStarter <twitter id> <twitter pwd> <track(comma separated filter terms)> localhost 6379
 * <p/>
 * storm jar rt-statss-pilot-0.1.0-SNAPSHOT-jar-with-dependencies.jar com.realtimecep.pilots.analytics.sns.ClusterTopologyStarter <twitter id> <twitter pwd> <track(comma separated filter terms)> localhost 6379
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

        builder.setBolt("data-extractor", new TwitterDataExtractorBolt(), 2)
                .shuffleGrouping("twitter-stream-reader");

        builder.setBolt("", new WordCountSaverBolt(), 5)
                .fieldsGrouping("data-extractor", new Fields("word"));


        // Step2. Define Configuration
        Config conf = new Config();

        conf.put("user", args[0]);
        conf.put("password", args[1]);
        conf.put("track", args[2]);
        conf.put("redisHost", args[3]);
        conf.put("redisPort", args[4]);

        conf.setDebug(false);
        conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0);
        conf.setNumWorkers(8);
        conf.setOptimize(true);

        Jedis jedis = new Jedis(args[3], Integer.parseInt(args[4]));
        jedis.connect();
        jedis.flushAll();


        // Step3. Run Topology
        StormSubmitter.submitTopology("statss-analytics-topology", conf, builder.createTopology());

    }
}
