package com.realtimecep.pilots.analytics.sns.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import com.realtimecep.pilots.analytics.sns.spouts.twitter.twitter4j.TwitterFilterStreamSpout;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.Split;

/**
 * Trident Topology Starter Class.
 * <p/>
 * java -cp rt-statss-pilot-0.1.0-SNAPSHOT-jar-with-dependencies.jar -Dlog4j.configuration=log4j.xml com.realtimecep.pilots.analytics.sns.LocalTopologyStarter <twitter id> <twitter pwd> <track(comma separated filter terms)> localhost 6379
 * <p/>
 * storm jar rt-statss-pilot-0.1.0-SNAPSHOT-jar-with-dependencies com.realtimecep.pilots.analytics.sns.ClusterTopologyStarter <twitter id> <twitter pwd> <track(comma separated filter terms)> localhost 6379
 * storm kill statss-analytics-topology
 *
 * @author <a href="iamtedwon@gmail.com">Ted Won</a>
 * @version 0.1.0
 */
public class LocalTridentTopologyStarter {

    public static void main(String[] args) {

        // Step1. Define Topology
        TridentTopology topology = new TridentTopology();
        LocalDRPC drpc = new LocalDRPC();

        TridentState tridentState = topology.newStream("tweets", new TwitterFilterStreamSpout())
                .each(new Fields("tweet"), new HashTagSplitter(), new Fields("word"))
                .groupBy(new Fields("word"))
                .persistentAggregate(new RedisState.Factory(),
                        new Count(),
                        new Fields("count"))
                .parallelismHint(3);

        Stream aggregate = topology.newDRPCStream("counts", drpc)
                .each(new Fields("args"), new Split(), new Fields("word"))
                .stateQuery(tridentState, new Fields("word"), new MapGet(), new Fields("count"))
                .each(new Fields("count"), new FilterNull())
                .groupBy(new Fields("word"))
                .aggregate(new Fields("count"), new Sum(), new Fields("sum"));

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
        cluster.submitTopology("trident-topology", conf, topology.build());

        while (true) {

            String counts = drpc.execute("counts", "안철수 문재인");

            System.out.println(counts);

            Utils.sleep(3000);
        }


    }
}
