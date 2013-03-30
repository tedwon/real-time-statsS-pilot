package com.realtimecep.storm.starter.wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.realtimecep.storm.starter.wordcount.bolts.WordCounter;
import com.realtimecep.storm.starter.wordcount.bolts.WordNormalizer;
import com.realtimecep.storm.starter.wordcount.spouts.WordReader;


/**
 * Topology Starter Class.
 * <p/>
 * $ java -cp rt-statss-pilot-0.1.0-SNAPSHOT-jar-with-dependencies.jar -Dlog4j.configuration=log4j.xml com.realtimecep.storm.starter.wordcount.WordCountTopologyStarter <input file path>
 * $ java -cp rt-statss-pilot-0.1.0-SNAPSHOT-jar-with-dependencies.jar -Dlog4j.configuration=log4j.xml com.realtimecep.storm.starter.wordcount.WordCountTopologyStarter src/main/resources/words.txt
 * <p/>
 * $ mvn exec:java -Dexec.mainClass ="com.realtimecep.storm.starter.wordcount.WordCountTopologyStarter" -Dexec.args ="src/main/resources/words.txt"
 *
 * @author <a href="iamtedwon@gmail.com">Ted Won</a>
 * @version 0.1.0
 */
public class WordCountTopologyStarter {
    public static void main(String[] args) throws InterruptedException {

        //Topology definition
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new WordReader());
        builder.setBolt("word-normalizer", new WordNormalizer())
                .shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCounter(), 1)
                .fieldsGrouping("word-normalizer", new Fields("word"));

        //Configuration
        Config conf = new Config();
        conf.put("wordsFile", args[0]);
        conf.setDebug(true);
        //Topology run
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Word-Count-Topology", conf, builder.createTopology());
        Thread.sleep(10000);
        cluster.shutdown();
    }
}
