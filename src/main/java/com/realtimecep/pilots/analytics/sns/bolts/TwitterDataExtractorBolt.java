package com.realtimecep.pilots.analytics.sns.bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Twitter Data Extractor Bolts Class.
 * <p/>
 *
 * @author <a href="iamtedwon@gmail.com">Ted Won</a>
 * @version 0.1.0
 */
public class TwitterDataExtractorBolt extends BaseBasicBolt {

    private static final long serialVersionUID = -3025639777071957758L;

    private Logger logger = LoggerFactory.getLogger(TwitterDataExtractorBolt.class);


    @Override
    public void prepare(Map conf, TopologyContext context) {
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String tweet = (String) input.getValueByField("tweet");

//        logger.info(tweet);

        String[] words = tweet.split(" ");
        for (String word : words) {
            word = word.trim();
            if (!word.isEmpty()) {
                word = word.toLowerCase();
                collector.emit(new Values(word));
            }
        }
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
