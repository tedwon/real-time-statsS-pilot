package com.realtimecep.pilots.analytics.sns.bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Twitter Data Extractor Bolts Class.
 * <p/>
 *
 * @author <a href="iamtedwon@gmail.com">Ted Won</a>
 * @version 0.1.0
 */
public class TwitterDataExtractorBolts extends BaseBasicBolt {

    private static final long serialVersionUID = -3025639777071957758L;

    private org.slf4j.Logger logger = LoggerFactory.getLogger(TwitterDataExtractorBolts.class);


    @Override
    public void prepare(Map conf, TopologyContext context) {
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String tweet = (String) input.getValueByField("tweet");

        logger.info(tweet);
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
