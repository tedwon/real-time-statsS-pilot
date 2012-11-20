package com.realtimecep.pilots.analytics.sns.bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: ted
 * Date: 11/20/12
 * Time: 3:06 PM
 * To change this template use File | Settings | File Templates.
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
