package com.realtimecep.pilots.analytics.sns.bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

/**
 * Word Counter Bolt Class.
 * <p/>
 *
 * @author <a href="iamtedwon@gmail.com">Ted Won</a>
 * @version 0.1.0
 */
public class WordCountSaverBolt extends BaseBasicBolt {

    private Logger logger = LoggerFactory.getLogger(WordCountSaverBolt.class);


    Integer id;
    String name;
    Map<String, Integer> counters;

    private Jedis jedis;

    /**
     * At the end of the spout (when the cluster is shutdown
     * We will show the word counters
     */
    @Override
    public void cleanup() {

    }

    /**
     * On create
     */
    @Override
    public void prepare(Map conf, TopologyContext context) {

        this.counters = new HashMap<String, Integer>();

        this.name = context.getThisComponentId();
        this.id = context.getThisTaskId();

        String redisHost = (String) conf.get("redisHost");
        Integer redisPort = Integer.parseInt((String) conf.get("redisPort"));

        this.jedis = new Jedis(redisHost, redisPort);
        this.jedis.connect();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }


    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String str = input.getString(0);
        /**
         * If the word dosn't exist in the map we will create
         * this, if not We will add 1
         */
        if (!counters.containsKey(str)) {
            counters.put(str, 1);
        } else {
            Integer c = counters.get(str) + 1;
            counters.put(str, c);
        }


//        logger.info("### " + counters);

        hincrBy(str, counters.get(str));


    }


    private void hincrBy(String key, Integer val) {

        logger.info("key: " + key + ", val: " + val);

//        if (!"대선".equals(key)
//                && !"박근혜".equals(key)
//                && !"snspage".equals(key)
//                && !"박".equals(key)
//                && !"대".equals(key)
//                && !"toptop".equals(key)
//                && !"박근혜".equals(key)
//                && !"박근혜".equals(key)) {
        jedis.hincrBy("word-count-dev-00", key, val);
//        }
    }
}