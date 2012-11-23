package com.realtimecep.pilots.analytics.sns.trident;

import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * HashTag Splitter Function Class.
 * <p/>
 *
 * @author <a href="iamtedwon@gmail.com">Ted Won</a>
 * @version 0.1.0
 */
public class HashTagSplitter extends BaseFunction {

    private static Logger logger = LoggerFactory.getLogger(HashTagSplitter.class);

    private static final long serialVersionUID = 4177035756923453986L;

    @Override
    public void execute(TridentTuple input, TridentCollector collector) {

        String tweet = (String) input.getValueByField("tweet");

//        logger.info("### " + tweet);

        String[] words = tweet.split(" ");
        for (String word : words) {
            word = word.trim();
            word = word.toLowerCase();
            if (!word.isEmpty() && filter(word)) {
                collector.emit(new Values(word));
            }
        }
    }


    /**
     * Filter.
     *
     * @param word Word
     * @return whether to filter
     */
    private boolean filter(String word) {

        if ("rt".equals(word)
                || "...".equals(word)
                || word.length() == 1) {

            return false;
        }

        return true;
    }
}
