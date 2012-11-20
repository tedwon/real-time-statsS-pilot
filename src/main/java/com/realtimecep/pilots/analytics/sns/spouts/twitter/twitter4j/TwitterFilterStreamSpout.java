package com.realtimecep.pilots.analytics.sns.spouts.twitter.twitter4j;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.slf4j.LoggerFactory;
import twitter4j.FilterQuery;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Twitter Filter Stream Spout Class.
 * <p/>
 *
 * @author <a href="iamtedwon@gmail.com">Ted Won</a>
 * @version 0.1.0
 */
public class TwitterFilterStreamSpout extends BaseRichSpout {

    private org.slf4j.Logger logger = LoggerFactory.getLogger(TwitterFilterStreamSpout.class);

    SpoutOutputCollector _collector;
    LinkedBlockingQueue<Status> queue = null;
    TwitterStream _twitterStream;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        queue = new LinkedBlockingQueue<Status>(1000);
        _collector = collector;

        String user = (String) conf.get("user");
        String password = (String) conf.get("password");
        String track = (String) conf.get("track");


        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
//                System.out.println("@" + status.getUser().getScreenName() + " - " + status.getText());
                queue.offer(status);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
            }

            @Override
            public void onException(Exception ex) {
            }
        };

        TwitterStream twitterStream =
                new TwitterStreamFactory(new ConfigurationBuilder()
                        .setUser(user)
                        .setPassword(password).build()).getInstance();
        twitterStream.addListener(listener);
        ArrayList<String> trackList = new ArrayList<String>();
        trackList.addAll(Arrays.asList(track.split(",")));

        String[] trackArray = trackList.toArray(new String[trackList.size()]);

        // filter() method internally creates a thread which manipulates TwitterStream and calls these adequate listener methods continuously.
        twitterStream.filter(new FilterQuery(0, null, trackArray));

    }

    @Override
    public void close() {
        _twitterStream.shutdown();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        return ret;
    }

    @Override
    public void ack(Object id) {
        logger.info("ack!");
    }

    @Override
    public void fail(Object id) {
        logger.info("fail!");
    }

    @Override
    public void nextTuple() {
        Status tweet = queue.poll();
        if (tweet == null) {
            Utils.sleep(50);
        } else {
//            logger.info(tweet.getText());
            _collector.emit(new Values(tweet.getText()));
        }
    }

}
