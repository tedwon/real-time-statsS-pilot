package com.realtimecep.storm.starter.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * 배치 단위로 데이터를 처리하는 기능을 테스트하기 위한 클래스.
 *
 * @author tedwon
 */
public class MyTridentWordCountBatch {

    public static class Split extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String sentence = tuple.getString(0);

//            System.out.println(sentence);

            MyEvent event = new MyEvent(sentence);

            collector.emit(new Values(event.getData5(), event));
        }
    }

    public static class Split2 extends BaseFunction {

        // 맵안에 채널 별 맵을 가진다.
        // key: channel
        // vale: (
        //  key: id + start time
        //  value: current time
        // )
        private Map<String, Map<String, String>> ratingData = Collections.synchronizedSortedMap(new ConcurrentSkipListMap<String, Map<String, String>>());


        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String channel = tuple.getStringByField("channel");
            MyEvent event = (MyEvent) tuple.getValueByField("word");

            String startOrStop = event.getData2();

            Map<String, String> channelData = null;

            // 시작이면,
            // map에 put
            String key = event.getData1() + "-" + event.getData3();
            if ("start".equalsIgnoreCase(startOrStop)) {


                if (ratingData.get(channel) == null) {
                    // 처음이면 채널 맵 생성
                    channelData = Collections.synchronizedSortedMap(new ConcurrentSkipListMap<String, String>());
                } else {
                    // 처널 맵에 시청 시작 데이터 추가
                    channelData = ratingData.get(channel);
                }

                channelData.put(key, event.getData4());
                ratingData.put(channel, channelData);

            } else {
                // 시청 종료이면,
                // map에 remove
                channelData = ratingData.get(channel);
                if (channelData != null) {
                    channelData.remove(key);
                }

                // apply updated data
                ratingData.put(channel, channelData);
            }

            System.out.println("!### " + channel + " : " + ratingData.get(channel).size());
        }
    }

    public static StormTopology buildTopology(LocalDRPC drpc) {
        MyFixedBatchSpout spout = new MyFixedBatchSpout(new Fields("sentence"), 2,
                new Values("1,start,2013-01-01 00:00:00,2013-01-01 00:00:10,11"),
                new Values("4,start,2013-01-01 00:00:00,2013-01-01 00:00:10,7"),
                new Values("2,start,2013-01-01 00:00:01,2013-01-01 00:00:11,11"),
                new Values("5,start,2013-01-01 00:00:01,2013-01-01 00:00:11,7"),
                new Values("3,start,2013-01-01 00:00:02,2013-01-01 00:00:12,11"),
                new Values("6,start,2013-01-01 00:00:02,2013-01-01 00:00:12,7"),
                new Values("1,stop,2013-01-01 00:00:00,2013-01-01 00:01:10,11"),
                new Values("4,stop,2013-01-01 00:00:00,2013-01-01 00:01:10,7"),
                new Values("2,stop,2013-01-01 00:00:01,2013-01-01 00:01:11,11"),
                new Values("5,stop,2013-01-01 00:00:01,2013-01-01 00:01:11,7"),
                new Values("3,stop,2013-01-01 00:00:02,2013-01-01 00:01:12,11"),
                new Values("6,stop,2013-01-01 00:00:02,2013-01-01 00:01:12,7")
        );
        spout.setCycle(false);

        TridentTopology topology = new TridentTopology();
        topology.newStream("spout1", spout)
                .parallelismHint(2)
                .each(new Fields("sentence"), new Split(), new Fields("channel", "word"))
                .groupBy(new Fields("channel"))
                .each(new Fields("channel", "word"), new Split2(), new Fields("myword"));


        return topology.build();
    }

    public static void main(String[] args) {

        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-Xmx2g");

        LocalCluster cluster = new LocalCluster();

        LocalDRPC drpc = new LocalDRPC();
//        cluster.submitTopology("wordCounter", conf, buildTopology(drpc));
        cluster.submitTopology("wordCounter", conf, buildTopology(null));

//        drpc.shutdown();
//        cluster.shutdown();


    }
}
