package com.realtimecep.storm.starter.trident;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

import java.util.List;
import java.util.Map;


public class MyFixedBatchSpout implements IBatchSpout {

    Fields fields;
    List<Object>[] outputs;
    int maxBatchSize;

    public MyFixedBatchSpout(Fields fields, int maxBatchSize, List<Object>... outputs) {
        this.fields = fields;
        this.outputs = outputs;
        this.maxBatchSize = maxBatchSize;
    }

    int index = 0;
    boolean cycle = false;

    public void setCycle(boolean cycle) {
        this.cycle = cycle;
    }

    @Override
    public void open(Map conf, TopologyContext context) {
        index = 0;
    }

    @Override
    public void emitBatch(long batchId, TridentCollector collector) {

        Utils.sleep(1000);
        if (index >= outputs.length && cycle) {
            index = 0;
        }

//        System.out.println("### start emitBatch: " + batchId + " --------------------------");
        for (int i = 0; index < outputs.length && i < maxBatchSize; index++, i++) {
            collector.emit(outputs[index]);
//            System.out.println("### " + batchId + " - " + index + " : " + outputs[index]);
        }
//        System.out.println("### stop emitBatch: " + batchId + " --------------------------");

    }

    @Override
    public void ack(long batchId) {

    }

    @Override
    public void close() {
    }

    @Override
    public Map getComponentConfiguration() {
        Config conf = new Config();
        conf.setMaxTaskParallelism(1);
        return conf;
    }

    @Override
    public Fields getOutputFields() {
        return fields;
    }

}
