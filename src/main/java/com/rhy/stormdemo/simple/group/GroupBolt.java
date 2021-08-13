package com.rhy.stormdemo.simple.group;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class GroupBolt extends BaseRichBolt {
    private SpoutOutputCollector spoutOutputCollector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        System.out.println("GroupBolt传入的数据：【"+this+"】"+tuple.getStringByField("str"));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
