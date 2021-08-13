package com.rhy.stormdemo.simple.group;

import com.rhy.stormdemo.simple.number.NumberBolt;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class GroupBolt extends BaseRichBolt {
    private SpoutOutputCollector spoutOutputCollector;
    private NumberBolt numberBolt;
    private NumberBolt numberBolt2;
    public GroupBolt(NumberBolt numberBolt,NumberBolt numberBolt2) {
        this.numberBolt = numberBolt;
        this.numberBolt2 = numberBolt2;
        System.out.println("GroupBolt实例化了【"+this.numberBolt+"】【"+numberBolt2+"】");
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
//        System.out.println("GroupBolt传入的数据：【"+this+"】"+tuple.getStringByField("str"));
        System.out.println("GroupBolt传入的数据：【"+this+"】【"+numberBolt+"】【"+numberBolt2+"】"+tuple.getStringByField("str"));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
