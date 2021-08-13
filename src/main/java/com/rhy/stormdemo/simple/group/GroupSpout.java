package com.rhy.stormdemo.simple.group;

import com.rhy.stormdemo.simple.number.NumberBolt;
import lombok.SneakyThrows;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class GroupSpout extends BaseRichSpout {
    private NumberBolt numberBolt;
    public GroupSpout(NumberBolt numberBolt) {
        this.numberBolt = numberBolt;
        System.out.println("GroupSpout实例化了【"+this.numberBolt+"】");
    }
    private SpoutOutputCollector spoutOutputCollector;
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }
    String[] strings = new String[]{"a","b","c"};
    int count = 0;
    @SneakyThrows
    @Override
    public void nextTuple() {
        System.out.println("GroupSpout【"+numberBolt+"】");
        int num = (int) (Math.random() * 3);
//        spoutOutputCollector.emit(new Values(strings[num]));
        //广播测试用
        spoutOutputCollector.emit(new Values(strings[num]+count++));
        Thread.sleep(1000);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("str"));
    }
    public static void main(String[] args) {
        for (int i = 0; i < 100; i++) {
            int num = (int) (Math.random() * 3);
            System.out.println(num);
        }
    }
}
