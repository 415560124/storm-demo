package com.rhy.stormdemo.simple.number;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IWindowedBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @author: Herion Lemon
 * @date: 2021/8/11 21:53
 * @slogan: 如果你想攀登高峰，切莫把彩虹当梯子
 * @description:
 */
public class NumberBolt2 extends BaseRichBolt {
    /**
     * 流发送器
     */
    private OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        //保存发送器在传递数据时需要用到
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        System.out.println("NumberBolt2接收到的Tuple："+tuple);
        System.out.println("NumberBolt2获取接受到的数据：按下标-"+tuple.getInteger(0)+"&按字段名-"+tuple.getIntegerByField("num"));
        Integer num = tuple.getIntegerByField("num");
        System.out.println("NumberBolt2统计的数据和："+num);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("num"));
    }
}
