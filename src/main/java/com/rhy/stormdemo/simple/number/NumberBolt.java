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
 * @date: 2021/8/11 21:32
 * @slogan: 如果你想攀登高峰，切莫把彩虹当梯子
 * @description:
 */
public class NumberBolt extends BaseRichBolt {
    /**
     * 流发送器
     */
    private OutputCollector outputCollector;

    private int count;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        //保存发送器在传递数据时需要用到
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        System.out.println("NumberBolt接收到的Tuple："+tuple);
        System.out.println("NumberBolt获取接受到的数据：按下标-"+tuple.getInteger(0)+"&按字段名-"+tuple.getIntegerByField("num")+";按下标-"+tuple.getString(1)+"&按字段名-"+tuple.getStringByField("name"));
        count += tuple.getIntegerByField("num");
        System.out.println("NumberBolt统计的数据和："+count);
        outputCollector.emit(new Values(count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("num"));
    }
}
