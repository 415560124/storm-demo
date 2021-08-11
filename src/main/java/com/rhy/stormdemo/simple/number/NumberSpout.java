package com.rhy.stormdemo.simple.number;

import lombok.SneakyThrows;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @author: Herion Lemon
 * @date: 2021/8/11 21:27
 * @slogan: 如果你想攀登高峰，切莫把彩虹当梯子
 * @description:
 */
public class NumberSpout extends BaseRichSpout {
    /**
     * 流发送器
     */
    private SpoutOutputCollector spoutOutputCollector;

    /**
     * 开启采集任务时会调用
     * @param map
     * @param topologyContext
     * @param spoutOutputCollector
     */
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        //保存发送器在传递数据时需要用到
        this.spoutOutputCollector = spoutOutputCollector;
    }

    /**
     * 在采集任务开始后，会一直调用这个方法采集数据
     */
    @SneakyThrows
    @Override
    public void nextTuple() {
        int num = (int) (Math.random() * 100);
        //往下传递数据
        spoutOutputCollector.emit(new Values(num,"rhy"));
        //避免太快
        Thread.sleep(1000);
    }

    /**
     * 往下传递的数据设置对应字段信息
     * @param outputFieldsDeclarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("num","name"));
    }

    public static void main(String[] args) {
        for (int i = 0; i < 100; i++) {
            int num = (int) (Math.random() * 100);
            System.out.println(num);
        }
    }
}
