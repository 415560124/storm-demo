package com.rhy.stormdemo.simple.spring;

import com.rhy.stormdemo.simple.spring.service.LineService;
import lombok.SneakyThrows;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Random;

@Component
public class WordSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    @Autowired
    private LineService lineService;
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @SneakyThrows
    @Override
    public void nextTuple() {
        String line = lineService.getLine();
        System.out.println("WordSpout传递的语句为："+line);
        collector.emit(new Values(line));
        Thread.sleep(1000);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }
}
