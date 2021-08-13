package com.rhy.stormdemo.simple.word;

import lombok.SneakyThrows;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

public class WordSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private String[] lines = new String[]{
        "hello wyy",
        "hello rhy",
        "hi our home",
        "hi our future"
    };
    private Random random;
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        random = new Random();
        this.collector = collector;
    }

    @SneakyThrows
    @Override
    public void nextTuple() {
        String line = lines[random.nextInt(lines.length)];
        System.out.println("WordSpout传递的语句为："+line);
        collector.emit(new Values(line));
        Thread.sleep(1000);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }
}
