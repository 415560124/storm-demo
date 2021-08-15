package com.rhy.stormdemo.simple.spring;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
@Component
public class WordCountBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<String,Integer> map = new HashMap<>();
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getStringByField("word");
        if(map.containsKey(word)){
            map.put(word,map.get(word)+1);
        }else {
            map.put(word,1);
        }
        System.out.println("WordCountBolt接收到的单词为：【"+word+"】数量【"+map.get(word)+"】");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
