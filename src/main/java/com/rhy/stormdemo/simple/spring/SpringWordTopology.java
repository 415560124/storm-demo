package com.rhy.stormdemo.simple.spring;

import com.rhy.stormdemo.simple.word.WordCountBolt;
import com.rhy.stormdemo.simple.word.WordSplitBolt;
import com.rhy.stormdemo.simple.word.WordSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public class SpringWordTopology {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("wordSpout",new WordSpout());
        builder.setBolt("wordSplitBolt",new WordSplitBolt()).shuffleGrouping("wordSpout");
        builder.setBolt("wordCountBolt",new WordCountBolt()).shuffleGrouping("wordSplitBolt");

        Config config = new Config();
//        config.setNumWorkers(3);
        StormTopology topology = builder.createTopology();
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("wordTopology",config,topology);
    }
}
