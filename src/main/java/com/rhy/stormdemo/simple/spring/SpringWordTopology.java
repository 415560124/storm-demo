package com.rhy.stormdemo.simple.spring;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;

@ComponentScan(basePackageClasses = SpringWordTopology.class)
public class SpringWordTopology {
    public static void main(String[] args) {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(SpringWordTopology.class);
        WordSpout wordSpout = context.getBean(WordSpout.class);
        WordSplitBolt wordSplitBolt = context.getBean(WordSplitBolt.class);
        WordCountBolt wordCountBolt = context.getBean(WordCountBolt.class);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("wordSpout",wordSpout);
        builder.setBolt("wordSplitBolt",wordSplitBolt).shuffleGrouping("wordSpout");
        builder.setBolt("wordCountBolt",wordCountBolt).shuffleGrouping("wordSplitBolt");

        Config config = new Config();
//        config.setNumWorkers(3);
        StormTopology topology = builder.createTopology();
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("wordTopology",config,topology);
    }
}
