package com.rhy.stormdemo.simple.group;

import com.rhy.stormdemo.simple.number.NumberBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.grouping.ShuffleGrouping;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class GroupTopology {
    public static void main(String[] args) {
        NumberBolt numberBolt = new NumberBolt();

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("groupSpout",new GroupSpout(numberBolt));
//        topologyBuilder.setBolt("groupBolt",new GroupBolt(),2).setNumTasks(2)
//        .shuffleGrouping("groupSpout");
        topologyBuilder.setBolt("groupBolt",new GroupBolt(new NumberBolt(),numberBolt),2)
//                .fieldsGrouping("groupSpout",new Fields("str"));
//                .allGrouping("groupSpout");
                .customGrouping("groupSpout",new RandomCustomGrouping());
        Config config = new Config();
        //配置worker数量
        //不能超过supervisor.slots.ports数量
        config.setNumWorkers(3);

        StormTopology topology = topologyBuilder.createTopology();
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("groupTopology",config,topology);
    }
}
